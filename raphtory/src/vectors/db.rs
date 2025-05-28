use std::{
    collections::HashSet,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use arroy::{distances::Cosine, Database as ArroyDatabase, Reader, Writer};
use futures_util::StreamExt;
use rand::{rngs::StdRng, SeedableRng};
use sysinfo::System;
use tempfile::TempDir;

use crate::{
    core::utils::errors::{GraphError, GraphResult},
    db::api::view::StaticGraphViewOps,
};

use super::{
    entity_ref::{EntityRef, IntoDbId},
    Embedding,
};

const LMDB_MAX_SIZE: usize = 1024 * 1024 * 1024 * 1024; // 1TB

#[derive(Clone)]
pub(super) struct NodeDb(pub(super) VectorDb);

impl Deref for NodeDb {
    type Target = VectorDb;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl EntityDb for NodeDb {
    fn from_vector_db(db: VectorDb) -> Self {
        Self(db)
    }

    fn get_db(&self) -> &VectorDb {
        &self.0
    }

    fn into_entity_ref(id: u32) -> EntityRef {
        EntityRef::Node(id)
    }

    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool {
        view.has_node(entity.as_node_gid(view).unwrap())
    }

    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = u32> {
        view.nodes().into_iter().map(|node| node.into_db_id())
    }
}

#[derive(Clone)]
pub(super) struct EdgeDb(pub(super) VectorDb);

impl Deref for EdgeDb {
    type Target = VectorDb;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl EntityDb for EdgeDb {
    fn from_vector_db(db: VectorDb) -> Self {
        Self(db)
    }

    fn get_db(&self) -> &VectorDb {
        &self.0
    }

    fn into_entity_ref(id: u32) -> EntityRef {
        EntityRef::Edge(id)
    }

    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool {
        let (src, dst) = entity.as_edge_gids(view).unwrap();
        view.has_edge(src, dst) // TODO: there should be a quicker way of chking of some edge exist by pid
    }

    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = u32> {
        view.edges().into_iter().map(|edge| edge.into_db_id())
    }
}

pub(super) trait EntityDb: Sized {
    fn from_vector_db(db: VectorDb) -> Self;
    fn get_db(&self) -> &VectorDb;
    fn into_entity_ref(id: u32) -> EntityRef;
    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool;
    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = u32> + 'static;

    async fn from_vectors(
        vectors: impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send,
        path: Option<PathBuf>,
    ) -> GraphResult<Self> {
        let db = VectorDb::from_vectors(vectors, path).await?;
        Ok(Self::from_vector_db(db))
    }

    fn from_path(path: &Path) -> GraphResult<Self> {
        VectorDb::from_path(path).map(Self::from_vector_db)
    }

    fn top_k<G: StaticGraphViewOps>(
        &self,
        query: &Embedding,
        k: usize,
        view: Option<G>,
        filter: Option<HashSet<EntityRef>>,
    ) -> GraphResult<impl Iterator<Item = (EntityRef, f32)>> {
        let candidates: Option<Box<dyn Iterator<Item = u32>>> = match (view, filter) {
            (None, None) => None,
            (view, Some(filter)) => Some(Box::new(
                filter
                    .into_iter()
                    .filter(move |entity| {
                        view.as_ref()
                            .map_or(true, |view| Self::view_has_entity(entity, view))
                    })
                    .map(|entity| entity.id()),
            )),
            (Some(view), None) => Some(Box::new(Self::all_valid_entities(view))),
        };
        self.top_k_with_candidates(query, k, candidates)
    }

    fn top_k_with_candidates(
        &self,
        query: &Embedding,
        k: usize,
        candidates: Option<impl Iterator<Item = u32>>,
    ) -> GraphResult<impl Iterator<Item = (EntityRef, f32)>> {
        let db = self.get_db();
        let rtxn = db.env.read_txn()?;
        let vectors = match Reader::open(&rtxn, 0, db.vectors) {
            Ok(reader) => {
                let mut query_builder = reader.nns(k);
                let candidates = candidates.map(|filter| roaring::RoaringBitmap::from_iter(filter));
                let query_builder = if let Some(filter) = &candidates {
                    query_builder.candidates(filter)
                } else {
                    &query_builder
                };
                query_builder.by_vector(&rtxn, query.as_ref())?
            }
            Err(arroy::Error::MissingMetadata(_)) => vec![], // this just means the db is empty
            Err(error) => return Err(error.into()),
        };
        Ok(vectors
            .into_iter()
            // for arroy, distance = (1.0 - score) / 2.0, where score is cosine: [-1, 1]
            .map(|(id, distance)| (Self::into_entity_ref(id), 1.0 - 2.0 * distance)))
    }
}

#[derive(Clone)]
pub(crate) struct VectorDb {
    pub(crate) vectors: ArroyDatabase<Cosine>,
    pub(crate) env: heed::Env,
    pub(crate) _tempdir: Option<Arc<TempDir>>, // do we really need this, is the file open not enough
    pub(crate) dimensions: OnceLock<usize>,
}

impl VectorDb {
    pub(super) fn insert_vector(&self, id: usize, embedding: &Embedding) -> GraphResult<()> {
        let mut wtxn = self.env.write_txn()?;

        let dimensions = self.dimensions.get_or_init(|| embedding.len());
        let writer = Writer::<Cosine>::new(self.vectors, 0, *dimensions);
        writer.add_item(&mut wtxn, id as u32, embedding.as_ref())?;

        let mut rng = StdRng::from_entropy();
        writer.builder(&mut rng).build(&mut wtxn)?;

        wtxn.commit()?;
        Ok(())
    }

    pub(super) fn get_id(&self, id: u32) -> GraphResult<Option<Embedding>> {
        let rtxn = self.env.read_txn()?;
        let reader = Reader::open(&rtxn, 0, self.vectors)?;
        let vector = reader.item_vector(&rtxn, id)?;
        Ok(vector.map(|vector| vector.into()))
    }

    fn from_path(path: &Path) -> GraphResult<Self> {
        let env = open_env(path)?;
        let rtxn = env.read_txn()?;
        let db: ArroyDatabase<Cosine> = env
            .open_database(&rtxn, None)?
            .ok_or_else(|| GraphError::VectorDbDoesntExist(path.display().to_string()))?;
        let first_vector = Reader::open(&rtxn, 0, db)
            .ok()
            .and_then(|reader| reader.iter(&rtxn).ok()?.next()?.ok());
        let dimensions = if let Some((_, vector)) = first_vector {
            vector.len().into()
        } else {
            OnceLock::new()
        };
        rtxn.commit()?;
        Ok(Self {
            vectors: db,
            env,
            _tempdir: None,
            dimensions,
        })
    }

    async fn from_vectors(
        vectors: impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send,
        path: Option<PathBuf>,
    ) -> GraphResult<Self> {
        let (env, tempdir) = match path {
            Some(path) => {
                std::fs::create_dir_all(&path)?;
                (open_env(&path)?, None)
            }
            None => {
                let tempdir = tempfile::tempdir()?;
                (open_env(tempdir.path())?, Some(tempdir.into()))
            }
        };

        let mut wtxn = env.write_txn()?;
        let db: ArroyDatabase<Cosine> = env.create_database(&mut wtxn, None)?;

        futures_util::pin_mut!(vectors);
        let first_vector = vectors.next().await;
        let dimensions = if let Some(Ok((first_id, first_vector))) = first_vector {
            let dimensions = first_vector.len();
            let writer = Writer::<Cosine>::new(db, 0, dimensions);

            writer.add_item(&mut wtxn, first_id, &first_vector)?;
            while let Some(result) = vectors.next().await {
                let (id, vector) = result?;
                writer.add_item(&mut wtxn, id, &vector)?;
            }

            // TODO: review this -> You can specify the number of trees to use or specify None.
            let mut rng = StdRng::seed_from_u64(42);
            writer
                .builder(&mut rng)
                .available_memory(System::new().total_memory() as usize / 2)
                .build(&mut wtxn)?;
            dimensions.into()
        } else {
            OnceLock::new()
        };

        wtxn.commit()?;

        Ok(Self {
            vectors: db,
            env,
            _tempdir: tempdir.into(),
            dimensions,
        })
    }
}

fn open_env(path: &Path) -> heed::Result<heed::Env> {
    unsafe {
        heed::EnvOpenOptions::new()
            .map_size(LMDB_MAX_SIZE)
            .open(path)
    }
}
