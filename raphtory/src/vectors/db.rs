use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use arroy::{distances::Cosine, Database as ArroyDatabase, Reader, Writer};
use futures_util::StreamExt;
use rand::{rngs::StdRng, SeedableRng};
use sysinfo::System;
use tempfile::TempDir;

use crate::{core::utils::errors::GraphResult, db::api::view::StaticGraphViewOps};

use super::{entity_ref::EntityRef, Embedding};

const LMDB_MAX_SIZE: usize = 1024 * 1024 * 1024 * 1024; // 1TB // TODO: review !!!!!!!!!!!!

#[derive(Clone)]
pub(super) struct NodeDb(pub(super) VectorDb);

impl EntityDb for NodeDb {
    fn from_vector_db(db: VectorDb) -> Self {
        Self(db)
    }

    fn get_db(&self) -> &VectorDb {
        &self.0
    }

    fn into_entity_ref(id: u32) -> EntityRef {
        EntityRef::Node(id as usize)
    }

    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool {
        view.has_node(entity.as_node(view).unwrap())
    }

    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = usize> {
        view.nodes().into_iter().map(|node| node.node.index())
    }
}

#[derive(Clone)]
pub(super) struct EdgeDb(pub(super) VectorDb);

impl EntityDb for EdgeDb {
    fn from_vector_db(db: VectorDb) -> Self {
        Self(db)
    }

    fn get_db(&self) -> &VectorDb {
        &self.0
    }

    fn into_entity_ref(id: u32) -> EntityRef {
        EntityRef::Edge(id as usize)
    }

    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool {
        let (src, dst) = entity.as_edge(view).unwrap(); // TODO: remove this?
        view.has_edge(src, dst) // FIXME: there should be a quicker way!!!!!!!!!!!!!!!!!!!
    }

    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = usize> {
        view.edges().into_iter().map(|edge| edge.edge.pid().0)
    }
}

// FIXME: remove unwraps in here
// TODO: rename this to GraphVectorSearch
// TODO: merge this and VectorDb !!!!!!!!!!!!!!!!!???????
pub(super) trait EntityDb: Sized {
    fn from_vector_db(db: VectorDb) -> Self;
    fn get_db(&self) -> &VectorDb;
    fn into_entity_ref(id: u32) -> EntityRef;
    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool;
    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = usize> + 'static;

    async fn from_vectors(
        vectors: impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send,
        path: Option<PathBuf>,
    ) -> GraphResult<Self> {
        let db = VectorDb::from_vectors(vectors, path).await?;
        Ok(Self::from_vector_db(db))
    }

    fn from_path(path: &Path) -> Self {
        Self::from_vector_db(VectorDb::from_path(path))
    }

    fn top_k<G: StaticGraphViewOps>(
        &self,
        query: &Embedding,
        k: usize,
        view: Option<G>,
        filter: Option<HashSet<EntityRef>>,
    ) -> impl Iterator<Item = (EntityRef, f32)> {
        let candidates: Option<Box<dyn Iterator<Item = u32>>> = match (view, filter) {
            (None, None) => None,
            (view, Some(filter)) => Some(Box::new(
                filter
                    .into_iter()
                    .filter(move |entity| {
                        view.as_ref()
                            .map_or(true, |view| Self::view_has_entity(entity, view))
                    })
                    .map(|entity| entity.as_u32()),
            )),
            (view, None) => Some(Box::new(
                Self::all_valid_entities(view.unwrap()).map(|id| id as u32), // FIXME: unwrap here doesnt make sense
            )),
        };
        self.top_k_with_candidates(query, k, candidates)
    }

    fn top_k_with_candidates(
        &self,
        query: &Embedding,
        k: usize,
        candidates: Option<impl Iterator<Item = u32>>,
    ) -> impl Iterator<Item = (EntityRef, f32)> {
        let db = self.get_db();
        let rtxn = db.env.read_txn().unwrap();
        // FIXME: if the db has no edges, I get a MissingMetadata here. Handle that properly,
        // Maybe the edge db should not exist at all if there are no edge embeddings
        // because there might be some errors other than MissingMetadata
        let vectors = if let Ok(reader) = Reader::open(&rtxn, 0, db.vectors) {
            let mut query_builder = reader.nns(k);
            let candidates = candidates
                .map(|filter| roaring::RoaringBitmap::from_iter(filter.map(|id| id as u32)));
            let query_builder = if let Some(filter) = &candidates {
                query_builder.candidates(filter)
            } else {
                &query_builder
            };
            query_builder.by_vector(&rtxn, query.as_ref()).unwrap()
        } else {
            vec![]
        };
        vectors
            .into_iter()
            // for arroy, distance = (1.0 - score) / 2.0, where score is cosine: [-1, 1]
            .map(|(id, distance)| (Self::into_entity_ref(id), 1.0 - 2.0 * distance))
    }
}

#[derive(Clone)]
pub(crate) struct VectorDb {
    // FIXME: save index value in here !!!!!!!!!!!!!!!!!!!
    pub(crate) vectors: ArroyDatabase<Cosine>, // TODO: review is this safe to clone? does it point to the same thing?
    pub(crate) env: heed::Env,
    pub(crate) _tempdir: Option<Arc<TempDir>>, // do I really need, is the file open not enough
    pub(crate) dimensions: OnceLock<usize>,
}

// TODO: merge this with the above
impl VectorDb {
    pub(super) fn insert_vector(&self, id: usize, embedding: &Embedding) {
        // FIXME: remove unwraps
        let mut wtxn = self.env.write_txn().unwrap();

        let dimensions = self.dimensions.get_or_init(|| embedding.len());
        let writer = Writer::<Cosine>::new(self.vectors, 0, *dimensions);
        writer
            .add_item(&mut wtxn, id as u32, embedding.as_ref())
            .unwrap();

        let mut rng = StdRng::from_entropy();
        writer.builder(&mut rng).build(&mut wtxn).unwrap();

        wtxn.commit().unwrap();
    }

    pub(super) fn get_id(&self, id: usize) -> Embedding {
        let rtxn = self.env.read_txn().unwrap();
        let reader = Reader::open(&rtxn, 0, self.vectors).unwrap();
        let vector = reader.item_vector(&rtxn, id as u32).unwrap().unwrap();
        vector.into()
    }

    fn from_path(path: &Path) -> Self {
        // TODO: fix unwraps!
        let env = open_env(path);
        let rtxn = env.read_txn().unwrap();
        let db: ArroyDatabase<Cosine> = env.open_database(&rtxn, None).unwrap().unwrap(); // this is the old implementation, causing an issue I think
        let first_vector = Reader::open(&rtxn, 0, db)
            .ok()
            .and_then(|reader| reader.iter(&rtxn).ok()?.next()?.ok());
        let dimensions = if let Some((_, vector)) = first_vector {
            // FIXME: maybe there should not be any db at all if this is the case?
            vector.len().into()
        } else {
            OnceLock::new()
        };
        rtxn.commit().unwrap();
        Self {
            vectors: db,
            env,
            _tempdir: None,
            dimensions,
        }
    }

    pub(super) async fn from_vectors(
        vectors: impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send,
        path: Option<PathBuf>,
    ) -> GraphResult<Self> {
        let (env, tempdir) = match path {
            Some(path) => {
                std::fs::create_dir_all(&path).unwrap();
                (open_env(&path), None)
            }
            None => {
                let tempdir = tempfile::tempdir()?;
                (open_env(tempdir.path()), Some(tempdir.into()))
            }
        };

        let mut wtxn = env.write_txn().unwrap(); // FIXME: remove unwrap
        let db: ArroyDatabase<Cosine> = env.create_database(&mut wtxn, None).unwrap();

        futures_util::pin_mut!(vectors);
        let first_vector = vectors.next().await;
        let dimensions = if let Some(Ok((first_id, first_vector))) = first_vector {
            let dimensions = first_vector.len(); // TODO: if vectors is empty, simply don't write anything!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            let writer = Writer::<Cosine>::new(db, 0, dimensions);

            writer.add_item(&mut wtxn, first_id, &first_vector).unwrap();
            while let Some(result) = vectors.next().await {
                let (id, vector) = result?;
                writer.add_item(&mut wtxn, id, &vector).unwrap();
            }

            // TODO: review this -> You can specify the number of trees to use or specify None.
            let mut rng = StdRng::seed_from_u64(42);
            writer
                .builder(&mut rng)
                .available_memory(System::new().total_memory() as usize / 2)
                .build(&mut wtxn)
                .unwrap();
            dimensions.into()
        } else {
            OnceLock::new()
        };

        wtxn.commit().unwrap();

        Ok(Self {
            vectors: db,
            env,
            _tempdir: tempdir.into(),
            dimensions,
        })
    }
}

fn open_env(path: &Path) -> heed::Env {
    let env = unsafe {
        heed::EnvOpenOptions::new()
            .map_size(LMDB_MAX_SIZE)
            .open(path)
    }
    .unwrap();
    // FIXME: remove unwrap
    env
}
