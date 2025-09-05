use std::{collections::HashSet, ops::Deref, path::Path};

use arroy::Reader;
use futures_util::StreamExt;

use super::{
    entity_ref::{EntityRef, IntoDbId},
    Embedding,
};
use crate::{
    db::api::view::StaticGraphViewOps, errors::GraphResult, prelude::GraphViewOps,
    vectors::vector_db::VectorDb,
};

const LMDB_MAX_SIZE: usize = 1024 * 1024 * 1024 * 1024; // 1TB

#[derive(Clone)]
pub(super) struct NodeDb<D: VectorDb>(pub(super) D);

impl<D: VectorDb> Deref for NodeDb<D> {
    type Target = D;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<D: VectorDb + 'static> EntityDb for NodeDb<D> {
    type VectorDb = D;

    fn get_db(&self) -> &Self::VectorDb {
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
pub(super) struct EdgeDb<D: VectorDb>(pub(super) D);

impl<D: VectorDb> Deref for EdgeDb<D> {
    type Target = D;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<D: VectorDb + 'static> EntityDb for EdgeDb<D> {
    type VectorDb = D;

    fn get_db(&self) -> &Self::VectorDb {
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
    type VectorDb: VectorDb;
    fn get_db(&self) -> &Self::VectorDb;
    fn into_entity_ref(id: u32) -> EntityRef;
    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool;
    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = u32> + 'static;

    // async fn from_vectors(
    //     vectors: impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send,
    //     path: Option<PathBuf>,
    // ) -> GraphResult<Self> {
    //     let db = VectorDb::from_vectors(vectors, path).await?;
    //     Ok(Self::from_vector_db(db))
    // }

    // fn from_path(path: &Path) -> GraphResult<Self> {
    //     VectorDb::from_path(path).map(Self::from_vector_db)
    // }

    fn from_path(path: &Path) -> GraphResult<Self> {
        todo!() // TODO: remove this function, only here for compilation
    }

    async fn insert_vector_stream(
        &self,
        vectors: impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send,
    ) -> GraphResult<()> {
        futures_util::pin_mut!(vectors);

        while let Some(result) = vectors.as_mut().chunks(1000).next().await {
            let vector_result: Vec<(usize, Embedding)> = result
                .into_iter()
                .map(|result| result.unwrap())
                .map(|(id, vector)| (id as usize, vector))
                .collect();
            self.get_db().insert_vectors(vector_result).await.unwrap()
        }
        Ok(())
    }

    async fn top_k<G: StaticGraphViewOps>(
        &self,
        query: &Embedding,
        k: usize,
        view: Option<G>,
        filter: Option<HashSet<EntityRef>>,
    ) -> GraphResult<impl Iterator<Item = (EntityRef, f32)>> {
        let result = self
            .get_db()
            .top_k(query, k)
            .await
            .map(|(id, score)| (Self::into_entity_ref(id as u32), score));
        Ok(result)
        // let candidates: Option<Box<dyn Iterator<Item = u32>>> = match (view, filter) {
        //     (None, None) => None,
        //     (view, Some(filter)) => Some(Box::new(
        //         filter
        //             .into_iter()
        //             .filter(move |entity| {
        //                 view.as_ref()
        //                     .is_none_or(|view| Self::view_has_entity(entity, view))
        //             })
        //             .map(|entity| entity.id()),
        //     )),
        //     (Some(view), None) => Some(Box::new(Self::all_valid_entities(view))),
        // };
        // self.top_k_with_candidates(query, k, candidates)
    }

    // fn top_k_with_candidates(
    //     &self,
    //     query: &Embedding,
    //     k: usize,
    //     candidates: Option<impl Iterator<Item = u32>>,
    // ) -> GraphResult<impl Iterator<Item = (EntityRef, f32)>> {
    //     let db = self.get_db();
    //     let rtxn = db.env.read_txn()?;
    //     let vectors = match Reader::open(&rtxn, 0, db.vectors) {
    //         Ok(reader) => {
    //             let mut query_builder = reader.nns(k);
    //             let candidates = candidates.map(|filter| roaring::RoaringBitmap::from_iter(filter));
    //             let query_builder = if let Some(filter) = &candidates {
    //                 query_builder.candidates(filter)
    //             } else {
    //                 &query_builder
    //             };
    //             query_builder.by_vector(&rtxn, query.as_ref())?
    //         }
    //         Err(arroy::Error::MissingMetadata(_)) => vec![], // this just means the db is empty
    //         Err(error) => return Err(error.into()),
    //     };
    //     Ok(vectors
    //         .into_iter()
    //         // for arroy, distance = (1.0 - score) / 2.0, where score is cosine: [-1, 1]
    //         .map(|(id, distance)| (Self::into_entity_ref(id), 1.0 - 2.0 * distance)))
    //     // TODO: make sure to include this correction into arroy impl!!!!!!!!!!
    // }
}
