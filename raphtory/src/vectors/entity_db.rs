use std::{collections::HashSet, future::Future, ops::Deref, pin::Pin};

use futures_util::StreamExt;

use super::{
    entity_ref::{EntityRef, IntoDbId},
    Embedding,
};
use crate::{
    db::api::view::StaticGraphViewOps,
    errors::GraphResult,
    prelude::GraphViewOps,
    vectors::{vector_collection::VectorCollection, VectorsQuery},
};

#[derive(Clone)]
pub(super) struct NodeDb<D: VectorCollection>(pub(super) D);

impl<D: VectorCollection> Deref for NodeDb<D> {
    type Target = D;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<D: VectorCollection + 'static> EntityDb for NodeDb<D> {
    type VectorDb = D;

    fn get_db(&self) -> &Self::VectorDb {
        &self.0
    }

    fn into_entity_ref(id: u64) -> EntityRef {
        EntityRef::Node(id)
    }

    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool {
        view.has_node(entity.as_node_gid(view).unwrap())
    }

    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = u64> + Send {
        view.nodes().into_iter().map(|node| node.into_db_id())
    }
}

#[derive(Clone)]
pub(super) struct EdgeDb<D: VectorCollection>(pub(super) D);

impl<D: VectorCollection> Deref for EdgeDb<D> {
    type Target = D;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<D: VectorCollection + 'static> EntityDb for EdgeDb<D> {
    type VectorDb = D;

    fn get_db(&self) -> &Self::VectorDb {
        &self.0
    }

    fn into_entity_ref(id: u64) -> EntityRef {
        EntityRef::Edge(id)
    }

    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool {
        let (src, dst) = entity.as_edge_gids(view).unwrap();
        view.has_edge(src, dst) // TODO: there should be a quicker way of chking of some edge exist by pid
    }

    fn all_valid_entities<G: StaticGraphViewOps>(view: G) -> impl Iterator<Item = u64> + Send {
        view.edges().into_iter().map(|edge| edge.into_db_id())
    }
}

pub(super) trait EntityDb: Sized {
    type VectorDb: VectorCollection + 'static;
    fn get_db(&self) -> &Self::VectorDb;
    fn into_entity_ref(id: u64) -> EntityRef;
    fn view_has_entity<G: StaticGraphViewOps>(entity: &EntityRef, view: &G) -> bool;
    fn all_valid_entities<G: StaticGraphViewOps>(
        view: G,
    ) -> impl Iterator<Item = u64> + Send + 'static;

    async fn insert_vector_stream(
        &self,
        vectors: impl futures_util::Stream<Item = GraphResult<(u64, Embedding)>> + Send,
    ) -> GraphResult<()> {
        futures_util::pin_mut!(vectors);

        while let Some(result) = vectors.as_mut().chunks(1000).next().await {
            let vector_result: Vec<(u64, Embedding)> = result
                .into_iter()
                .map(|result| result.unwrap())
                .map(|(id, vector)| (id, vector))
                .collect();
            let ids = vector_result.iter().map(|(id, _)| *id).collect();
            let vectors = vector_result.into_iter().map(|(_, vector)| vector);
            self.get_db().insert_vectors(ids, vectors).await.unwrap()
        }
        Ok(())
    }

    fn top_k<G: StaticGraphViewOps>(
        &self,
        query: &Embedding,
        k: usize,
        view: Option<G>,
        filter: Option<HashSet<EntityRef>>,
    ) -> VectorsQuery<GraphResult<Vec<(EntityRef, f32)>>> {
        let candidates: Option<Vec<u64>> = match (view, filter) {
            (None, None) => None,
            (view, Some(filter)) => Some(
                filter
                    .into_iter()
                    .filter(move |entity| {
                        view.as_ref()
                            .is_none_or(|view| Self::view_has_entity(entity, view))
                    })
                    .map(|entity| entity.id())
                    .collect(),
            ),
            (Some(view), None) => Some(Self::all_valid_entities(view).collect()),
        };

        let db = self.get_db().clone();
        let query = query.clone();
        VectorsQuery::new(Box::pin(async move {
            Ok(db
                .top_k_with_distances(&query, k, candidates)
                .await?
                .map(|(id, distance)| (Self::into_entity_ref(id), distance))
                .collect::<Vec<_>>())
        }))
    }
}
