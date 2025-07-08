use super::vector_selection::GqlVectorSelection;
use crate::{embeddings::EmbedQuery, model::blocking_io};
use async_graphql::Context;
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::api::view::MaterializedGraph, errors::GraphResult,
    vectors::vectorised_graph::VectorisedGraph,
};

#[derive(InputObject)]
pub(super) struct Window {
    start: i64,
    end: i64,
}

pub(super) trait IntoWindowTuple {
    fn into_window_tuple(self) -> Option<(i64, i64)>;
}

impl IntoWindowTuple for Option<Window> {
    fn into_window_tuple(self) -> Option<(i64, i64)> {
        self.map(|window| (window.start, window.end))
    }
}

#[derive(ResolvedObject)]
#[graphql(name = "VectorisedGraph")]
pub(crate) struct GqlVectorisedGraph(VectorisedGraph<MaterializedGraph>);

impl From<VectorisedGraph<MaterializedGraph>> for GqlVectorisedGraph {
    fn from(value: VectorisedGraph<MaterializedGraph>) -> Self {
        Self(value.clone())
    }
}

#[ResolvedObjectFields]
impl GqlVectorisedGraph {
    async fn empty_selection(&self) -> GqlVectorSelection {
        self.0.empty_selection().into()
    }

    async fn entities_by_similarity(
        &self,
        ctx: &Context<'_>,
        query: String,
        limit: usize,
        window: Option<Window>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = ctx.embed_query(query).await?;
        let w = window.into_window_tuple();
        let cloned = self.0.clone();
        blocking_io(move || Ok(cloned.entities_by_similarity(&vector, limit, w)?.into())).await
    }

    async fn nodes_by_similarity(
        &self,
        ctx: &Context<'_>,
        query: String,
        limit: usize,
        window: Option<Window>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = ctx.embed_query(query).await?;
        let w = window.into_window_tuple();
        let cloned = self.0.clone();
        blocking_io(move || Ok(cloned.nodes_by_similarity(&vector, limit, w)?.into())).await
    }

    async fn edges_by_similarity(
        &self,
        ctx: &Context<'_>,
        query: String,
        limit: usize,
        window: Option<Window>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = ctx.embed_query(query).await?;
        let w = window.into_window_tuple();
        let cloned = self.0.clone();
        blocking_io(move || Ok(cloned.edges_by_similarity(&vector, limit, w)?.into())).await
    }
}
