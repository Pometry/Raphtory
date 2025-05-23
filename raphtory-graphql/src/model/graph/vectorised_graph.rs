use async_graphql::Context;
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::errors::GraphResult, db::api::view::MaterializedGraph,
    vectors::vectorised_graph::VectorisedGraph,
};

use crate::embeddings::EmbedQuery;

use super::vector_selection::GqlVectorSelection;

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
        let window = window.into_window_tuple();
        Ok(self.0.entities_by_similarity(&vector, limit, window).into())
    }

    async fn nodes_by_similarity(
        &self,
        ctx: &Context<'_>,
        query: String,
        limit: usize,
        window: Option<Window>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = ctx.embed_query(query).await?;
        let window = window.into_window_tuple();
        Ok(self.0.nodes_by_similarity(&vector, limit, window).into())
    }

    async fn edges_by_similarity(
        &self,
        ctx: &Context<'_>,
        query: String,
        limit: usize,
        window: Option<Window>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = ctx.embed_query(query).await?;
        let window = window.into_window_tuple();
        Ok(self.0.edges_by_similarity(&vector, limit, None).into())
    }
}
