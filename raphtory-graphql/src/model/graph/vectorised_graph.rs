use crate::{model::graph::timeindex::GqlTimeInput, rayon::blocking_compute};

use super::vector_selection::GqlVectorSelection;
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::api::view::MaterializedGraph, errors::GraphResult,
    vectors::vectorised_graph::VectorisedGraph,
};
use raphtory_api::core::{storage::timeindex::AsTime, utils::time::IntoTime};

#[derive(InputObject)]
pub(super) struct VectorisedGraphWindow {
    /// Inclusive lower bound of the search window.
    start: GqlTimeInput,
    /// Exclusive upper bound of the search window.
    end: GqlTimeInput,
}

pub(super) trait IntoWindowTuple {
    fn into_window_tuple(self) -> Option<(i64, i64)>;
}

impl IntoWindowTuple for Option<VectorisedGraphWindow> {
    fn into_window_tuple(self) -> Option<(i64, i64)> {
        self.map(|window| (window.start.into_time().t(), window.end.into_time().t()))
    }
}

/// A graph with embedded vector representations for its nodes and edges.
/// Exposes similarity search over documents, nodes, and edges, plus
/// selection building (`emptySelection`) and index maintenance
/// (`optimizeIndex`).
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
    /// Rebuild (or incrementally update) the on-disk vector indexes for nodes
    /// and edges so subsequent similarity searches hit the fresh embeddings.
    /// Safe to call repeatedly; returns true on success.
    async fn optimize_index(&self) -> GraphResult<bool> {
        self.0.optimize_index().await?;
        Ok(true)
    }

    /// Returns an empty selection of documents.
    async fn empty_selection(&self) -> GqlVectorSelection {
        self.0.empty_selection().into()
    }

    /// Find the highest-scoring nodes *and* edges (mixed) by similarity to a
    /// natural-language query. The query is embedded server-side and matched
    /// against indexed entity vectors.

    async fn entities_by_similarity(
        &self,
        #[graphql(desc = "Natural-language search string; embedded by the server.")] query: String,
        #[graphql(desc = "Maximum number of results to return.")] limit: usize,
        #[graphql(
            desc = "Optional `{start, end}` to restrict matches to entities active in that interval."
        )]
        window: Option<VectorisedGraphWindow>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = self.0.embed_text(query).await?;
        let w = window.into_window_tuple();
        let cloned = self.0.clone();
        let query =
            blocking_compute(move || cloned.entities_by_similarity(&vector, limit, w)).await;
        Ok(query.execute().await?.into())
    }

    /// Find the highest-scoring nodes by similarity to a natural-language
    /// query. The query is embedded server-side and matched against indexed
    /// node vectors.

    async fn nodes_by_similarity(
        &self,
        #[graphql(desc = "Natural-language search string; embedded by the server.")] query: String,
        #[graphql(desc = "Maximum number of nodes to return.")] limit: usize,
        #[graphql(
            desc = "Optional `{start, end}` to restrict matches to nodes active in that interval."
        )]
        window: Option<VectorisedGraphWindow>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = self.0.embed_text(query).await?;
        let w = window.into_window_tuple();
        let cloned = self.0.clone();
        let query = blocking_compute(move || cloned.nodes_by_similarity(&vector, limit, w)).await;
        Ok(query.execute().await?.into())
    }

    /// Find the highest-scoring edges by similarity to a natural-language
    /// query. The query is embedded server-side and matched against indexed
    /// edge vectors.

    async fn edges_by_similarity(
        &self,
        #[graphql(desc = "Natural-language search string; embedded by the server.")] query: String,
        #[graphql(desc = "Maximum number of edges to return.")] limit: usize,
        #[graphql(
            desc = "Optional `{start, end}` to restrict matches to edges active in that interval."
        )]
        window: Option<VectorisedGraphWindow>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = self.0.embed_text(query).await?;
        let w = window.into_window_tuple();
        let cloned = self.0.clone();
        let query = blocking_compute(move || cloned.edges_by_similarity(&vector, limit, w)).await;
        Ok(query.execute().await?.into())
    }
}
