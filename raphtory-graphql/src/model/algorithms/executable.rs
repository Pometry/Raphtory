//! The machinery that turns an algorithm into a GraphQL resolver.

use crate::rayon::blocking_compute;
use dynamic_graphql::ResolvedObject;
use raphtory::{db::api::view::DynamicGraph, errors::GraphError};

/// The algorithms that can be run on a graph view.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Algorithms")]
pub(crate) struct GqlAlgorithms {
    pub(crate) graph: DynamicGraph,
}

impl From<DynamicGraph> for GqlAlgorithms {
    fn from(graph: DynamicGraph) -> Self {
        Self { graph }
    }
}

impl GqlAlgorithms {
    /// Runs algorithm on the blocking thread pool.
    pub(crate) async fn run<F: FnOnce(DynamicGraph) -> O + Send + 'static, O: Send + 'static>(
        &self,
        algo: F,
    ) -> O {
        let graph = self.graph.clone();
        blocking_compute(move || algo(graph)).await
    }
}
