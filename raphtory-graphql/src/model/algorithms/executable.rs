//! The machinery that turns an algorithm into a GraphQL resolver.

use crate::rayon::blocking_compute;
use dynamic_graphql::ResolvedObject;
use raphtory::{db::api::view::DynamicGraph, errors::GraphError};

/// A graph algorithm executable through the GraphQL API.
pub(crate) trait GqlExecutableAlgorithm: 'static {
    /// The algorithm's arguments, assembled from the GraphQL field arguments
    type Args: Send + 'static;

    /// The GraphQL-facing result, typically a GqlNodeState but can be different (e.g. scalars)
    type Output: Send + 'static;

    /// Runs the algorithm on the given graph view
    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError>;
}

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
    /// Runs algorithm `A` on the blocking thread pool.
    pub(crate) async fn run<A: GqlExecutableAlgorithm>(
        &self,
        args: A::Args,
    ) -> Result<A::Output, GraphError> {
        let graph = self.graph.clone();
        blocking_compute(move || A::execute(&graph, args)).await
    }
}
