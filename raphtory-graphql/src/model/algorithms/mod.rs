//! Statically defined graph algorithms exposed through `Graph.algorithm`.

use crate::{
    model::{
        algorithms::pagerank::{GqlPagerank, GqlPagerankArgs},
        graph::node_state::GqlNodeState,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{db::api::view::DynamicGraph, errors::GraphError};

pub(crate) mod pagerank;

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
    async fn run<A: GqlExecutableAlgorithm>(&self, args: A::Args) -> Result<A::Output, GraphError> {
        let graph = self.graph.clone();
        blocking_compute(move || A::execute(&graph, args)).await
    }
}

#[ResolvedObjectFields]
impl GqlAlgorithms {
    /// Returns the PageRank centrality of every node in the graph.
    async fn pagerank(
        &self,
        #[graphql(desc = "Number of iterations to run. Defaults to 20.")] iter_count: Option<
            usize,
        >,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
        #[graphql(desc = "Convergence tolerance. Defaults to 0.000001.")] tol: Option<f64>,
        #[graphql(desc = "Probability that the spread continues. Defaults to 0.85.")]
        damping_factor: Option<f64>,
        #[graphql(desc = "Edge property to use as weight. If unset, all edges have weight 1.")]
        weight: Option<String>,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlPagerank>(GqlPagerankArgs {
            iter_count,
            threads,
            tol,
            damping_factor,
            weight,
        })
        .await
    }
}
