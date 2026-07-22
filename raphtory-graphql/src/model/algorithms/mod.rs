//! Statically defined graph algorithms exposed through `Graph.algorithm`.

use crate::{
    model::{
        algorithms::{
            all_local_reciprocity::{GqlAllLocalReciprocity, GqlAllLocalReciprocityArgs},
            balance::{GqlBalance, GqlBalanceArgs},
            betweenness_centrality::{GqlBetweennessCentrality, GqlBetweennessCentralityArgs},
            degree_centrality::{GqlDegreeCentrality, GqlDegreeCentralityArgs},
            dijkstra::{GqlDijkstra, GqlDijkstraArgs},
            hits::{GqlHits, GqlHitsArgs},
            in_components::{GqlInComponents, GqlInComponentsArgs},
            label_propagation::{GqlLabelPropagation, GqlLabelPropagationArgs},
            local_clustering_coefficient_batch::{
                GqlLocalClusteringCoefficientBatch, GqlLocalClusteringCoefficientBatchArgs,
            },
            louvain::{GqlLouvain, GqlLouvainArgs},
            out_components::{GqlOutComponents, GqlOutComponentsArgs},
            pagerank::{GqlPagerank, GqlPagerankArgs},
            single_source_shortest_path::{
                GqlSingleSourceShortestPath, GqlSingleSourceShortestPathArgs,
            },
            strongly_connected_components::{
                GqlStronglyConnectedComponents, GqlStronglyConnectedComponentsArgs,
            },
            weakly_connected_components::{
                GqlWeaklyConnectedComponents, GqlWeaklyConnectedComponentsArgs,
            },
        },
        graph::node_state::GqlNodeState,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{Enum, ResolvedObject, ResolvedObjectFields};
use raphtory::{db::api::view::DynamicGraph, errors::GraphError};
use raphtory_api::core::Direction;

pub(crate) mod all_local_reciprocity;
pub(crate) mod balance;
pub(crate) mod betweenness_centrality;
pub(crate) mod degree_centrality;
pub(crate) mod dijkstra;
pub(crate) mod hits;
pub(crate) mod in_components;
pub(crate) mod label_propagation;
pub(crate) mod local_clustering_coefficient_batch;
pub(crate) mod louvain;
pub(crate) mod out_components;
pub(crate) mod pagerank;
pub(crate) mod single_source_shortest_path;
pub(crate) mod strongly_connected_components;
pub(crate) mod weakly_connected_components;

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

/// Edge direction to follow during traversal.
#[derive(Enum, Copy, Clone)]
#[graphql(name = "Direction")]
pub(crate) enum GqlDirection {
    Out,
    In,
    Both,
}

impl From<GqlDirection> for Direction {
    fn from(direction: GqlDirection) -> Self {
        match direction {
            GqlDirection::Out => Direction::OUT,
            GqlDirection::In => Direction::IN,
            GqlDirection::Both => Direction::BOTH,
        }
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
        #[graphql(desc = "Number of iterations to run. Defaults to 20.")] iter_count: Option<usize>,
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

    /// Returns the degree centrality of every node.
    async fn degree_centrality(&self) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlDegreeCentrality>(GqlDegreeCentralityArgs)
            .await
    }

    /// Returns the betweenness centrality of every node.
    async fn betweenness_centrality(
        &self,
        #[graphql(desc = "Number of nodes to sample. Defaults to all nodes.")] k: Option<usize>,
        #[graphql(desc = "Whether to normalize the values. Defaults to true.")] normalized: Option<
            bool,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlBetweennessCentrality>(GqlBetweennessCentralityArgs {
            k,
            normalized: normalized.unwrap_or(true),
        })
        .await
    }

    /// Returns the HITS hub and authority scores of every node.
    async fn hits(
        &self,
        #[graphql(desc = "Number of iterations to run. Defaults to 20.")] iter_count: Option<usize>,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlHits>(GqlHitsArgs {
            iter_count: iter_count.unwrap_or(20),
            threads,
        })
        .await
    }

    /// Returns the shortest (unweighted) path from `source` to every reachable node.
    async fn single_source_shortest_path(
        &self,
        #[graphql(desc = "Source node id.")] source: String,
        #[graphql(desc = "Optional maximum path length; stops the search once reached.")]
        cutoff: Option<usize>,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlSingleSourceShortestPath>(GqlSingleSourceShortestPathArgs { source, cutoff })
            .await
    }

    /// Returns the in component (all nodes that can reach it following out-edges) of every node.
    async fn in_components(
        &self,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlInComponents>(GqlInComponentsArgs { threads })
            .await
    }

    /// Returns the out component (all reachable nodes following out-edges) of every node.
    async fn out_components(
        &self,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlOutComponents>(GqlOutComponentsArgs { threads })
            .await
    }

    /// Returns the weakly connected component id of every node.
    async fn weakly_connected_components(&self) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlWeaklyConnectedComponents>(GqlWeaklyConnectedComponentsArgs)
            .await
    }

    /// Returns the strongly connected component id of every node.
    async fn strongly_connected_components(&self) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlStronglyConnectedComponents>(GqlStronglyConnectedComponentsArgs)
            .await
    }

    /// Returns the community of every node (Louvain).
    async fn louvain(
        &self,
        #[graphql(desc = "Resolution parameter for modularity. Defaults to 1.0.")]
        resolution: Option<f64>,
        #[graphql(desc = "Edge property to use as weight. If unset, all edges have weight 1.")]
        weight_prop: Option<String>,
        #[graphql(desc = "Convergence tolerance. Defaults to 1e-8.")] tol: Option<f64>,
        #[graphql(desc = "Seed for the node-shuffling rng. If unset, seeded from the OS.")]
        rng_seed: Option<u64>,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlLouvain>(GqlLouvainArgs {
            resolution: resolution.unwrap_or(1.0),
            weight_prop,
            tol,
            rng_seed,
        })
        .await
    }

    /// Returns the community of every node (label propagation).
    async fn label_propagation(
        &self,
        #[graphql(desc = "Number of iterations to run. Defaults to 20.")] iter_count: Option<usize>,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlLabelPropagation>(GqlLabelPropagationArgs {
            iter_count: iter_count.unwrap_or(20),
            threads,
        })
        .await
    }

    /// Returns the weighted shortest path from `source` to each of `targets` (Dijkstra).
    async fn dijkstra(
        &self,
        #[graphql(desc = "Source node id.")] source: String,
        #[graphql(desc = "Target node ids.")] targets: Vec<String>,
        #[graphql(desc = "Edge property to use as weight. If unset, all edges have weight 1.")]
        weight: Option<String>,
        #[graphql(desc = "Edge direction to follow. Defaults to BOTH.")] direction: Option<
            GqlDirection,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlDijkstra>(GqlDijkstraArgs {
            source,
            targets,
            weight,
            direction: direction.unwrap_or(GqlDirection::Both),
        })
        .await
    }

    /// Returns the local reciprocity of every node.
    async fn all_local_reciprocity(&self) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlAllLocalReciprocity>(GqlAllLocalReciprocityArgs)
            .await
    }

    /// Returns the net sum of edge weights (balance) of every node.
    async fn balance(
        &self,
        #[graphql(desc = "Edge property to use as weight. Defaults to `weight`.")] name: Option<
            String,
        >,
        #[graphql(desc = "Edge direction to consider. Defaults to BOTH.")] direction: Option<
            GqlDirection,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlBalance>(GqlBalanceArgs {
            name: name.unwrap_or_else(|| "weight".to_string()),
            direction: direction.unwrap_or(GqlDirection::Both),
        })
        .await
    }

    /// Returns the local clustering coefficient of each of the given nodes.
    async fn local_clustering_coefficient_batch(
        &self,
        #[graphql(desc = "Node ids to compute the coefficient for.")] nodes: Vec<String>,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlLocalClusteringCoefficientBatch>(GqlLocalClusteringCoefficientBatchArgs {
            nodes,
        })
        .await
    }
}
