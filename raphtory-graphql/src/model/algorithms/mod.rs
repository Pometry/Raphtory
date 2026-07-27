//! Statically defined graph algorithms exposed through `Graph.algorithm`.

use crate::{
    model::{
        algorithms::{
            all_local_reciprocity::{GqlAllLocalReciprocity, GqlAllLocalReciprocityArgs},
            average_degree::{GqlAverageDegree, GqlAverageDegreeArgs},
            balance::{GqlBalance, GqlBalanceArgs},
            betweenness_centrality::{GqlBetweennessCentrality, GqlBetweennessCentralityArgs},
            cohesive_fruchterman_reingold::{
                GqlCohesiveFruchtermanReingold, GqlCohesiveFruchtermanReingoldArgs,
            },
            degree_centrality::{GqlDegreeCentrality, GqlDegreeCentralityArgs},
            dijkstra::{GqlDijkstra, GqlDijkstraArgs},
            directed_graph_density::{GqlDirectedGraphDensity, GqlDirectedGraphDensityArgs},
            fast_rp::{GqlFastRp, GqlFastRpArgs},
            fruchterman_reingold::{GqlFruchtermanReingold, GqlFruchtermanReingoldArgs},
            global_clustering_coefficient::{
                GqlGlobalClusteringCoefficient, GqlGlobalClusteringCoefficientArgs,
            },
            global_reciprocity::{GqlGlobalReciprocity, GqlGlobalReciprocityArgs},
            hits::{GqlHits, GqlHitsArgs},
            in_component::{GqlInComponent, GqlInComponentArgs},
            in_components::{GqlInComponents, GqlInComponentsArgs},
            label_propagation::{GqlLabelPropagation, GqlLabelPropagationArgs},
            local_clustering_coefficient_batch::{
                GqlLocalClusteringCoefficientBatch, GqlLocalClusteringCoefficientBatchArgs,
            },
            local_temporal_three_node_motifs::{
                GqlLocalTemporalThreeNodeMotifs, GqlLocalTemporalThreeNodeMotifsArgs,
            },
            louvain::{GqlLouvain, GqlLouvainArgs},
            out_component::{GqlOutComponent, GqlOutComponentArgs},
            out_components::{GqlOutComponents, GqlOutComponentsArgs},
            pagerank::{GqlPagerank, GqlPagerankArgs},
            single_source_shortest_path::{
                GqlSingleSourceShortestPath, GqlSingleSourceShortestPathArgs,
            },
            strongly_connected_components::{
                GqlStronglyConnectedComponents, GqlStronglyConnectedComponentsArgs,
            },
            temporally_reachable_nodes::{
                GqlTemporallyReachableNodes, GqlTemporallyReachableNodesArgs,
            },
            weakly_connected_components::{
                GqlWeaklyConnectedComponents, GqlWeaklyConnectedComponentsArgs,
            },
        },
        graph::{filtering::GqlViewFilter, node_id::GqlNodeId, node_state::GqlNodeState},
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{Enum, ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{
        api::view::{DynamicGraph, Filter, IntoDynamic},
        graph::views::filter::model::{
            edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter, DynView,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::Direction;

pub(crate) mod all_local_reciprocity;
pub(crate) mod average_degree;
pub(crate) mod balance;
pub(crate) mod betweenness_centrality;
pub(crate) mod cohesive_fruchterman_reingold;
pub(crate) mod degree_centrality;
pub(crate) mod dijkstra;
pub(crate) mod directed_graph_density;
pub(crate) mod fast_rp;
pub(crate) mod fruchterman_reingold;
pub(crate) mod global_clustering_coefficient;
pub(crate) mod global_reciprocity;
pub(crate) mod hits;
pub(crate) mod in_component;
pub(crate) mod in_components;
pub(crate) mod label_propagation;
pub(crate) mod local_clustering_coefficient_batch;
pub(crate) mod local_temporal_three_node_motifs;
pub(crate) mod louvain;
pub(crate) mod out_component;
pub(crate) mod out_components;
pub(crate) mod pagerank;
pub(crate) mod single_source_shortest_path;
pub(crate) mod strongly_connected_components;
pub(crate) mod temporally_reachable_nodes;
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

/// Applies an optional composite filter, returning the filtered view (or the
/// graph unchanged if no filter is given).
pub(crate) fn filtered_view(
    graph: &DynamicGraph,
    filter: Option<GqlViewFilter>,
) -> Result<DynamicGraph, GraphError> {
    let Some(filter) = filter else {
        return Ok(graph.clone());
    };
    let mut graph = graph.clone();
    if let Some(nodes) = filter.nodes {
        let nodes: CompositeNodeFilter = nodes.try_into()?;
        graph = graph.filter(nodes)?.into_dynamic();
    }
    if let Some(edges) = filter.edges {
        let edges: CompositeEdgeFilter = edges.try_into()?;
        graph = graph.filter(edges)?.into_dynamic();
    }
    if let Some(view) = filter.graph {
        let view: DynView = view.try_into()?;
        graph = graph.filter(view)?.into_dynamic();
    }
    Ok(graph)
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

    /// Returns the in component of a single node (nodes that can reach it, with their distance).
    async fn in_component(
        &self,
        #[graphql(desc = "Node id.")] node: GqlNodeId,
        #[graphql(
            desc = "Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view."
        )]
        filter: Option<GqlViewFilter>,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlInComponent>(GqlInComponentArgs { node, filter })
            .await
    }

    /// Returns the out component of a single node (nodes it can reach, with their distance).
    async fn out_component(
        &self,
        #[graphql(desc = "Node id.")] node: GqlNodeId,
        #[graphql(
            desc = "Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view."
        )]
        filter: Option<GqlViewFilter>,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlOutComponent>(GqlOutComponentArgs { node, filter })
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

    /// Returns the global clustering coefficient of the graph.
    async fn global_clustering_coefficient(&self) -> Result<f64, GraphError> {
        self.run::<GqlGlobalClusteringCoefficient>(GqlGlobalClusteringCoefficientArgs)
            .await
    }

    /// Returns the directed graph density (fraction of possible directed edges present).
    async fn directed_graph_density(&self) -> Result<f64, GraphError> {
        self.run::<GqlDirectedGraphDensity>(GqlDirectedGraphDensityArgs)
            .await
    }

    /// Returns the global reciprocity of the graph.
    async fn global_reciprocity(&self) -> Result<f64, GraphError> {
        self.run::<GqlGlobalReciprocity>(GqlGlobalReciprocityArgs)
            .await
    }

    /// Returns the average (undirected) degree of the graph's nodes.
    async fn average_degree(&self) -> Result<f64, GraphError> {
        self.run::<GqlAverageDegree>(GqlAverageDegreeArgs).await
    }

    /// Returns the FastRP embedding of every node.
    async fn fast_rp(
        &self,
        #[graphql(desc = "Dimension of the embedding.")] embedding_dim: usize,
        #[graphql(desc = "Normalization strength applied to neighbour contributions.")]
        normalization_strength: f64,
        #[graphql(desc = "Weight of each iteration's contribution to the embedding.")]
        iter_weights: Vec<f64>,
        #[graphql(desc = "Seed for the rng. If unset, seeded from the OS.")] seed: Option<u64>,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlFastRp>(GqlFastRpArgs {
            embedding_dim,
            normalization_strength,
            iter_weights,
            seed,
            threads,
        })
        .await
    }

    /// Returns the nodes temporally reachable from `seedNodes` starting at `startTime`.
    async fn temporally_reachable_nodes(
        &self,
        #[graphql(desc = "Maximum number of hops to traverse.")] max_hops: usize,
        #[graphql(desc = "Time at which the traversal starts.")] start_time: i64,
        #[graphql(desc = "Node ids to start from.")] seed_nodes: Vec<String>,
        #[graphql(desc = "Node ids that halt the traversal when reached.")] stop_nodes: Option<
            Vec<String>,
        >,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlTemporallyReachableNodes>(GqlTemporallyReachableNodesArgs {
            max_hops,
            start_time,
            seed_nodes,
            stop_nodes,
            threads,
        })
        .await
    }

    /// Returns the 2D layout position of every node (Fruchterman-Reingold).
    async fn fruchterman_reingold(
        &self,
        #[graphql(desc = "Number of iterations to run. Defaults to 100.")] iter_count: Option<u64>,
        #[graphql(desc = "Scale of the layout. Defaults to 1.0.")] scale: Option<f64>,
        #[graphql(desc = "Initial node size. Defaults to 1.0.")] node_start_size: Option<f64>,
        #[graphql(desc = "Cooloff factor. Defaults to 0.95.")] cooloff_factor: Option<f64>,
        #[graphql(desc = "Time step. Defaults to 0.1.")] dt: Option<f64>,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlFruchtermanReingold>(GqlFruchtermanReingoldArgs {
            iter_count: iter_count.unwrap_or(100),
            scale: scale.unwrap_or(1.0),
            node_start_size: node_start_size.unwrap_or(1.0),
            cooloff_factor: cooloff_factor.unwrap_or(0.95),
            dt: dt.unwrap_or(0.1),
        })
        .await
    }

    /// Returns the 2D layout position of every node (cohesive Fruchterman-Reingold).
    async fn cohesive_fruchterman_reingold(
        &self,
        #[graphql(desc = "Number of iterations to run. Defaults to 100.")] iter_count: Option<u64>,
        #[graphql(desc = "Scale of the layout. Defaults to 1.0.")] scale: Option<f64>,
        #[graphql(desc = "Initial node size. Defaults to 1.0.")] node_start_size: Option<f64>,
        #[graphql(desc = "Cooloff factor. Defaults to 0.95.")] cooloff_factor: Option<f64>,
        #[graphql(desc = "Time step. Defaults to 0.1.")] dt: Option<f64>,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlCohesiveFruchtermanReingold>(GqlCohesiveFruchtermanReingoldArgs {
            iter_count: iter_count.unwrap_or(100),
            scale: scale.unwrap_or(1.0),
            node_start_size: node_start_size.unwrap_or(1.0),
            cooloff_factor: cooloff_factor.unwrap_or(0.95),
            dt: dt.unwrap_or(0.1),
        })
        .await
    }

    /// Returns the local temporal three-node motif counts of every node.
    async fn local_temporal_three_node_motifs(
        &self,
        #[graphql(desc = "Maximum time difference between the first and last edge of a motif.")]
        delta: i64,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlLocalTemporalThreeNodeMotifs>(GqlLocalTemporalThreeNodeMotifsArgs {
            delta,
            threads,
        })
        .await
    }
}
