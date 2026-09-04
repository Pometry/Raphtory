//! The `Graph.algorithm` field resolvers: one per algorithm exposed through the GraphQL API.

use crate::{
    model::{
        algorithms::{
            inputs::{GqlDirection, GqlSeeds},
            outputs::{GqlMatching, GqlMotifCounts},
        },
        graph::{
            filtering::GqlFilter, node_id::GqlNodeId, node_state::GqlNodeState,
            timeindex::GqlTimeInput, WindowDuration,
        },
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use rand::{prelude::StdRng, SeedableRng};
use raphtory::{
    algorithms::{
        bipartite::max_weight_matching::max_weight_matching,
        centrality::{
            betweenness::betweenness_centrality, degree_centrality::degree_centrality, hits::hits,
            pagerank::page_rank,
        },
        community_detection::{
            label_propagation::label_propagation, louvain::louvain, modularity::ModularityUnDir,
        },
        components::{
            in_component, in_component_filtered, in_components, in_components_filtered,
            out_component, out_component_filtered, out_components, out_components_filtered,
            strongly_connected_components, weakly_connected_components,
        },
        dynamics::temporal::epidemics::temporal_SEIR,
        embeddings::fast_rp::fast_rp,
        layout::{
            cohesive_fruchterman_reingold::cohesive_fruchterman_reingold,
            fruchterman_reingold::fruchterman_reingold_unbounded,
        },
        metrics::{
            balance::balance,
            clustering_coefficient::{
                global_clustering_coefficient::global_clustering_coefficient,
                local_clustering_coefficient::local_clustering_coefficient,
                local_clustering_coefficient_batch::local_clustering_coefficient_batch,
            },
            degree::{
                average_degree, max_degree, max_in_degree, max_out_degree, min_degree,
                min_in_degree, min_out_degree,
            },
            directed_graph_density::directed_graph_density,
            reciprocity::{all_local_reciprocity, global_reciprocity},
        },
        motifs::{
            global_temporal_three_node_motifs::{
                global_temporal_three_node_motif, temporal_three_node_motif_multi,
            },
            local_temporal_three_node_motifs::temporal_three_node_motif,
            local_triangle_count::local_triangle_count,
            temporal_rich_club_coefficient::temporal_rich_club_coefficient,
            triangle_count::triangle_count,
            triplet_count::triplet_count,
        },
        pathing::{
            dijkstra::dijkstra_single_source_shortest_paths,
            single_source_shortest_path::single_source_shortest_path,
            temporal_reachability::temporally_reachable_nodes,
        },
    },
    core::entities::nodes::node_ref::AsNodeRef,
    db::{api::view::DynamicGraph, graph::node::NodeView},
    errors::GraphError,
    prelude::{GraphViewOps, TimeOps},
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::OptionAsStr};

fn get_node(
    graph: DynamicGraph,
    node: GqlNodeId,
) -> Result<NodeView<'static, DynamicGraph>, GraphError> {
    let node_id = node.0;
    let node = graph
        .node(node_id.as_node_ref())
        .ok_or(GraphError::NodeMissingError(node_id))?;
    Ok(node)
}

/// The algorithms that can be run on a graph view.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Algorithms")]
pub struct GqlAlgorithms {
    pub(crate) graph: DynamicGraph,
}

impl From<DynamicGraph> for GqlAlgorithms {
    fn from(graph: DynamicGraph) -> Self {
        Self { graph }
    }
}

impl GqlAlgorithms {
    /// Runs algorithm on the blocking thread pool.
    pub async fn run<F: FnOnce(DynamicGraph) -> O + Send + 'static, O: Send + 'static>(
        &self,
        algo: F,
    ) -> O {
        let graph = self.graph.clone();
        blocking_compute(move || algo(graph)).await
    }
}

#[ResolvedObjectFields]
impl GqlAlgorithms {
    /// Returns the PageRank centrality of every node in the graph.
    pub async fn pagerank(
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
    ) -> GqlNodeState {
        self.run(move |graph| {
            page_rank(
                &graph,
                weight.as_str(),
                iter_count,
                threads,
                tol,
                true,
                damping_factor,
            )
            .into()
        })
        .await
    }

    /// Returns the degree centrality of every node.
    pub async fn degree_centrality(&self) -> GqlNodeState {
        self.run(|graph| degree_centrality(&graph).into()).await
    }

    /// Returns the betweenness centrality of every node.
    pub async fn betweenness_centrality(
        &self,
        #[graphql(desc = "Number of nodes to sample. Defaults to all nodes.")] k: Option<usize>,
        #[graphql(desc = "Whether to normalize the values. Defaults to true.")] normalized: Option<
            bool,
        >,
    ) -> GqlNodeState {
        self.run(move |graph| betweenness_centrality(&graph, k, normalized.unwrap_or(true)).into())
            .await
    }

    /// Returns the HITS hub and authority scores of every node.
    pub async fn hits(
        &self,
        #[graphql(desc = "Number of iterations to run. Defaults to 20.")] iter_count: Option<usize>,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> GqlNodeState {
        self.run(move |graph| hits(&graph, iter_count.unwrap_or(20), threads).into())
            .await
    }

    /// Returns the shortest (unweighted) path from `source` to every reachable node.
    pub async fn single_source_shortest_path(
        &self,
        #[graphql(desc = "Source node id.")] source: GqlNodeId,
        #[graphql(desc = "Optional maximum path length; stops the search once reached.")]
        cutoff: Option<usize>,
    ) -> GqlNodeState {
        self.run(move |graph| single_source_shortest_path(&graph, source, cutoff).into())
            .await
    }

    /// Returns the in component (all nodes that can reach it following out-edges) of every node.
    pub async fn in_components(
        &self,
        #[graphql(
            desc = "Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view."
        )]
        filter: Option<GqlFilter>,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        Ok(self
            .run(move |graph| match filter {
                None => Ok(in_components(&graph, threads)),
                Some(filter) => in_components_filtered(&graph, threads, filter),
            })
            .await?
            .into())
    }

    /// Returns the out component (all reachable nodes following out-edges) of every node.
    pub async fn out_components(
        &self,
        #[graphql(
            desc = "Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view."
        )]
        filter: Option<GqlFilter>,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        Ok(self
            .run(move |graph| match filter {
                None => Ok(out_components(&graph, threads)),
                Some(filter) => out_components_filtered(&graph, threads, filter),
            })
            .await?
            .into())
    }

    /// Returns the in component of a single node (nodes that can reach it, with their distance).
    pub async fn in_component(
        &self,
        #[graphql(desc = "Node id.")] node: GqlNodeId,
        #[graphql(
            desc = "Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view."
        )]
        filter: Option<GqlFilter>,
    ) -> Result<GqlNodeState, GraphError> {
        Ok(self
            .run(move |graph| {
                let node = get_node(graph, node)?;
                match filter {
                    None => Ok(in_component(node)),
                    Some(filter) => in_component_filtered(node, filter),
                }
            })
            .await?
            .into())
    }

    /// Returns the out component of a single node (nodes it can reach, with their distance).
    pub async fn out_component(
        &self,
        #[graphql(desc = "Node id.")] node: GqlNodeId,
        #[graphql(
            desc = "Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view."
        )]
        filter: Option<GqlFilter>,
    ) -> Result<GqlNodeState, GraphError> {
        Ok(self
            .run(move |graph| {
                let node = get_node(graph, node)?;
                match filter {
                    None => Ok(out_component(node)),
                    Some(filter) => out_component_filtered(node, filter),
                }
            })
            .await?
            .into())
    }

    /// Returns the local triangle count of a single node (0 if it has degree < 2), or null if
    /// the node does not exist in the view.
    pub async fn local_triangle_count(
        &self,
        #[graphql(desc = "Node id.")] node: GqlNodeId,
    ) -> Option<usize> {
        self.run(move |graph| local_triangle_count(&graph, node))
            .await
    }

    /// Returns the local clustering coefficient of a single node (0 if it has degree < 2), or
    /// null if the node does not exist in the view.
    pub async fn local_clustering_coefficient(
        &self,
        #[graphql(desc = "Node id.")] node: GqlNodeId,
    ) -> Option<f64> {
        self.run(move |graph| local_clustering_coefficient(&graph, node))
            .await
    }

    /// Returns the weakly connected component id of every node.
    pub async fn weakly_connected_components(&self) -> GqlNodeState {
        self.run(|graph| weakly_connected_components(&graph).into())
            .await
    }

    /// Returns the strongly connected component id of every node.
    pub async fn strongly_connected_components(&self) -> GqlNodeState {
        self.run(|graph| strongly_connected_components(&graph).into())
            .await
    }

    /// Returns the community of every node (Louvain).
    pub async fn louvain(
        &self,
        #[graphql(desc = "Resolution parameter for modularity. Defaults to 1.0.")]
        resolution: Option<f64>,
        #[graphql(desc = "Edge property to use as weight. If unset, all edges have weight 1.")]
        weight_prop: Option<String>,
        #[graphql(desc = "Convergence tolerance. Defaults to 1e-8.")] tol: Option<f64>,
        #[graphql(desc = "Seed for the node-shuffling rng. If unset, seeded from the OS.")]
        rng_seed: Option<u64>,
    ) -> GqlNodeState {
        self.run(move |graph| {
            louvain::<ModularityUnDir, _>(
                &graph,
                resolution.unwrap_or(1.0),
                weight_prop.as_str(),
                tol,
                rng_seed,
            )
            .into()
        })
        .await
    }

    /// Returns the community of every node (label propagation).
    pub async fn label_propagation(
        &self,
        #[graphql(desc = "Number of iterations to run. Defaults to 20.")] iter_count: Option<usize>,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> GqlNodeState {
        self.run(move |graph| {
            label_propagation(&graph, iter_count.unwrap_or(20), None, threads).into()
        })
        .await
    }

    /// Returns the weighted shortest path from `source` to each of `targets` (Dijkstra).
    pub async fn dijkstra(
        &self,
        #[graphql(desc = "Source node id.")] source: GqlNodeId,
        #[graphql(desc = "Target node ids.")] targets: Vec<GqlNodeId>,
        #[graphql(desc = "Edge property to use as weight.")] weight: Option<String>,
        #[graphql(desc = "Edge direction to follow. Defaults to BOTH.")] direction: Option<
            GqlDirection,
        >,
        #[graphql(
            desc = "Weight for edges that do not have a weight. Used if `weight` is not specified or the edge does not have a value for that property. Defaults to 1."
        )]
        default_weight: Option<Prop>,
    ) -> Result<GqlNodeState, GraphError> {
        Ok(self
            .run(move |graph| {
                dijkstra_single_source_shortest_paths(
                    &graph,
                    source,
                    targets,
                    weight.as_str(),
                    direction.unwrap_or(GqlDirection::Both).into(),
                    default_weight,
                )
            })
            .await?
            .into())
    }

    /// Returns the local reciprocity of every node.
    pub async fn all_local_reciprocity(&self) -> GqlNodeState {
        self.run(move |graph| all_local_reciprocity(&graph).into())
            .await
    }

    /// Returns the net sum of edge weights (balance) of every node.
    pub async fn balance(
        &self,
        #[graphql(desc = "Edge property to use as weight. Defaults to `weight`.")] name: Option<
            String,
        >,
        #[graphql(desc = "Edge direction to consider. Defaults to BOTH.")] direction: Option<
            GqlDirection,
        >,
    ) -> Result<GqlNodeState, GraphError> {
        Ok(self
            .run(move |graph| {
                balance(
                    &graph,
                    name.unwrap_or("weight".to_string()),
                    direction.unwrap_or(GqlDirection::Both).into(),
                )
            })
            .await?
            .into())
    }

    /// Returns the local clustering coefficient of each of the given nodes.
    pub async fn local_clustering_coefficient_batch(
        &self,
        #[graphql(desc = "Node ids to compute the coefficient for.")] nodes: Vec<GqlNodeId>,
    ) -> GqlNodeState {
        self.run(move |graph| local_clustering_coefficient_batch(&graph, nodes).into())
            .await
    }

    /// Returns the global clustering coefficient of the graph.
    pub async fn global_clustering_coefficient(&self) -> f64 {
        self.run(|graph| global_clustering_coefficient(&graph))
            .await
    }

    /// Returns the directed graph density (fraction of possible directed edges present).
    pub async fn directed_graph_density(&self) -> f64 {
        self.run(|graph| directed_graph_density(&graph)).await
    }

    /// Returns the global reciprocity of the graph.
    pub async fn global_reciprocity(&self) -> f64 {
        self.run(|graph| global_reciprocity(&graph)).await
    }

    /// Returns the average (undirected) degree of the graph's nodes.
    pub async fn average_degree(&self) -> f64 {
        self.run(|graph| average_degree(&graph)).await
    }

    /// Returns the maximum (undirected) degree of any node in the graph.
    pub async fn max_degree(&self) -> usize {
        self.run(|graph| max_degree(&graph)).await
    }

    /// Returns the minimum (undirected) degree of any node in the graph.
    pub async fn min_degree(&self) -> usize {
        self.run(|graph| min_degree(&graph)).await
    }

    /// Returns the maximum out-degree of any node in the graph.
    pub async fn max_out_degree(&self) -> usize {
        self.run(|graph| max_out_degree(&graph)).await
    }

    /// Returns the maximum in-degree of any node in the graph.
    pub async fn max_in_degree(&self) -> usize {
        self.run(|graph| max_in_degree(&graph)).await
    }

    /// Returns the minimum out-degree of any node in the graph.
    pub async fn min_out_degree(&self) -> usize {
        self.run(|graph| min_out_degree(&graph)).await
    }

    /// Returns the minimum in-degree of any node in the graph.
    pub async fn min_in_degree(&self) -> usize {
        self.run(|graph| min_in_degree(&graph)).await
    }

    /// Returns the number of connected triplets (paths of length 2) in the graph.
    pub async fn triplet_count(
        &self,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> usize {
        self.run(move |graph| triplet_count(&graph, threads)).await
    }

    /// Returns the number of triangles in the graph.
    pub async fn triangle_count(
        &self,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> usize {
        self.run(move |graph| triangle_count(&graph, threads)).await
    }

    /// Returns the FastRP embedding of every node.
    pub async fn fast_rp(
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
    ) -> GqlNodeState {
        self.run(move |graph| {
            fast_rp(
                &graph,
                embedding_dim,
                normalization_strength,
                iter_weights,
                seed,
                threads,
            )
            .into()
        })
        .await
    }

    /// Returns the nodes temporally reachable from `seedNodes` starting at `startTime`.
    pub async fn temporally_reachable_nodes(
        &self,
        #[graphql(desc = "Maximum number of hops to traverse.")] max_hops: usize,
        #[graphql(desc = "Time at which the traversal starts.")] start_time: i64,
        #[graphql(desc = "Node ids to start from.")] seed_nodes: Vec<GqlNodeId>,
        #[graphql(desc = "Node ids that halt the traversal when reached.")] stop_nodes: Option<
            Vec<GqlNodeId>,
        >,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> GqlNodeState {
        self.run(move |graph| {
            temporally_reachable_nodes(
                &graph, threads, max_hops, start_time, seed_nodes, stop_nodes,
            )
            .into()
        })
        .await
    }

    /// Returns the 2D layout position of every node (Fruchterman-Reingold).
    pub async fn fruchterman_reingold(
        &self,
        #[graphql(desc = "Number of iterations to run. Defaults to 100.")] iter_count: Option<u64>,
        #[graphql(desc = "Scale of the layout. Defaults to 1.0.")] scale: Option<f64>,
        #[graphql(desc = "Initial node size. Defaults to 1.0.")] node_start_size: Option<f64>,
        #[graphql(desc = "Cooloff factor. Defaults to 0.95.")] cooloff_factor: Option<f64>,
        #[graphql(desc = "Time step. Defaults to 0.1.")] dt: Option<f64>,
    ) -> GqlNodeState {
        self.run(move |graph| {
            fruchterman_reingold_unbounded(
                &graph,
                iter_count.unwrap_or(100),
                scale.unwrap_or(1.0),
                node_start_size.unwrap_or(1.0),
                cooloff_factor.unwrap_or(0.95),
                dt.unwrap_or(0.1),
            )
            .into()
        })
        .await
    }

    /// Returns the 2D layout position of every node (cohesive Fruchterman-Reingold).
    pub async fn cohesive_fruchterman_reingold(
        &self,
        #[graphql(desc = "Number of iterations to run. Defaults to 100.")] iter_count: Option<u64>,
        #[graphql(desc = "Scale of the layout. Defaults to 1.0.")] scale: Option<f64>,
        #[graphql(desc = "Initial node size. Defaults to 1.0.")] node_start_size: Option<f64>,
        #[graphql(desc = "Cooloff factor. Defaults to 0.95.")] cooloff_factor: Option<f64>,
        #[graphql(desc = "Time step. Defaults to 0.1.")] dt: Option<f64>,
    ) -> GqlNodeState {
        self.run(move |graph| {
            cohesive_fruchterman_reingold(
                &graph,
                iter_count.unwrap_or(100),
                scale.unwrap_or(1.0),
                node_start_size.unwrap_or(1.0),
                cooloff_factor.unwrap_or(0.95),
                dt.unwrap_or(0.1),
            )
            .into()
        })
        .await
    }

    /// Returns the local temporal three-node motif counts of every node.
    pub async fn local_temporal_three_node_motifs(
        &self,
        #[graphql(desc = "Maximum time difference between the first and last edge of a motif.")]
        delta: i64,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> GqlNodeState {
        self.run(move |graph| temporal_three_node_motif(&graph, delta, threads).into())
            .await
    }

    /// Returns the graph-wide temporal three-node motif counts: 40 counts in a
    /// fixed order (8 two-node, 24 star, then 8 triangle motifs).
    pub async fn global_temporal_three_node_motif(
        &self,
        #[graphql(desc = "Maximum time difference between the first and last edge of a motif.")]
        delta: i64,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Vec<usize> {
        self.run(move |graph| global_temporal_three_node_motif(&graph, delta, threads).to_vec())
            .await
    }

    /// Returns the graph-wide temporal three-node motif counts for each of
    /// `deltas`, one row of 40 counts per delta, in the order given.
    pub async fn global_temporal_three_node_motif_multi(
        &self,
        #[graphql(desc = "Maximum time differences to compute the motif counts for.")] deltas: Vec<
            i64,
        >,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Vec<GqlMotifCounts> {
        self.run(move |graph| {
            temporal_three_node_motif_multi(&graph, deltas.clone(), threads)
                .into_iter()
                .zip(deltas)
                .map(|(res, delta)| GqlMotifCounts {
                    delta,
                    counts: res.to_vec(),
                })
                .collect()
        })
        .await
    }

    /// Returns the temporal rich club coefficient: the maximal density among the
    /// nodes of degree at least `k` that persists over `windowSize` consecutive
    /// snapshots. The snapshots are the rolling windows described by
    /// `rollingWindow` / `rollingStep`.
    pub async fn temporal_rich_club_coefficient(
        &self,
        #[graphql(desc = "Minimum degree a node must have to be in the rich club.")] k: usize,
        #[graphql(desc = "Number of consecutive snapshots the edges must persist over.")]
        window_size: usize,
        #[graphql(desc = "Width of each snapshot.")] rolling_window: WindowDuration,
        #[graphql(
            desc = "Optional gap between the start of one snapshot and the next. Defaults to `rollingWindow`, i.e. non-overlapping snapshots."
        )]
        rolling_step: Option<WindowDuration>,
    ) -> Result<f64, GraphError> {
        self.run(move |graph| {
            Ok::<_, GraphError>(temporal_rich_club_coefficient(
                &graph,
                graph.rolling(rolling_window, rolling_step)?,
                k,
                window_size,
            ))
        })
        .await
    }

    /// Simulates an SEIR epidemic, returning the infection, activation and
    /// recovery times of every node that was infected.
    pub async fn temporal_seir(
        &self,
        #[graphql(desc = "How the initially infected nodes are chosen.")] seeds: GqlSeeds,
        #[graphql(
            desc = "Probability that an encounter between an active and a susceptible node infects it."
        )]
        infection_prob: f64,
        #[graphql(desc = "Time of the initial infection.")] initial_infection: GqlTimeInput,
        #[graphql(desc = "Rate at which infected nodes recover. If unset, nodes never recover.")]
        recovery_rate: Option<f64>,
        #[graphql(
            desc = "Rate at which infected nodes become infectious. If unset, they are infectious immediately."
        )]
        incubation_rate: Option<f64>,
        #[graphql(desc = "Seed for the random number generator. If unset, seeded from the OS.")]
        rng_seed: Option<u64>,
    ) -> Result<GqlNodeState, GraphError> {
        Ok(self
            .run(move |graph| {
                let mut rng = match rng_seed {
                    Some(seed) => StdRng::seed_from_u64(seed),
                    None => StdRng::from_os_rng(),
                };
                temporal_SEIR(
                    &graph,
                    recovery_rate,
                    incubation_rate,
                    infection_prob,
                    initial_infection,
                    seeds,
                    &mut rng,
                )
            })
            .await?
            .into())
    }

    /// Returns a maximum weight matching of the graph, treated as undirected.
    pub async fn max_weight_matching(
        &self,
        #[graphql(desc = "Edge property to use as weight. If unset, all edges have weight 1.")]
        weight_prop: Option<String>,
        #[graphql(desc = "Only consider maximum-cardinality matchings. Defaults to false.")]
        max_cardinality: Option<bool>,
        #[graphql(desc = "Verify that the matching found is optimum. Defaults to false.")]
        verify_optimum: Option<bool>,
    ) -> GqlMatching {
        self.run(move |graph| {
            max_weight_matching(
                &graph,
                weight_prop.as_str(),
                max_cardinality.unwrap_or(false),
                verify_optimum.unwrap_or(false),
            )
            .into()
        })
        .await
    }
}
