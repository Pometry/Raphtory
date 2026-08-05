//! The `Graph.algorithm` field resolvers: one per algorithm exposed through the GraphQL API.

use crate::model::{
    algorithms::{
        alternating_mask::{GqlAlternatingMask, GqlAlternatingMaskArgs},
        bipartite::max_weight_matching::{GqlMaxWeightMatching, GqlMaxWeightMatchingArgs},
        centrality::{
            betweenness_centrality::{GqlBetweennessCentrality, GqlBetweennessCentralityArgs},
            degree_centrality::{GqlDegreeCentrality, GqlDegreeCentralityArgs},
            hits::{GqlHits, GqlHitsArgs},
            pagerank::{GqlPagerank, GqlPagerankArgs},
        },
        community_detection::{
            label_propagation::{GqlLabelPropagation, GqlLabelPropagationArgs},
            louvain::{GqlLouvain, GqlLouvainArgs},
        },
        components::{
            in_component::{GqlInComponent, GqlInComponentArgs},
            in_components::{GqlInComponents, GqlInComponentsArgs},
            out_component::{GqlOutComponent, GqlOutComponentArgs},
            out_components::{GqlOutComponents, GqlOutComponentsArgs},
            strongly_connected_components::{
                GqlStronglyConnectedComponents, GqlStronglyConnectedComponentsArgs,
            },
            weakly_connected_components::{
                GqlWeaklyConnectedComponents, GqlWeaklyConnectedComponentsArgs,
            },
        },
        dynamics::temporal::temporal_seir::{GqlSeeds, GqlTemporalSeir, GqlTemporalSeirArgs},
        embeddings::fast_rp::{GqlFastRp, GqlFastRpArgs},
        executable::GqlAlgorithms,
        inputs::GqlDirection,
        layout::{
            cohesive_fruchterman_reingold::{
                GqlCohesiveFruchtermanReingold, GqlCohesiveFruchtermanReingoldArgs,
            },
            fruchterman_reingold::{GqlFruchtermanReingold, GqlFruchtermanReingoldArgs},
        },
        metrics::{
            all_local_reciprocity::{GqlAllLocalReciprocity, GqlAllLocalReciprocityArgs},
            average_degree::{GqlAverageDegree, GqlAverageDegreeArgs},
            balance::{GqlBalance, GqlBalanceArgs},
            clustering_coefficient::{
                global_clustering_coefficient::{
                    GqlGlobalClusteringCoefficient, GqlGlobalClusteringCoefficientArgs,
                },
                local_clustering_coefficient::{
                    GqlLocalClusteringCoefficient, GqlLocalClusteringCoefficientArgs,
                },
                local_clustering_coefficient_batch::{
                    GqlLocalClusteringCoefficientBatch, GqlLocalClusteringCoefficientBatchArgs,
                },
            },
            directed_graph_density::{GqlDirectedGraphDensity, GqlDirectedGraphDensityArgs},
            global_reciprocity::{GqlGlobalReciprocity, GqlGlobalReciprocityArgs},
            max_degree::{GqlMaxDegree, GqlMaxDegreeArgs},
            max_in_degree::{GqlMaxInDegree, GqlMaxInDegreeArgs},
            max_out_degree::{GqlMaxOutDegree, GqlMaxOutDegreeArgs},
            min_degree::{GqlMinDegree, GqlMinDegreeArgs},
            min_in_degree::{GqlMinInDegree, GqlMinInDegreeArgs},
            min_out_degree::{GqlMinOutDegree, GqlMinOutDegreeArgs},
        },
        motifs::{
            global_temporal_three_node_motif::{
                GqlGlobalTemporalThreeNodeMotif, GqlGlobalTemporalThreeNodeMotifArgs,
            },
            global_temporal_three_node_motif_multi::{
                GqlGlobalTemporalThreeNodeMotifMulti, GqlGlobalTemporalThreeNodeMotifMultiArgs,
                GqlMotifCounts,
            },
            local_temporal_three_node_motifs::{
                GqlLocalTemporalThreeNodeMotifs, GqlLocalTemporalThreeNodeMotifsArgs,
            },
            local_triangle_count::{GqlLocalTriangleCount, GqlLocalTriangleCountArgs},
            temporal_rich_club_coefficient::{
                GqlTemporalRichClubCoefficient, GqlTemporalRichClubCoefficientArgs,
            },
            triangle_count::{GqlTriangleCount, GqlTriangleCountArgs},
            triplet_count::{GqlTripletCount, GqlTripletCountArgs},
        },
        pathing::{
            dijkstra::{GqlDijkstra, GqlDijkstraArgs},
            single_source_shortest_path::{
                GqlSingleSourceShortestPath, GqlSingleSourceShortestPathArgs,
            },
            temporally_reachable_nodes::{
                GqlTemporallyReachableNodes, GqlTemporallyReachableNodesArgs,
            },
        },
    },
    graph::{
        filtering::GqlViewFilter, matching::GqlMatching, node_id::GqlNodeId,
        node_state::GqlNodeState, timeindex::GqlTimeInput, WindowDuration,
    },
};
use dynamic_graphql::ResolvedObjectFields;
use raphtory::errors::GraphError;

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

    /// Returns the local triangle count of a single node (0 if it has degree < 2), or null if
    /// the node does not exist in the view.
    async fn local_triangle_count(
        &self,
        #[graphql(desc = "Node id.")] node: GqlNodeId,
        #[graphql(
            desc = "Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view."
        )]
        filter: Option<GqlViewFilter>,
    ) -> Result<Option<usize>, GraphError> {
        self.run::<GqlLocalTriangleCount>(GqlLocalTriangleCountArgs { node, filter })
            .await
    }

    /// Returns the local clustering coefficient of a single node (0 if it has degree < 2), or
    /// null if the node does not exist in the view.
    async fn local_clustering_coefficient(
        &self,
        #[graphql(desc = "Node id.")] node: GqlNodeId,
        #[graphql(
            desc = "Optional composite filter (node, edge, and graph-view); the algorithm runs on the resulting view."
        )]
        filter: Option<GqlViewFilter>,
    ) -> Result<Option<f64>, GraphError> {
        self.run::<GqlLocalClusteringCoefficient>(GqlLocalClusteringCoefficientArgs {
            node,
            filter,
        })
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

    /// Returns the maximum (undirected) degree of any node in the graph.
    async fn max_degree(&self) -> Result<usize, GraphError> {
        self.run::<GqlMaxDegree>(GqlMaxDegreeArgs).await
    }

    /// Returns the minimum (undirected) degree of any node in the graph.
    async fn min_degree(&self) -> Result<usize, GraphError> {
        self.run::<GqlMinDegree>(GqlMinDegreeArgs).await
    }

    /// Returns the maximum out-degree of any node in the graph.
    async fn max_out_degree(&self) -> Result<usize, GraphError> {
        self.run::<GqlMaxOutDegree>(GqlMaxOutDegreeArgs).await
    }

    /// Returns the maximum in-degree of any node in the graph.
    async fn max_in_degree(&self) -> Result<usize, GraphError> {
        self.run::<GqlMaxInDegree>(GqlMaxInDegreeArgs).await
    }

    /// Returns the minimum out-degree of any node in the graph.
    async fn min_out_degree(&self) -> Result<usize, GraphError> {
        self.run::<GqlMinOutDegree>(GqlMinOutDegreeArgs).await
    }

    /// Returns the minimum in-degree of any node in the graph.
    async fn min_in_degree(&self) -> Result<usize, GraphError> {
        self.run::<GqlMinInDegree>(GqlMinInDegreeArgs).await
    }

    /// Returns the number of connected triplets (paths of length 2) in the graph.
    async fn triplet_count(
        &self,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<usize, GraphError> {
        self.run::<GqlTripletCount>(GqlTripletCountArgs { threads })
            .await
    }

    /// Returns the number of triangles in the graph.
    async fn triangle_count(
        &self,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<usize, GraphError> {
        self.run::<GqlTriangleCount>(GqlTriangleCountArgs { threads })
            .await
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

    /// Returns the graph-wide temporal three-node motif counts: 40 counts in a
    /// fixed order (8 two-node, 24 star, then 8 triangle motifs).
    async fn global_temporal_three_node_motif(
        &self,
        #[graphql(desc = "Maximum time difference between the first and last edge of a motif.")]
        delta: i64,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<Vec<usize>, GraphError> {
        self.run::<GqlGlobalTemporalThreeNodeMotif>(GqlGlobalTemporalThreeNodeMotifArgs {
            delta,
            threads,
        })
        .await
    }

    /// Returns the graph-wide temporal three-node motif counts for each of
    /// `deltas`, one row of 40 counts per delta, in the order given.
    async fn global_temporal_three_node_motif_multi(
        &self,
        #[graphql(desc = "Maximum time differences to compute the motif counts for.")] deltas: Vec<
            i64,
        >,
        #[graphql(desc = "Number of threads to use. Defaults to all available.")] threads: Option<
            usize,
        >,
    ) -> Result<Vec<GqlMotifCounts>, GraphError> {
        self.run::<GqlGlobalTemporalThreeNodeMotifMulti>(GqlGlobalTemporalThreeNodeMotifMultiArgs {
            deltas,
            threads,
        })
        .await
    }

    /// Returns the temporal rich club coefficient: the maximal density among the
    /// nodes of degree at least `k` that persists over `windowSize` consecutive
    /// snapshots. The snapshots are the rolling windows described by
    /// `rollingWindow` / `rollingStep`.
    async fn temporal_rich_club_coefficient(
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
        self.run::<GqlTemporalRichClubCoefficient>(GqlTemporalRichClubCoefficientArgs {
            k,
            window_size,
            rolling_window,
            rolling_step,
        })
        .await
    }

    /// Returns an alternating boolean mask over the nodes.
    async fn alternating_mask(&self) -> Result<GqlNodeState, GraphError> {
        self.run::<GqlAlternatingMask>(GqlAlternatingMaskArgs).await
    }

    /// Simulates an SEIR epidemic, returning the infection, activation and
    /// recovery times of every node that was infected.
    async fn temporal_seir(
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
        self.run::<GqlTemporalSeir>(GqlTemporalSeirArgs {
            seeds,
            infection_prob,
            initial_infection,
            recovery_rate,
            incubation_rate,
            rng_seed,
        })
        .await
    }

    /// Returns a maximum weight matching of the graph, treated as undirected.
    async fn max_weight_matching(
        &self,
        #[graphql(desc = "Edge property to use as weight. If unset, all edges have weight 1.")]
        weight_prop: Option<String>,
        #[graphql(desc = "Only consider maximum-cardinality matchings. Defaults to false.")]
        max_cardinality: Option<bool>,
        #[graphql(desc = "Verify that the matching found is optimum. Defaults to false.")]
        verify_optimum: Option<bool>,
    ) -> Result<GqlMatching, GraphError> {
        self.run::<GqlMaxWeightMatching>(GqlMaxWeightMatchingArgs {
            weight_prop,
            max_cardinality: max_cardinality.unwrap_or(false),
            verify_optimum: verify_optimum.unwrap_or(false),
        })
        .await
    }
}
