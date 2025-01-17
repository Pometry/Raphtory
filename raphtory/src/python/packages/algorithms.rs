#![allow(non_snake_case)]

use crate::{
    algorithms::{
        algorithm_result::AlgorithmResult,
        bipartite::max_weight_matching::max_weight_matching as mwm,
        centrality::{
            betweenness::betweenness_centrality as betweenness_rs,
            degree_centrality::degree_centrality as degree_centrality_rs, hits::hits as hits_rs,
            pagerank::unweighted_page_rank,
        },
        community_detection::{
            label_propagation::label_propagation as label_propagation_rs,
            louvain::louvain as louvain_rs, modularity::ModularityUnDir,
        },
        components,
        dynamics::temporal::epidemics::{temporal_SEIR as temporal_SEIR_rs, Infected, SeedError},
        embeddings::fast_rp as fast_rp_rs,
        layout::{
            cohesive_fruchterman_reingold::cohesive_fruchterman_reingold as cohesive_fruchterman_reingold_rs,
            fruchterman_reingold::fruchterman_reingold_unbounded as fruchterman_reingold_rs,
        },
        metrics::{
            balance::balance as balance_rs,
            degree::{
                average_degree as average_degree_rs, max_degree as max_degree_rs,
                max_in_degree as max_in_degree_rs, max_out_degree as max_out_degree_rs,
                min_degree as min_degree_rs, min_in_degree as min_in_degree_rs,
                min_out_degree as min_out_degree_rs,
            },
            directed_graph_density::directed_graph_density as directed_graph_density_rs,
            local_clustering_coefficient::local_clustering_coefficient as local_clustering_coefficient_rs,
            reciprocity::{
                all_local_reciprocity as all_local_reciprocity_rs,
                global_reciprocity as global_reciprocity_rs,
            },
        },
        motifs::{
            global_temporal_three_node_motifs::{
                global_temporal_three_node_motif as global_temporal_three_node_motif_rs,
                temporal_three_node_motif_multi as global_temporal_three_node_motif_general_rs,
            },
            local_temporal_three_node_motifs::temporal_three_node_motif as local_three_node_rs,
            local_triangle_count::local_triangle_count as local_triangle_count_rs,
            temporal_rich_club_coefficient::temporal_rich_club_coefficient as temporal_rich_club_rs,
        },
        pathing::{
            dijkstra::dijkstra_single_source_shortest_paths as dijkstra_single_source_shortest_paths_rs,
            single_source_shortest_path::single_source_shortest_path as single_source_shortest_path_rs,
            temporal_reachability::temporally_reachable_nodes as temporal_reachability_rs,
        },
        projections::temporal_bipartite_projection::temporal_bipartite_projection as temporal_bipartite_rs,
    },
    core::Prop,
    db::{api::view::internal::DynamicGraph, graph::node::NodeView},
    python::{
        graph::{node::PyNode, views::graph_view::PyGraphView},
        utils::{PyNodeRef, PyTime},
    },
};
use ordered_float::OrderedFloat;
use pyo3::prelude::*;
use rand::{prelude::StdRng, SeedableRng};
use raphtory_api::core::{entities::GID, Direction};
use std::collections::{HashMap, HashSet};

#[cfg(feature = "storage")]
use crate::python::graph::disk_graph::PyDiskGraph;
use crate::{
    algorithms::bipartite::max_weight_matching::Matching, core::utils::errors::GraphError,
    db::api::state::NodeState, prelude::Graph,
};
#[cfg(feature = "storage")]
use pometry_storage::algorithms::connected_components::connected_components as connected_components_rs;

/// Implementations of various graph algorithms that can be run on a graph.
///
/// To run an algorithm simply import the module and call the function with the graph as the argument
///

/// Local triangle count - calculates the number of triangles (a cycle of length 3) a node participates in.
///
/// This function returns the number of pairs of neighbours of a given node which are themselves connected.
///
/// Arguments:
///     g (GraphView) : Raphtory graph, this can be directed or undirected but will be treated as undirected
///     v (NodeInput) : node id or name
///
/// Returns:
///     int : number of triangles associated with node v
///
#[pyfunction]
pub fn local_triangle_count(g: &PyGraphView, v: PyNodeRef) -> Option<usize> {
    local_triangle_count_rs(&g.graph, v)
}

/// Weakly connected components -- partitions the graph into node sets which are mutually reachable by an undirected path
///
/// This function assigns a component id to each node such that nodes with the same component id are mutually reachable
/// by an undirected path.
///
/// Arguments:
///     g (GraphView) : Raphtory graph
///     iter_count (int) : Maximum number of iterations to run. Note that this will terminate early if the labels converge prior to the number of iterations being reached.
///
/// Returns:
///     AlgorithmResult : AlgorithmResult object mapping nodes to their component ids.
#[pyfunction]
#[pyo3(signature = (g, iter_count=9223372036854775807))]
pub fn weakly_connected_components(
    g: &PyGraphView,
    iter_count: usize,
) -> AlgorithmResult<DynamicGraph, GID, GID> {
    components::weakly_connected_components(&g.graph, iter_count, None)
}

/// Strongly connected components
///
/// Partitions the graph into node sets which are mutually reachable by an directed path
///
/// Arguments:
///     g (GraphView) : Raphtory graph
///
/// Returns:
///     list[list[int]] : List of strongly connected nodes identified by ids
#[pyfunction]
#[pyo3(signature = (g))]
pub fn strongly_connected_components(
    g: &PyGraphView,
) -> AlgorithmResult<DynamicGraph, usize, usize> {
    components::strongly_connected_components(&g.graph)
}

#[cfg(feature = "storage")]
#[pyfunction]
#[pyo3(signature = (g))]
pub fn connected_components(g: &PyDiskGraph) -> Vec<usize> {
    connected_components_rs(g.graph.as_ref())
}

/// In components -- Finding the "in-component" of a node in a directed graph involves identifying all nodes that can be reached following only incoming edges.
///
/// Arguments:
///     g (GraphView) : Raphtory graph
///
/// Returns:
///     AlgorithmResult : AlgorithmResult object mapping each node to an array containing the ids of all nodes within their 'in-component'
#[pyfunction]
#[pyo3(signature = (g))]
pub fn in_components(g: &PyGraphView) -> AlgorithmResult<DynamicGraph, Vec<GID>, Vec<GID>> {
    components::in_components(&g.graph, None)
}

/// In component -- Finding the "in-component" of a node in a directed graph involves identifying all nodes that can be reached following only incoming edges.
///
/// Arguments:
///     node (Node) : The node whose in-component we wish to calculate
///
/// Returns:
///    An array containing the Nodes within the given nodes in-component
#[pyfunction]
#[pyo3(signature = (node))]
pub fn in_component(node: &PyNode) -> NodeState<'static, usize, DynamicGraph> {
    components::in_component(node.node.clone())
}

/// Out components -- Finding the "out-component" of a node in a directed graph involves identifying all nodes that can be reached following only outgoing edges.
///
/// Arguments:
///     g (GraphView) : Raphtory graph
///
/// Returns:
///     AlgorithmResult : AlgorithmResult object mapping each node to an array containing the ids of all nodes within their 'out-component'
#[pyfunction]
#[pyo3(signature = (g))]
pub fn out_components(g: &PyGraphView) -> AlgorithmResult<DynamicGraph, Vec<GID>, Vec<GID>> {
    components::out_components(&g.graph, None)
}

/// Out component -- Finding the "out-component" of a node in a directed graph involves identifying all nodes that can be reached following only outgoing edges.
///
/// Arguments:
///     node (Node) : The node whose out-component we wish to calculate
///
/// Returns:
///    NodeStateUsize: A NodeState mapping the nodes in the out-component to their distance from the starting node.
#[pyfunction]
#[pyo3(signature = (node))]
pub fn out_component(node: &PyNode) -> NodeState<'static, usize, DynamicGraph> {
    components::out_component(node.node.clone())
}

/// Pagerank -- pagerank centrality value of the nodes in a graph
///
/// This function calculates the Pagerank value of each node in a graph. See https://en.wikipedia.org/wiki/PageRank for more information on PageRank centrality.
/// A default damping factor of 0.85 is used. This is an iterative algorithm which terminates if the sum of the absolute difference in pagerank values between iterations
/// is less than the max diff value given.
///
/// Arguments:
///     g (GraphView) : Raphtory graph
///     iter_count (int) : Maximum number of iterations to run. Note that this will terminate early if convergence is reached.
///     max_diff (Optional[float]) : Optional parameter providing an alternative stopping condition.
///         The algorithm will terminate if the sum of the absolute difference in pagerank values between iterations
///         is less than the max diff value given.
///
/// Returns:
///     AlgorithmResult : AlgorithmResult with string keys and float values mapping node names to their pagerank value.
#[pyfunction]
#[pyo3(signature = (g, iter_count=20, max_diff=None, use_l2_norm=true, damping_factor=0.85))]
pub fn pagerank(
    g: &PyGraphView,
    iter_count: usize,
    max_diff: Option<f64>,
    use_l2_norm: bool,
    damping_factor: Option<f64>,
) -> AlgorithmResult<DynamicGraph, f64, OrderedFloat<f64>> {
    unweighted_page_rank(
        &g.graph,
        Some(iter_count),
        None,
        max_diff,
        use_l2_norm,
        damping_factor,
    )
}

/// Temporally reachable nodes -- the nodes that are reachable by a time respecting path followed out from a set of seed nodes at a starting time.
///
/// This function starts at a set of seed nodes and follows all time respecting paths until either a) a maximum number of hops is reached, b) one of a set of
/// stop nodes is reached, or c) no further time respecting edges exist. A time respecting path is a sequence of nodes v_1, v_2, ... , v_k such that there exists
/// a sequence of edges (v_i, v_i+1, t_i) with t_i < t_i+1 for i = 1, ... , k - 1.
///
/// Arguments:
///     g (GraphView) : directed Raphtory graph
///     max_hops (int) : maximum number of hops to propagate out
///     start_time (int) : time at which to start the path (such that t_1 > start_time for any path starting from these seed nodes)
///     seed_nodes (list[NodeInput]) : list of node names or ids which should be the starting nodes
///     stop_nodes (Optional[list[NodeInput]]) : nodes at which a path shouldn't go any further
///
/// Returns:
///     AlgorithmResult : AlgorithmResult with string keys and float values mapping node names to their pagerank value.
#[pyfunction]
#[pyo3(signature = (g, max_hops, start_time, seed_nodes, stop_nodes=None))]
pub fn temporally_reachable_nodes(
    g: &PyGraphView,
    max_hops: usize,
    start_time: i64,
    seed_nodes: Vec<PyNodeRef>,
    stop_nodes: Option<Vec<PyNodeRef>>,
) -> AlgorithmResult<DynamicGraph, Vec<(i64, String)>, Vec<(i64, String)>> {
    temporal_reachability_rs(&g.graph, None, max_hops, start_time, seed_nodes, stop_nodes)
}

/// Local clustering coefficient - measures the degree to which nodes in a graph tend to cluster together.
///
/// The proportion of pairs of neighbours of a node who are themselves connected.
///
/// Arguments:
///     g (GraphView) : Raphtory graph, can be directed or undirected but will be treated as undirected.
///     v (NodeInput): node id or name
///
/// Returns:
///     float : the local clustering coefficient of node v in g.
#[pyfunction]
pub fn local_clustering_coefficient(g: &PyGraphView, v: PyNodeRef) -> Option<f32> {
    local_clustering_coefficient_rs(&g.graph, v)
}

/// Graph density - measures how dense or sparse a graph is.
///
/// The ratio of the number of directed edges in the graph to the total number of possible directed
/// edges (given by N * (N-1) where N is the number of nodes).
///
/// Arguments:
///     g (GraphView) : a directed Raphtory graph
///
/// Returns:
///     float : Directed graph density of G.
#[pyfunction]
pub fn directed_graph_density(g: &PyGraphView) -> f32 {
    directed_graph_density_rs(&g.graph)
}

/// The average (undirected) degree of all nodes in the graph.
///
/// Note that this treats the graph as simple and undirected and is equal to twice
/// the number of undirected edges divided by the number of nodes.
///
/// Arguments:
///     g (GraphView) : a Raphtory graph
///
/// Returns:
///     float : the average degree of the nodes in the graph
#[pyfunction]
pub fn average_degree(g: &PyGraphView) -> f64 {
    average_degree_rs(&g.graph)
}

/// The maximum out degree of any node in the graph.
///
/// Arguments:
///     g (GraphView) : a directed Raphtory graph
///
/// Returns:
///     int : value of the largest outdegree
#[pyfunction]
pub fn max_out_degree(g: &PyGraphView) -> usize {
    max_out_degree_rs(&g.graph)
}

/// The maximum in degree of any node in the graph.
///
/// Arguments:
///     g (GraphView) : a directed Raphtory graph
///
/// Returns:
///     int : value of the largest indegree
#[pyfunction]
pub fn max_in_degree(g: &PyGraphView) -> usize {
    max_in_degree_rs(&g.graph)
}

/// The minimum out degree of any node in the graph.
///
/// Arguments:
///     g (GraphView) : a directed Raphtory graph
///
/// Returns:
///     int : value of the smallest outdegree
#[pyfunction]
pub fn min_out_degree(g: &PyGraphView) -> usize {
    min_out_degree_rs(&g.graph)
}

/// The minimum in degree of any node in the graph.
///
/// Arguments:
///     g (GraphView) : a directed Raphtory graph
///
/// Returns:
///     int : value of the smallest indegree
#[pyfunction]
pub fn min_in_degree(g: &PyGraphView) -> usize {
    min_in_degree_rs(&g.graph)
}

/// Reciprocity - measure of the symmetry of relationships in a graph, the global reciprocity of
/// the entire graph.
/// This calculates the number of reciprocal connections (edges that go in both directions) in a
/// graph and normalizes it by the total number of directed edges.
///
/// Arguments:
///     g (GraphView) : a directed Raphtory graph
///
/// Returns:
///     float : reciprocity of the graph between 0 and 1.

#[pyfunction]
pub fn global_reciprocity(g: &PyGraphView) -> f64 {
    global_reciprocity_rs(&g.graph, None)
}

/// Local reciprocity - measure of the symmetry of relationships associated with a node
///
/// This measures the proportion of a node's outgoing edges which are reciprocated with an incoming edge.
///
/// Arguments:
///     g (GraphView) : a directed Raphtory graph
///
/// Returns:
///     AlgorithmResult : AlgorithmResult with string keys and float values mapping each node name to its reciprocity value.
///
#[pyfunction]
pub fn all_local_reciprocity(
    g: &PyGraphView,
) -> AlgorithmResult<DynamicGraph, f64, OrderedFloat<f64>> {
    all_local_reciprocity_rs(&g.graph, None)
}

/// Computes the number of connected triplets within a graph
///
/// A connected triplet (also known as a wedge, 2-hop path) is a pair of edges with one node in common. For example, the triangle made up of edges
/// A-B, B-C, C-A is formed of three connected triplets.
///
/// Arguments:
///     g (GraphView) : a Raphtory graph, treated as undirected
///
/// Returns:
///     int : the number of triplets in the graph
#[pyfunction]
pub fn triplet_count(g: &PyGraphView) -> usize {
    crate::algorithms::motifs::triplet_count::triplet_count(&g.graph, None)
}

/// Computes the global clustering coefficient of a graph. The global clustering coefficient is
/// defined as the number of triangles in the graph divided by the number of triplets in the graph.
///
/// Note that this is also known as transitivity and is different to the average clustering coefficient.
///
/// Arguments:
///     g (GraphView) : a Raphtory graph, treated as undirected
///
/// Returns:
///     float : the global clustering coefficient of the graph
///
/// See also:
///     [`Triplet Count`](triplet_count)
#[pyfunction]
pub fn global_clustering_coefficient(g: &PyGraphView) -> f64 {
    crate::algorithms::metrics::clustering_coefficient::clustering_coefficient(&g.graph)
}

/// Computes the number of three edge, up-to-three node delta-temporal motifs in the graph, using the algorithm of Paranjape et al, Motifs in Temporal Networks (2017).
/// We point the reader to this reference for more information on the algorithm and background, but provide a short summary below.
///
///  Motifs included:
///
///  Stars
///
///  There are three classes (in the order they are outputted) of star motif on three nodes based on the switching behaviour of the edges between the two leaf nodes.
///
///   - PRE: Stars of the form i<->j, i<->j, i<->k (ie two interactions with leaf j followed by one with leaf k)
///   - MID: Stars of the form i<->j, i<->k, i<->j (ie switching interactions from leaf j to leaf k, back to j again)
///   - POST: Stars of the form i<->j, i<->k, i<->k (ie one interaction with leaf j followed by two with leaf k)
///
///  Within each of these classes is 8 motifs depending on the direction of the first to the last edge -- incoming "I" or outgoing "O".
///  These are enumerated in the order III, IIO, IOI, IOO, OII, OIO, OOI, OOO (like binary with "I"-0 and "O"-1).
///
///  Two node motifs:
///
///  Also included are two node motifs, of which there are 8 when counted from the perspective of each node. These are characterised by the direction of each edge, enumerated
///  in the above order. Note that for the global graph counts, each motif is counted in both directions (a single III motif for one node is an OOO motif for the other node).
///
///  Triangles:
///
///  There are 8 triangle motifs:
///
///   1. i --> j, k --> j, i --> k
///   2. i --> j, k --> j, k --> i
///   3. i --> j, j --> k, i --> k
///   4. i --> j, j --> k, k --> i
///   5. i --> j, k --> i, j --> k
///   6. i --> j, k --> i, k --> j
///   7. i --> j, i --> k, j --> k
///   8. i --> j, i --> k, k --> j
///
/// Arguments:
///     g (GraphView) : A directed raphtory graph
///     delta (int): Maximum time difference between the first and last edge of the motif. NB if time for edges was given as a UNIX epoch, this should be given in seconds, otherwise milliseconds should be used (if edge times were given as string)
///
/// Returns:
///     list : A 40 dimensional array with the counts of each motif, given in the same order as described above. Note that the two-node motif counts are symmetrical so it may be more useful just to consider the first four elements.
///
/// Notes:
///     This is achieved by calling the local motif counting algorithm, summing the resulting arrays and dealing with overcounted motifs: the triangles (by dividing each motif count by three) and two-node motifs (dividing by two).
///
#[pyfunction]
pub fn global_temporal_three_node_motif(g: &PyGraphView, delta: i64) -> [usize; 40] {
    global_temporal_three_node_motif_rs(&g.graph, delta, None)
}

/// Projects a temporal bipartite graph into an undirected temporal graph over the pivot node type. Let `G` be a bipartite graph with node types `A` and `B`. Given `delta > 0`, the projection graph `G'` pivoting over type `B` nodes,
/// will make a connection between nodes `n1` and `n2` (of type `A`) at time `(t1 + t2)/2` if they respectively have an edge at time `t1`, `t2` with the same node of type `B` in `G`, and `|t2-t1| < delta`.
///
/// Arguments:
///     g (GraphView) : A directed raphtory graph
///     delta (int): Time period
///     pivot (str) : node type to pivot over. If a bipartite graph has types `A` and `B`, and `B` is the pivot type, the new graph will consist of type `A` nodes.
///
/// Returns:
///     Graph: Projected (unipartite) temporal graph.
#[pyfunction]
#[pyo3(signature = (g, delta, pivot_type))]
pub fn temporal_bipartite_graph_projection(
    g: &PyGraphView,
    delta: i64,
    pivot_type: String,
) -> Graph {
    temporal_bipartite_rs(&g.graph, delta, pivot_type)
}

/// Computes the global counts of three-edge up-to-three node temporal motifs for a range of timescales. See `global_temporal_three_node_motif` for an interpretation of each row returned.
///
/// Arguments:
///     g (GraphView) : A directed raphtory graph
///     deltas(list[int]): A list of delta values to use.
///
/// Returns:
///     list[list[int]] : A list of 40d arrays, each array is the motif count for a particular value of delta, returned in the order that the deltas were given as input.
#[pyfunction]
pub fn global_temporal_three_node_motif_multi(
    g: &PyGraphView,
    deltas: Vec<i64>,
) -> Vec<[usize; 40]> {
    global_temporal_three_node_motif_general_rs(&g.graph, deltas, None)
}

/// Computes the number of each type of motif that each node participates in. See global_temporal_three_node_motifs for a summary of the motifs involved.
///
/// Arguments:
///     g (GraphView) : A directed raphtory graph
///     delta (int): Maximum time difference between the first and last edge of the motif. NB if time for edges was given as a UNIX epoch, this should be given in seconds, otherwise milliseconds should be used (if edge times were given as string)
///
/// Returns:
///     dict : A dictionary with node ids as keys and a 40d array of motif counts as values (in the same order as the global motif counts) with the number of each motif that node participates in.
///
/// Notes:
///     For this local count, a node is counted as participating in a motif in the following way. For star motifs, only the centre node counts
///    the motif. For two node motifs, both constituent nodes count the motif. For triangles, all three constituent nodes count the motif.
#[pyfunction]
pub fn local_temporal_three_node_motifs(
    g: &PyGraphView,
    delta: i64,
) -> HashMap<String, Vec<usize>> {
    local_three_node_rs(&g.graph, vec![delta], None)
        .into_iter()
        .map(|(k, v)| (String::from(k), v[0].clone()))
        .collect::<HashMap<String, Vec<usize>>>()
}

/// HITS (Hubs and Authority) Algorithm:
///
/// AuthScore of a node (A) = Sum of HubScore of all nodes pointing at node (A) from previous iteration /
/// Sum of HubScore of all nodes in the current iteration
///
/// HubScore of a node (A) = Sum of AuthScore of all nodes pointing away from node (A) from previous iteration /
/// Sum of AuthScore of all nodes in the current iteration
///
/// Arguments:
///     g (GraphView): Graph to run the algorithm on
///     iter_count (int): How many iterations to run the algorithm
///     threads (int, optional): Number of threads to use
///
/// Returns:
///     AlgorithmResult: An AlgorithmResult object containing the mapping from node ID to the hub and authority score of the node
#[pyfunction]
#[pyo3(signature = (g, iter_count=20, threads=None))]
pub fn hits(
    g: &PyGraphView,
    iter_count: usize,
    threads: Option<usize>,
) -> AlgorithmResult<DynamicGraph, (f32, f32), (OrderedFloat<f32>, OrderedFloat<f32>)> {
    hits_rs(&g.graph, iter_count, threads)
}

/// Sums the weights of edges in the graph based on the specified direction.
///
/// This function computes the sum of edge weights based on the direction provided, and can be executed in parallel using a given number of threads.
///
/// Arguments:
///     g (GraphView): The graph view on which the operation is to be performed.
///     name (str): The name of the edge property used as the weight. Defaults to "weight".
///     direction (Direction): Specifies the direction of the edges to be considered for summation. Defaults to "both".
///             * "out": Only consider outgoing edges.
///             * "in": Only consider incoming edges.
///             * "both": Consider both outgoing and incoming edges. This is the default.
///     threads (int, optional): The number of threads to be used for parallel execution.
///
/// Returns:
///     AlgorithmResult: A result containing a mapping of node names to the computed sum of their associated edge weights.
///
#[pyfunction]
#[pyo3[signature = (g, name="weight".to_string(), direction=Direction::BOTH,  threads=None)]]
pub fn balance(
    g: &PyGraphView,
    name: String,
    direction: Direction,
    threads: Option<usize>,
) -> Result<AlgorithmResult<DynamicGraph, f64, OrderedFloat<f64>>, GraphError> {
    balance_rs(&g.graph, name.clone(), direction, threads)
}

/// Computes the degree centrality of all nodes in the graph. The values are normalized
/// by dividing each result with the maximum possible degree. Graphs with self-loops can have
/// values of centrality greater than 1.
///
/// Arguments:
///     g (GraphView): The graph view on which the operation is to be performed.
///     threads (int, optional): The number of threads to be used for parallel execution.
///
/// Returns:
///     AlgorithmResult: A result containing a mapping of node names to the computed sum of their associated degree centrality.
#[pyfunction]
#[pyo3[signature = (g, threads=None)]]
pub fn degree_centrality(
    g: &PyGraphView,
    threads: Option<usize>,
) -> AlgorithmResult<DynamicGraph, f64, OrderedFloat<f64>> {
    degree_centrality_rs(&g.graph, threads)
}

/// Returns the largest degree found in the graph
///
/// Arguments:
///     g (GraphView): The graph view on which the operation is to be performed.
///
/// Returns:
///     int: The largest degree
#[pyfunction]
#[pyo3[signature = (g)]]
pub fn max_degree(g: &PyGraphView) -> usize {
    max_degree_rs(&g.graph)
}

/// Returns the smallest degree found in the graph
///
/// Arguments:
///     g (GraphView): The graph view on which the operation is to be performed.
///
/// Returns:
///     int: The smallest degree found
#[pyfunction]
#[pyo3[signature = (g)]]
pub fn min_degree(g: &PyGraphView) -> usize {
    min_degree_rs(&g.graph)
}

/// Calculates the single source shortest paths from a given source node.
///
/// Arguments:
///     g (GraphView): A reference to the graph. Must implement `GraphViewOps`.
///     source (NodeInput): The source node.
///     cutoff (int, optional): An optional cutoff level. The algorithm will stop if this level is reached.
///
/// Returns:
///     AlgorithmResult: Returns an `AlgorithmResult[str, list[str]]` containing the shortest paths from the source to all reachable nodes.
///
#[pyfunction]
#[pyo3[signature = (g, source, cutoff=None)]]
pub fn single_source_shortest_path(
    g: &PyGraphView,
    source: PyNodeRef,
    cutoff: Option<usize>,
) -> AlgorithmResult<DynamicGraph, Vec<String>, Vec<String>> {
    single_source_shortest_path_rs(&g.graph, source, cutoff)
}

/// Finds the shortest paths from a single source to multiple targets in a graph.
///
/// Arguments:
///     g (GraphView): The graph to search in.
///     source (NodeInput): The source node.
///     targets (list[NodeInput]): A list of target nodes.
///     direction (Direction): The direction of the edges to be considered for the shortest path. Defaults to "both".
///     weight (str): The name of the weight property for the edges. Defaults to "weight".
///
/// Returns:
///     dict: Returns a `Dict` where the key is the target node and the value is a tuple containing the total cost and a vector of nodes representing the shortest path.
///
#[pyfunction]
#[pyo3[signature = (g, source, targets, direction=Direction::BOTH, weight="weight".to_string())]]
pub fn dijkstra_single_source_shortest_paths(
    g: &PyGraphView,
    source: PyNodeRef,
    targets: Vec<PyNodeRef>,
    direction: Direction,
    weight: Option<String>,
) -> PyResult<HashMap<String, (Prop, Vec<String>)>> {
    match dijkstra_single_source_shortest_paths_rs(&g.graph, source, targets, weight, direction) {
        Ok(result) => Ok(result),
        Err(err_msg) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(err_msg)),
    }
}

/// Computes the betweenness centrality for nodes in a given graph.
///
/// Arguments:
///     g (GraphView): A reference to the graph.
///     k (int, optional): Specifies the number of nodes to consider for the centrality computation.
///         All nodes are considered by default.
///     normalized (bool): Indicates whether to normalize the centrality values.
///
/// Returns:
///     AlgorithmResult: Returns an `AlgorithmResult` containing the betweenness centrality of each node.
#[pyfunction]
#[pyo3[signature = (g, k=None, normalized=true)]]
pub fn betweenness_centrality(
    g: &PyGraphView,
    k: Option<usize>,
    normalized: bool,
) -> AlgorithmResult<DynamicGraph, f64, OrderedFloat<f64>> {
    betweenness_rs(&g.graph, k, normalized)
}

/// Computes components using a label propagation algorithm
///
/// Arguments:
///     g (GraphView): A reference to the graph
///     seed (bytes, optional): Array of 32 bytes of u8 which is set as the rng seed
///
/// Returns:
///     list[set[Node]]: A list of sets each containing nodes that have been grouped
///
#[pyfunction]
#[pyo3[signature = (g, seed=None)]]
pub fn label_propagation(
    g: &PyGraphView,
    seed: Option<[u8; 32]>,
) -> PyResult<Vec<HashSet<NodeView<DynamicGraph>>>> {
    match label_propagation_rs(&g.graph, seed) {
        Ok(result) => Ok(result),
        Err(err_msg) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(err_msg)),
    }
}

/// Simulate an SEIR dynamic on the network
///
/// The algorithm uses the event-based sampling strategy from https://doi.org/10.1371/journal.pone.0246961
///
/// Arguments:
///     graph (GraphView): the graph view
///     seeds (int | float | list[NodeInput]): the seeding strategy to use for the initial infection (if `int`, choose fixed number
///            of nodes at random, if `float` infect each node with this probability, if `list`
///            initially infect the specified nodes
///     infection_prob (float): the probability for a contact between infected and susceptible nodes to lead
///                     to a transmission
///     initial_infection (int | str | datetime): the time of the initial infection
///     recovery_rate (float | None): optional recovery rate (if None, simulates SEI dynamic where nodes never recover)
///                    the actual recovery time is sampled from an exponential distribution with this rate
///     incubation_rate ( float | None): optional incubation rate (if None, simulates SI or SIR dynamics where infected
///                      nodes are infectious at the next time step)
///                      the actual incubation time is sampled from an exponential distribution with
///                      this rate
///     rng_seed (int | None): optional seed for the random number generator
///
/// Returns:
///     AlgorithmResult: Returns an `Infected` object for each infected node with attributes
///     
///     `infected`: the time stamp of the infection event
///
///     `active`: the time stamp at which the node actively starts spreading the infection (i.e., the end of the incubation period)
///
///     `recovered`: the time stamp at which the node recovered (i.e., stopped spreading the infection)
///
#[pyfunction(name = "temporal_SEIR")]
#[pyo3(signature = (graph, seeds, infection_prob, initial_infection, recovery_rate=None, incubation_rate=None, rng_seed=None))]
pub fn temporal_SEIR(
    graph: &PyGraphView,
    seeds: crate::python::algorithm::epidemics::PySeed,
    infection_prob: f64,
    initial_infection: PyTime,
    recovery_rate: Option<f64>,
    incubation_rate: Option<f64>,
    rng_seed: Option<u64>,
) -> Result<AlgorithmResult<DynamicGraph, Infected>, SeedError> {
    let mut rng = match rng_seed {
        None => StdRng::from_entropy(),
        Some(seed) => StdRng::seed_from_u64(seed),
    };
    temporal_SEIR_rs(
        &graph.graph,
        recovery_rate,
        incubation_rate,
        infection_prob,
        initial_infection,
        seeds,
        &mut rng,
    )
}

/// Louvain algorithm for community detection
///
/// Arguments:
///     graph (GraphView): the graph view
///     resolution (float): the resolution paramter for modularity
///     weight_prop (str | None): the edge property to use for weights (has to be float)
///     tol (None | float): the floating point tolerance for deciding if improvements are significant (default: 1e-8)
#[pyfunction]
#[pyo3[signature=(graph, resolution=1.0, weight_prop=None, tol=None)]]
pub fn louvain(
    graph: &PyGraphView,
    resolution: f64,
    weight_prop: Option<&str>,
    tol: Option<f64>,
) -> AlgorithmResult<DynamicGraph, usize> {
    louvain_rs::<ModularityUnDir, _>(&graph.graph, resolution, weight_prop, tol)
}

/// Fruchterman Reingold layout algorithm
///
/// Arguments:
///     graph (GraphView): the graph view
///     iterations (int | None): the number of iterations to run (default: 100)
///     scale (float | None): the scale to apply (default: 1.0)
///     node_start_size (float | None): the start node size to assign random positions (default: 1.0)
///     cooloff_factor (float | None): the cool off factor for the algorithm (default: 0.95)
///     dt (float | None): the time increment between iterations (default: 0.1)
///
/// Returns:
///     a dict with the position for each node as a list with two numbers [x, y]
#[pyfunction]
#[pyo3[signature=(graph, iterations=100, scale=1.0, node_start_size=1.0, cooloff_factor=0.95, dt=0.1)]]
pub fn fruchterman_reingold(
    graph: &PyGraphView,
    iterations: u64,
    scale: f32,
    node_start_size: f32,
    cooloff_factor: f32,
    dt: f32,
) -> HashMap<GID, [f32; 2]> {
    fruchterman_reingold_rs(
        &graph.graph,
        iterations,
        scale,
        node_start_size,
        cooloff_factor,
        dt,
    )
    .into_iter()
    .map(|(id, vector)| (id, [vector.x, vector.y]))
    .collect()
}

/// Cohesive version of `fruchterman_reingold` that adds virtual edges between isolated nodes
#[pyfunction]
#[pyo3[signature=(graph, iterations=100, scale=1.0, node_start_size=1.0, cooloff_factor=0.95, dt=0.1)]]
pub fn cohesive_fruchterman_reingold(
    graph: &PyGraphView,
    iterations: u64,
    scale: f32,
    node_start_size: f32,
    cooloff_factor: f32,
    dt: f32,
) -> HashMap<GID, [f32; 2]> {
    cohesive_fruchterman_reingold_rs(
        &graph.graph,
        iterations,
        scale,
        node_start_size,
        cooloff_factor,
        dt,
    )
    .into_iter()
    .map(|(id, vector)| (id, [vector.x, vector.y]))
    .collect()
}

/// Temporal rich club coefficient
///
/// The traditional rich-club coefficient in a static undirected graph measures the density of connections between the highest
/// degree nodes. It takes a single parameter k, creates a subgraph of the nodes of degree greater than or equal to k, and
/// returns the density of this subgraph.
///
/// In a temporal graph taking the form of a sequence of static snapshots, the temporal rich club coefficient takes a parameter k
/// and a window size delta (both positive integers). It measures the maximal density of the highest connected nodes (of degree
/// greater than or equal to k in the aggregate graph) that persists at least a delta number of consecutive snapshots. For an in-depth
/// definition and usage example, please read to the following paper: Pedreschi, N., Battaglia, D., & Barrat, A. (2022). The temporal
/// rich club phenomenon. Nature Physics, 18(8), 931-938.
///
/// Arguments:
///     graph (GraphView): the aggregate graph
///     views (iterator(GraphView)): sequence of graphs (can be obtained by calling g.rolling(..) on an aggregate graph g)
///     k (int): min degree of nodes to include in rich-club
///     delta (int): the number of consecutive snapshots over which the edges should persist
///
/// Returns:
///     the rich-club coefficient as a float.
#[pyfunction]
#[pyo3[signature = (graph, views, k, delta)]]
pub fn temporal_rich_club_coefficient(
    graph: PyGraphView,
    views: &Bound<PyAny>,
    k: usize,
    delta: usize,
) -> PyResult<f64> {
    let py_iterator = views.try_iter()?;
    let views = py_iterator
        .map(|view| view.and_then(|view| Ok(view.downcast::<PyGraphView>()?.get().graph.clone())))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(temporal_rich_club_rs(graph.graph, views, k, delta))
}

/// Compute a maximum-weighted matching in the general undirected weighted
/// graph given by "edges". If `max_cardinality` is true, only
/// maximum-cardinality matchings are considered as solutions.
///
/// The algorithm is based on "Efficient Algorithms for Finding Maximum
/// Matching in Graphs" by Zvi Galil, ACM Computing Surveys, 1986.
///
/// Based on networkx implementation
/// <https://github.com/networkx/networkx/blob/3351206a3ce5b3a39bb2fc451e93ef545b96c95b/networkx/algorithms/matching.py>
///
/// With reference to the standalone protoype implementation from:
/// <http://jorisvr.nl/article/maximum-matching>
///
/// <http://jorisvr.nl/files/graphmatching/20130407/mwmatching.py>
///
/// The function takes time O(n**3)
///
/// Arguments:
///     graph (GraphView): The graph to compute the maximum weight matching for
///     weight_prop (str, optional): The property on the edge to use for the weight. If not
///         provided,
///     max_cardinality (bool): If set to true compute the maximum-cardinality matching
///         with maximum weight among all maximum-cardinality matchings. Defaults to True.
///     verify_optimum_flag (bool): If true prior to returning an additional routine
///         to verify the optimal solution was found will be run after computing
///         the maximum weight matching. If it's true and the found matching is not
///         an optimal solution this function will panic. This option should
///         normally be only set true during testing. Defaults to False.
///
/// Returns:
///     Matching: The matching
#[pyfunction]
#[pyo3(signature = (graph, weight_prop=None, max_cardinality=true, verify_optimum_flag=false))]
pub fn max_weight_matching(
    graph: &PyGraphView,
    weight_prop: Option<&str>,
    max_cardinality: bool,
    verify_optimum_flag: bool,
) -> Matching<DynamicGraph> {
    mwm(
        &graph.graph,
        weight_prop,
        max_cardinality,
        verify_optimum_flag,
    )
}

/// Computes embedding vectors for each vertex of an undirected/bidirectional graph according to the Fast RP algorithm.
/// Original Paper: https://doi.org/10.48550/arXiv.1908.11512
/// Arguments:
///     g (GraphView): The graph view on which embeddings are generated.
///     embedding_dim (int): The size (dimension) of the generated embeddings.
///     normalization_strength (float): The extent to which high-degree vertices should be discounted (range: 1-0)
///     iter_weights (list[float]): The scalar weights to apply to the results of each iteration
///     seed (int, optional): The seed for initialisation of random vectors
///     threads (int, optional): The number of threads to be used for parallel execution.
///
/// Returns:
///     AlgorithmResult: Vertices mapped to their corresponding embedding vectors
#[pyfunction]
#[pyo3[signature = (g, embedding_dim, normalization_strength, iter_weights, seed=None, threads=None)]]
pub fn fast_rp(
    g: &PyGraphView,
    embedding_dim: usize,
    normalization_strength: f64,
    iter_weights: Vec<f64>,
    seed: Option<u64>,
    threads: Option<usize>,
) -> AlgorithmResult<DynamicGraph, Vec<f64>, Vec<OrderedFloat<f64>>> {
    fast_rp_rs(
        &g.graph,
        embedding_dim,
        normalization_strength,
        iter_weights,
        seed,
        threads,
    )
}
