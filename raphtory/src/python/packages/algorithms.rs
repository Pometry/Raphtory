use std::collections::HashMap;

use crate::python::graph::edge::PyDirection;
/// Implementations of various graph algorithms that can be run on a graph.
///
/// To run an algorithm simply import the module and call the function with the graph as the argument
///
use crate::{
    algorithms::{
        algorithm_result::AlgorithmResult,
        balance::balance as balance_rs,
        connected_components,
        degree::{
            average_degree as average_degree_rs, max_in_degree as max_in_degree_rs,
            max_out_degree as max_out_degree_rs, min_in_degree as min_in_degree_rs,
            min_out_degree as min_out_degree_rs,
        },
        directed_graph_density::directed_graph_density as directed_graph_density_rs,
        hits::hits as hits_rs,
        local_clustering_coefficient::local_clustering_coefficient as local_clustering_coefficient_rs,
        local_triangle_count::local_triangle_count as local_triangle_count_rs,
        motifs::three_node_temporal_motifs::{
            global_temporal_three_node_motif as global_temporal_three_node_motif_rs,
            global_temporal_three_node_motif_general as global_temporal_three_node_motif_general_rs,
            temporal_three_node_motif as local_three_node_rs,
        },
        pagerank::unweighted_page_rank,
        reciprocity::{
            all_local_reciprocity as all_local_reciprocity_rs,
            global_reciprocity as global_reciprocity_rs,
        },
        temporal_reachability::temporally_reachable_nodes as temporal_reachability_rs,
    },
    core::entities::vertices::vertex_ref::VertexRef,
    python::{graph::views::graph_view::PyGraphView, utils::PyInputVertex},
};
use ordered_float::OrderedFloat;
use pyo3::prelude::*;

/// Local triangle count - calculates the number of triangles (a cycle of length 3) a vertex participates in.
///
/// This function returns the number of pairs of neighbours of a given node which are themselves connected.
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph, this can be directed or undirected but will be treated as undirected
///     v (int or str) : vertex id or name
///
/// Returns:
///     triangles(int) : number of triangles associated with vertex v
///
#[pyfunction]
pub fn local_triangle_count(g: &PyGraphView, v: VertexRef) -> Option<usize> {
    local_triangle_count_rs(&g.graph, v)
}

/// Weakly connected components -- partitions the graph into node sets which are mutually reachable by an undirected path
///
/// This function assigns a component id to each vertex such that vertices with the same component id are mutually reachable
/// by an undirected path.
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph
///     iter_count (int) : Maximum number of iterations to run. Note that this will terminate early if the labels converge prior to the number of iterations being reached.
///
/// Returns:
///     AlgorithmResult : AlgorithmResult object with string keys and integer values mapping vertex names to their component ids.
#[pyfunction]
#[pyo3(signature = (g, iter_count=9223372036854775807))]
pub fn weakly_connected_components(
    g: &PyGraphView,
    iter_count: usize,
) -> AlgorithmResult<String, u64> {
    connected_components::weakly_connected_components(&g.graph, iter_count, None)
}

/// Pagerank -- pagerank centrality value of the vertices in a graph
///
/// This function calculates the Pagerank value of each vertex in a graph. See https://en.wikipedia.org/wiki/PageRank for more information on PageRank centrality.
/// A default damping factor of 0.85 is used. This is an iterative algorithm which terminates if the sum of the absolute difference in pagerank values between iterations
/// is less than the max diff value given.
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph
///     iter_count (int) : Maximum number of iterations to run. Note that this will terminate early if convergence is reached.
///     max_diff (float) : Optional parameter providing an alternative stopping condition. The algorithm will terminate if the sum of the absolute difference in pagerank values between iterations
/// is less than the max diff value given.
///
/// Returns:
///     AlgorithmResult : AlgorithmResult with string keys and float values mapping vertex names to their pagerank value.
#[pyfunction]
#[pyo3(signature = (g, iter_count=20, max_diff=None))]
pub fn pagerank(
    g: &PyGraphView,
    iter_count: usize,
    max_diff: Option<f64>,
) -> AlgorithmResult<String, f64, OrderedFloat<f64>> {
    unweighted_page_rank(&g.graph, iter_count, None, max_diff, true)
}

/// Temporally reachable nodes -- the nodes that are reachable by a time respecting path followed out from a set of seed nodes at a starting time.
///
/// This function starts at a set of seed nodes and follows all time respecting paths until either a) a maximum number of hops is reached, b) one of a set of
/// stop nodes is reached, or c) no further time respecting edges exist. A time respecting path is a sequence of nodes v_1, v_2, ... , v_k such that there exists
/// a sequence of edges (v_i, v_i+1, t_i) with t_i < t_i+1 for i = 1, ... , k - 1.
///
/// Arguments:
///     g (Raphtory graph) : directed Raphtory graph
///     max_hops (int) : maximum number of hops to propagate out
///     start_time (int) : time at which to start the path (such that t_1 > start_time for any path starting from these seed nodes)
///     seed_nodes (list(str) or list(int)) : list of vertex names or ids which should be the starting nodes
///     stop_nodes (list(str) or list(int)) : nodes at which a path shouldn't go any further
///
/// Returns:
///     AlgorithmResult : AlgorithmResult with string keys and float values mapping vertex names to their pagerank value.
#[pyfunction]
pub fn temporally_reachable_nodes(
    g: &PyGraphView,
    max_hops: usize,
    start_time: i64,
    seed_nodes: Vec<PyInputVertex>,
    stop_nodes: Option<Vec<PyInputVertex>>,
) -> AlgorithmResult<String, Vec<(i64, String)>> {
    temporal_reachability_rs(&g.graph, None, max_hops, start_time, seed_nodes, stop_nodes)
}

/// Local clustering coefficient - measures the degree to which nodes in a graph tend to cluster together.
///
/// The proportion of pairs of neighbours of a node who are themselves connected.
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph, can be directed or undirected but will be treated as undirected.
///     v (int or str): vertex id or name
///
/// Returns:
///     float : the local clustering coefficient of vertex v in g.
#[pyfunction]
pub fn local_clustering_coefficient(g: &PyGraphView, v: VertexRef) -> Option<f32> {
    local_clustering_coefficient_rs(&g.graph, v)
}

/// Graph density - measures how dense or sparse a graph is.
///
/// The ratio of the number of directed edges in the graph to the total number of possible directed
/// edges (given by N * (N-1) where N is the number of nodes).
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     float : Directed graph density of G.
#[pyfunction]
pub fn directed_graph_density(g: &PyGraphView) -> f32 {
    directed_graph_density_rs(&g.graph)
}

/// The average (undirected) degree of all vertices in the graph.
///
/// Note that this treats the graph as simple and undirected and is equal to twice
/// the number of undirected edges divided by the number of nodes.
///
/// Arguments:
///     g (Raphtory graph) : a Raphtory graph
///
/// Returns:
///     float : the average degree of the nodes in the graph
#[pyfunction]
pub fn average_degree(g: &PyGraphView) -> f64 {
    average_degree_rs(&g.graph)
}

/// The maximum out degree of any vertex in the graph.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     int : value of the largest outdegree
#[pyfunction]
pub fn max_out_degree(g: &PyGraphView) -> usize {
    max_out_degree_rs(&g.graph)
}

/// The maximum in degree of any vertex in the graph.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     int : value of the largest indegree
#[pyfunction]
pub fn max_in_degree(g: &PyGraphView) -> usize {
    max_in_degree_rs(&g.graph)
}

/// The minimum out degree of any vertex in the graph.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     int : value of the smallest outdegree
#[pyfunction]
pub fn min_out_degree(g: &PyGraphView) -> usize {
    min_out_degree_rs(&g.graph)
}

/// The minimum in degree of any vertex in the graph.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
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
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     float : reciprocity of the graph between 0 and 1.

#[pyfunction]
pub fn global_reciprocity(g: &PyGraphView) -> f64 {
    global_reciprocity_rs(&g.graph, None)
}

/// Local reciprocity - measure of the symmetry of relationships associated with a vertex
///
/// This measures the proportion of a vertex's outgoing edges which are reciprocated with an incoming edge.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     AlgorithmResult : AlgorithmResult with string keys and float values mapping each vertex name to its reciprocity value.
///
#[pyfunction]
pub fn all_local_reciprocity(g: &PyGraphView) -> AlgorithmResult<String, f64, OrderedFloat<f64>> {
    all_local_reciprocity_rs(&g.graph, None)
}

/// Computes the number of connected triplets within a graph
///
/// A connected triplet (also known as a wedge, 2-hop path) is a pair of edges with one node in common. For example, the triangle made up of edges
/// A-B, B-C, C-A is formed of three connected triplets.
///
/// Arguments:
///     g (Raphtory graph) : a Raphtory graph, treated as undirected
///
/// Returns:
///     int : the number of triplets in the graph
#[pyfunction]
pub fn triplet_count(g: &PyGraphView) -> usize {
    crate::algorithms::triplet_count::triplet_count(&g.graph, None)
}

/// Computes the global clustering coefficient of a graph. The global clustering coefficient is
/// defined as the number of triangles in the graph divided by the number of triplets in the graph.
///
/// Note that this is also known as transitivity and is different to the average clustering coefficient.
///
/// Arguments:
///     g (Raphtory graph) : a Raphtory graph, treated as undirected
///
/// Returns:
///     float : the global clustering coefficient of the graph
///
/// See also:
///     [`Triplet Count`](triplet_count)
#[pyfunction]
pub fn global_clustering_coefficient(g: &PyGraphView) -> f64 {
    crate::algorithms::clustering_coefficient::clustering_coefficient(&g.graph)
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
///  Also included are two node motifs, of which there are 8 when counted from the perspective of each vertex. These are characterised by the direction of each edge, enumerated
///  in the above order. Note that for the global graph counts, each motif is counted in both directions (a single III motif for one vertex is an OOO motif for the other vertex).
///
///  Triangles:
///
///  There are 8 triangle motifs:
///
///   1. i --> j, k --> j, i --> k
///   2. i --> j, k --> i, j --> k
///   3. i --> j, j --> k, i --> k
///   4. i --> j, i --> k, j --> k
///   5. i --> j, k --> j, k --> i
///   6. i --> j, k --> i, k --> j
///   7. i --> j, j --> k, k --> i
///   8. i --> j, i --> k, k --> j
///
/// Arguments:
///     g (raphtory graph) : A directed raphtory graph
///     delta (int) - Maximum time difference between the first and last edge of the
/// motif. NB if time for edges was given as a UNIX epoch, this should be given in seconds, otherwise
/// milliseconds should be used (if edge times were given as string)
///
/// Returns:
///     list : A 40 dimensional array with the counts of each motif, given in the same order as described above. Note that the two-node motif counts are symmetrical so it may be more useful just to consider the first four elements.
///
/// Notes:
///     This is achieved by calling the local motif counting algorithm, summing the resulting arrays and dealing with overcounted motifs: the triangles (by dividing each motif count by three) and two-node motifs (dividing by two).
///
#[pyfunction]
pub fn global_temporal_three_node_motif(g: &PyGraphView, delta: i64) -> Vec<usize> {
    global_temporal_three_node_motif_rs(&g.graph, delta, None)
}

#[pyfunction]
pub fn global_temporal_three_node_motif_multi(
    g: &PyGraphView,
    deltas: Vec<i64>,
) -> Vec<Vec<usize>> {
    global_temporal_three_node_motif_general_rs(&g.graph, deltas, None)
}

/// Computes the number of each type of motif that each node participates in. See global_temporal_three_node_motifs for a summary of the motifs involved.
///
/// Arguments:
///     g (raphtory graph) : A directed raphtory graph
///     delta (int) - Maximum time difference between the first and last edge of the
/// motif. NB if time for edges was given as a UNIX epoch, this should be given in seconds, otherwise
/// milliseconds should be used (if edge times were given as string)
///
/// Returns:
///     AlgorithmResult : An AlgorithmResult with node ids as keys and a 40d array of motif counts (in the same order as the global motif counts) with the number of each
/// motif that node participates in.
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
/// AuthScore of a vertex (A) = Sum of HubScore of all vertices pointing at vertex (A) from previous iteration /
///     Sum of HubScore of all vertices in the current iteration
///
/// HubScore of a vertex (A) = Sum of AuthScore of all vertices pointing away from vertex (A) from previous iteration /
///     Sum of AuthScore of all vertices in the current iteration
///
/// Returns
///
/// * An AlgorithmResult object containing the mapping from vertex ID to the hub and authority score of the vertex
#[pyfunction]
#[pyo3(signature = (g, iter_count=20, threads=None))]
pub fn hits(
    g: &PyGraphView,
    iter_count: usize,
    threads: Option<usize>,
) -> AlgorithmResult<String, (f32, f32), (OrderedFloat<f32>, OrderedFloat<f32>)> {
    hits_rs(&g.graph, iter_count, threads)
}

/// Sums the weights of edges in the graph based on the specified direction.
///
/// This function computes the sum of edge weights based on the direction provided, and can be executed in parallel using a given number of threads.
///
/// Arguments:
/// * `g` (`&PyGraphView`): The graph view on which the operation is to be performed.
/// * `name` (`String`, default = "weight"): The name of the edge property used as the weight. Defaults to "weight" if not provided.
/// * `direction` (`PyDirection`, default = `PyDirection::new("BOTH")`): Specifies the direction of the edges to be considered for summation.
///    - `PyDirection::new("OUT")`: Only consider outgoing edges.
///    - `PyDirection::new("IN")`: Only consider incoming edges.
///    - `PyDirection::new("BOTH")`: Consider both outgoing and incoming edges. This is the default.
/// * `threads` (`Option<usize>`, default = `None`): The number of threads to be used for parallel execution. Defaults to single-threaded operation if not provided.
///
/// Returns:
/// `AlgorithmResult<String, OrderedFloat<f64>>`: A result containing a mapping of vertex names to the computed sum of their associated edge weights.
///
#[pyfunction]
#[pyo3[signature = (g, name="weight".to_string(), direction=PyDirection::new("BOTH"),  threads=None)]]
pub fn balance(
    g: &PyGraphView,
    name: String,
    direction: PyDirection,
    threads: Option<usize>,
) -> AlgorithmResult<String, f64, OrderedFloat<f64>> {
    balance_rs(&g.graph, name.clone(), direction.into(), threads)
}
