/// Implementations of various graph algorithms that can be run on a graph.
///
/// To run an algorithm simply import the module and call the function with the graph as the argument
///
use crate::python::graph_view::PyGraphView;
use std::collections::HashMap;

use crate::algorithms::connected_components;
use crate::algorithms::degree::{
    average_degree as average_degree_rs, max_in_degree as max_in_degree_rs,
    max_out_degree as max_out_degree_rs, min_in_degree as min_in_degree_rs,
    min_out_degree as min_out_degree_rs,
};
use crate::algorithms::directed_graph_density::directed_graph_density as directed_graph_density_rs;
use crate::algorithms::local_clustering_coefficient::local_clustering_coefficient as local_clustering_coefficient_rs;
use crate::algorithms::local_triangle_count::local_triangle_count as local_triangle_count_rs;
use crate::algorithms::motifs::three_node_local::global_temporal_three_node_motifs as global_temporal_three_node_motif_rs;
use crate::algorithms::motifs::three_node_local::local_temporal_three_node_motifs as local_three_node_rs;
use crate::algorithms::pagerank::unweighted_page_rank;
use crate::algorithms::reciprocity::{
    all_local_reciprocity as all_local_reciprocity_rs, global_reciprocity as global_reciprocity_rs,
};
use crate::algorithms::temporal_reachability::temporally_reachable_nodes as temporal_reachability_rs;
use crate::core::vertex_ref::VertexRef;
use crate::python::utils::PyInputVertex;
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
///     dict : Dictionary with string keys and integer values mapping vertex names to their component ids.
#[pyfunction]
pub fn weakly_connected_components(
    g: &PyGraphView,
    iter_count: usize,
) -> PyResult<HashMap<String, u64>> {
    Ok(connected_components::weakly_connected_components(
        &g.graph, iter_count, None,
    ))
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
///     dict : Dictionary with string keys and float values mapping vertex names to their pagerank value.
#[pyfunction]
pub fn pagerank(
    g: &PyGraphView,
    iter_count: usize,
    max_diff: Option<f64>,
) -> PyResult<HashMap<String, f64>> {
    Ok(unweighted_page_rank(
        &g.graph, iter_count, None, max_diff, true,
    ))
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
///     dict : Dictionary with string keys and float values mapping vertex names to their pagerank value.
#[pyfunction]
pub fn temporally_reachable_nodes(
    g: &PyGraphView,
    max_hops: usize,
    start_time: i64,
    seed_nodes: Vec<PyInputVertex>,
    stop_nodes: Option<Vec<PyInputVertex>>,
) -> Result<HashMap<String, Vec<(i64, String)>>, PyErr> {
    Ok(temporal_reachability_rs(
        &g.graph, None, max_hops, start_time, seed_nodes, stop_nodes,
    ))
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
///     dict : a dictionary with string keys and float values mapping each vertex name to its reciprocity value.
///
#[pyfunction]
pub fn all_local_reciprocity(g: &PyGraphView) -> HashMap<String, f64> {
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

///
#[pyfunction]
pub fn global_temporal_three_node_motif(g: &PyGraphView, delta: i64) -> Vec<usize> {
    global_temporal_three_node_motif_rs(&g.graph, delta)
}

#[pyfunction]
pub fn local_temporal_three_node_motifs(g: &PyGraphView, delta: i64) -> HashMap<u64, Vec<usize>> {
    local_three_node_rs(&g.graph, delta)
}
