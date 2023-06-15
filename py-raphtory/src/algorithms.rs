/// Implementations of various graph algorithms that can be run on a graph.
///
/// To run an algorithm simply import the module and call the function with the graph as the argument
///
use crate::graph_view::PyGraphView;
use std::collections::HashMap;

use crate::utils;
use crate::utils::{extract_input_vertex, InputVertexBox};
use pyo3::prelude::*;
use raphtory::algorithms::connected_components;
use raphtory::algorithms::degree::{
    average_degree as average_degree_rs, max_in_degree as max_in_degree_rs,
    max_out_degree as max_out_degree_rs, min_in_degree as min_in_degree_rs,
    min_out_degree as min_out_degree_rs,
};
use raphtory::algorithms::directed_graph_density::directed_graph_density as directed_graph_density_rs;
use raphtory::algorithms::generic_taint::generic_taint as generic_taint_rs;
use raphtory::algorithms::local_clustering_coefficient::local_clustering_coefficient as local_clustering_coefficient_rs;
use raphtory::algorithms::local_triangle_count::local_triangle_count as local_triangle_count_rs;
use raphtory::algorithms::motifs::three_node_local::local_temporal_three_node_motifs as local_three_node_rs;
use raphtory::algorithms::motifs::three_node_local::global_temporal_three_node_motifs as global_temporal_three_node_motif_rs;
use raphtory::algorithms::pagerank::unweighted_page_rank;
use raphtory::algorithms::reciprocity::{
    all_local_reciprocity as all_local_reciprocity_rs, global_reciprocity as global_reciprocity_rs,
};

/// Local triangle count - calculates the number of triangles (a cycle of length 3) for a node.
/// It measures the local clustering of a graph.
///
/// This is useful for understanding the level of connectivity and the likelihood of information
/// or influence spreading through a network.
///
/// For example, in a social network, the local triangle count of a user's profile can reveal the
/// number of mutual friends they have and the level of interconnectivity between those friends.
/// A high local triangle count for a user indicates that they are part of a tightly-knit group
/// of people, which can be useful for targeted advertising or identifying key influencers
/// within a network.
///
/// Local triangle count can also be used in other domains such as biology, where it can be used
/// to analyze protein interaction networks, or in transportation networks, where it can be used
/// to identify critical junctions or potential traffic bottlenecks.
///
#[pyfunction]
pub fn local_triangle_count(g: &PyGraphView, v: &PyAny) -> PyResult<Option<usize>> {
    let v = utils::extract_vertex_ref(v)?;
    Ok(local_triangle_count_rs(&g.graph, v))
}

#[pyfunction]
pub fn weakly_connected_components(
    g: &PyGraphView,
    iter_count: usize,
) -> PyResult<HashMap<String, u64>> {
    Ok(connected_components::weakly_connected_components(
        &g.graph, iter_count, None,
    ))
}

#[pyfunction]
pub fn pagerank(
    g: &PyGraphView,
    iter_count: usize,
    max_diff: Option<f64>,
) -> PyResult<HashMap<String, f64>> {
    Ok(unweighted_page_rank(&g.graph, iter_count, None, max_diff, true))
}

#[pyfunction]
pub fn generic_taint(
    g: &PyGraphView,
    iter_count: usize,
    start_time: i64,
    infected_nodes: Vec<&PyAny>,
    stop_nodes: Vec<&PyAny>,
) -> Result<HashMap<String, Vec<(i64, String)>>, PyErr> {
    let infected_nodes: PyResult<Vec<InputVertexBox>> = infected_nodes
        .into_iter()
        .map(|v| extract_input_vertex(v))
        .collect();
    let stop_nodes: PyResult<Vec<InputVertexBox>> = stop_nodes
        .into_iter()
        .map(|v| extract_input_vertex(v))
        .collect();

    Ok(generic_taint_rs(
        &g.graph,
        None,
        iter_count,
        start_time,
        infected_nodes?,
        stop_nodes?,
    ))
}

/// Local Clustering coefficient - measures the degree to which nodes in a graph tend to cluster together.
///
/// The proportion of pairs of neighbours of a node who are themselves connected.
/// 
/// Arguments:
///     g (Raphtory graph) : Raphtory graph, can be directed or undirected but will be treated as undirected.
///     v (int or str): vertex id
/// 
/// Returns:
///     cluster (float) : the local clustering coefficient of vertex v in g.
#[pyfunction]
pub fn local_clustering_coefficient(g: &PyGraphView, v: &PyAny) -> PyResult<Option<f32>> {
    let v = utils::extract_vertex_ref(v)?;
    Ok(local_clustering_coefficient_rs(&g.graph, v))
}

/// Graph density - measures how dense or sparse a graph is.
///
/// The ratio of the number of edges in the graph to the total number of possible
/// edges. A dense graph has a high edge-to-vertex ratio, while a sparse graph has a low
/// edge-to-vertex ratio.
///
/// 
#[pyfunction]
pub fn directed_graph_density(g: &PyGraphView) -> f32 {
    directed_graph_density_rs(&g.graph)
}

/// The average degree of all vertices in the graph.
#[pyfunction]
pub fn average_degree(g: &PyGraphView) -> f64 {
    average_degree_rs(&g.graph)
}

/// The maximum out degree of any vertex in the graph.
#[pyfunction]
pub fn max_out_degree(g: &PyGraphView) -> usize {
    max_out_degree_rs(&g.graph)
}

/// The maximum in degree of any vertex in the graph.
#[pyfunction]
pub fn max_in_degree(g: &PyGraphView) -> usize {
    max_in_degree_rs(&g.graph)
}

/// The minimum out degree of any vertex in the graph.
#[pyfunction]
pub fn min_out_degree(g: &PyGraphView) -> usize {
    min_out_degree_rs(&g.graph)
}

/// The minimum in degree of any vertex in the graph.
#[pyfunction]
pub fn min_in_degree(g: &PyGraphView) -> usize {
    min_in_degree_rs(&g.graph)
}

/// Reciprocity - measure of the symmetry of relationships in a graph, the global reciprocity of
/// the entire graph.
/// This calculates the number of reciprocal connections (edges that go in both directions) in a
/// graph and normalizes it by the total number of edges.
///
/// In a social network context, reciprocity measures the likelihood that if person A is linked
/// to person B, then person B is linked to person A. This algorithm can be used to determine the
/// level of symmetry or balance in a social network. It can also reveal the power dynamics in a
/// group or community. For example, if one person has many connections that are not reciprocated,
/// it could indicate that this person has more power or influence in the network than others.
///
/// In a business context, reciprocity can be used to study customer behavior. For instance, in a
/// transactional network, if a customer tends to make a purchase from a seller and then the seller
/// makes a purchase from the same customer, it can indicate a strong reciprocal relationship
/// between them. On the other hand, if the seller does not make a purchase from the same customer,
/// it could imply a less reciprocal or more one-sided relationship.
#[pyfunction]
pub fn global_reciprocity(g: &PyGraphView) -> f64 {
    global_reciprocity_rs(&g.graph, None)
}

/// Reciprocity - measure of the symmetry of relationships in a graph.
/// the reciprocity of every vertex in the graph as a tuple of vector id and the reciprocity
/// This calculates the number of reciprocal connections (edges that go in both directions) in a
/// graph and normalizes it by the total number of edges.
///
/// In a social network context, reciprocity measures the likelihood that if person A is linked
/// to person B, then person B is linked to person A. This algorithm can be used to determine the
/// level of symmetry or balance in a social network. It can also reveal the power dynamics in a
/// group or community. For example, if one person has many connections that are not reciprocated,
/// it could indicate that this person has more power or influence in the network than others.
///
/// In a business context, reciprocity can be used to study customer behavior. For instance, in a
/// transactional network, if a customer tends to make a purchase from a seller and then the seller
/// makes a purchase from the same customer, it can indicate a strong reciprocal relationship
/// between them. On the other hand, if the seller does not make a purchase from the same customer,
/// it could imply a less reciprocal or more one-sided relationship.
///
#[pyfunction]
pub fn all_local_reciprocity(g: &PyGraphView) -> HashMap<String, f64> {
    all_local_reciprocity_rs(&g.graph, None)
}

/// Computes the number of both open and closed triplets within a graph
///
/// An open triplet, is one where a node has two neighbors, but no edge between them.
/// A closed triplet is one where a node has two neighbors, and an edge between them.
#[pyfunction]
pub fn triplet_count(g: &PyGraphView) -> usize {
    raphtory::algorithms::triplet_count::triplet_count(&g.graph, None)
}

/// Computes the global clustering coefficient of a graph. The global clustering coefficient is
/// defined as the number of triangles in the graph divided by the number of triplets in the graph.
#[pyfunction]
pub fn global_clustering_coefficient(g: &PyGraphView) -> f64 {
    raphtory::algorithms::clustering_coefficient::clustering_coefficient(&g.graph)
}

#[pyfunction]
pub fn global_temporal_three_node_motif(g: &PyGraphView, delta: i64) -> Vec<usize> {
    global_temporal_three_node_motif_rs(&g.graph, delta)
}

#[pyfunction]
pub fn local_temporal_three_node_motifs(
    g: &PyGraphView, delta: i64
) -> HashMap<u64, Vec<usize>> {
    local_three_node_rs(&g.graph, delta)
}