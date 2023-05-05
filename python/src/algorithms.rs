/// Implementations of various graph algorithms that can be run on a graph.
///
/// To run an algorithm simply import the module and call the function with the graph as the argument
///
use crate::graph_view::PyGraphView;
use itertools::Itertools;
use pyo3::exceptions::PyTypeError;
use std::collections::HashMap;

use crate::utils;
use crate::utils::{extract_input_vertex, InputVertexBox};
use pyo3::prelude::*;
use raphtory::algorithms::degree::{
    average_degree as average_degree_rs, max_in_degree as max_in_degree_rs,
    max_out_degree as max_out_degree_rs, min_in_degree as min_in_degree_rs,
    min_out_degree as min_out_degree_rs,
};
use raphtory::algorithms::directed_graph_density::directed_graph_density as directed_graph_density_rs;
use raphtory::algorithms::generic_taint::generic_taint as generic_taint_rs;
use raphtory::algorithms::local_clustering_coefficient::local_clustering_coefficient as local_clustering_coefficient_rs;
use raphtory::algorithms::local_triangle_count::local_triangle_count as local_triangle_count_rs;
use raphtory::algorithms::pagerank::unweighted_page_rank;
use raphtory::algorithms::connected_components;
use raphtory::algorithms::reciprocity::{
    all_local_reciprocity as all_local_reciprocity_rs, global_reciprocity as global_reciprocity_rs,
};
use rustc_hash::FxHashMap;

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
pub(crate) fn local_triangle_count(g: &PyGraphView, v: &PyAny) -> PyResult<Option<usize>> {
    let v = utils::extract_vertex_ref(v)?;
    Ok(local_triangle_count_rs(&g.graph, v.g_id))
}

#[pyfunction]
pub(crate) fn weakly_connected_components(
    g: &PyGraphView,
    iter_count: usize,
) -> PyResult<FxHashMap<String, u64>> {
    Ok(connected_components::weakly_connected_components(&g.graph, iter_count, None))
}

#[pyfunction]
pub(crate) fn pagerank(
    g: &PyGraphView,
    iter_count: usize,
    max_diff: Option<f32>,
) -> PyResult<FxHashMap<String, f32>> {
    Ok(unweighted_page_rank(&g.graph, iter_count, None, max_diff))
}

#[pyfunction]
pub(crate) fn generic_taint(
    g: &PyGraphView,
    iter_count: usize,
    start_time: i64,
    infected_nodes: Vec<&PyAny>,
    stop_nodes: Vec<&PyAny>,
) -> PyResult<HashMap<String, Vec<(usize, i64, String)>>> {
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
        iter_count,
        start_time,
        infected_nodes?,
        stop_nodes?,
    ))
}

/// Local Clustering coefficient - measures the degree to which nodes in a graph tend to cluster together.
///
/// It is calculated by dividing the number of triangles (sets of three nodes that are all
/// connected to each other) in the graph by the total number of possible triangles.
/// The resulting value is a number between 0 and 1 that represents the density of
/// clustering in the graph.
///
/// A high clustering coefficient indicates that nodes tend to be
/// connected to nodes that are themselves connected to each other, while a low clustering
/// coefficient indicates that nodes tend to be connected to nodes that are not connected
/// to each other.
///
/// In a social network of a particular community, we can compute the clustering
/// coefficient of each node to get an idea of how strongly connected and cohesive
/// that node's neighborhood is.
///
/// A high clustering coefficient for a node in a social network indicates that the
/// node's neighbors tend to be strongly connected with each other, forming a tightly-knit
/// group or community. In contrast, a low clustering coefficient for a node indicates that
/// its neighbors are relatively less connected with each other, suggesting a more fragmented
/// or diverse community.
#[pyfunction]
pub(crate) fn local_clustering_coefficient(g: &PyGraphView, v: &PyAny) -> PyResult<Option<f32>> {
    let v = utils::extract_vertex_ref(v)?;
    Ok(local_clustering_coefficient_rs(&g.graph, v.g_id))
}

/// Graph density - measures how dense or sparse a graph is.
///
/// It is defined as the ratio of the number of edges in the graph to the total number of possible
/// edges. A dense graph has a high edge-to-vertex ratio, while a sparse graph has a low
/// edge-to-vertex ratio.
///
/// For example in social network analysis, a dense graph may indicate a highly interconnected
/// community, while a sparse graph may indicate more isolated individuals.
#[pyfunction]
pub(crate) fn directed_graph_density(g: &PyGraphView) -> f32 {
    directed_graph_density_rs(&g.graph)
}

/// The average degree of all vertices in the graph.
#[pyfunction]
pub(crate) fn average_degree(g: &PyGraphView) -> f64 {
    average_degree_rs(&g.graph)
}

/// The maximum out degree of any vertex in the graph.
#[pyfunction]
pub(crate) fn max_out_degree(g: &PyGraphView) -> usize {
    max_out_degree_rs(&g.graph)
}

/// The maximum in degree of any vertex in the graph.
#[pyfunction]
pub(crate) fn max_in_degree(g: &PyGraphView) -> usize {
    max_in_degree_rs(&g.graph)
}

/// The minimum out degree of any vertex in the graph.
#[pyfunction]
pub(crate) fn min_out_degree(g: &PyGraphView) -> usize {
    min_out_degree_rs(&g.graph)
}

/// The minimum in degree of any vertex in the graph.
#[pyfunction]
pub(crate) fn min_in_degree(g: &PyGraphView) -> usize {
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
pub(crate) fn global_reciprocity(g: &PyGraphView) -> f64 {
    global_reciprocity_rs(&g.graph)
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
pub(crate) fn all_local_reciprocity(g: &PyGraphView) -> HashMap<u64, f64> {
    all_local_reciprocity_rs(&g.graph)
}

/// Computes the number of both open and closed triplets within a graph
///
/// An open triplet, is one where a node has two neighbors, but no edge between them.
/// A closed triplet is one where a node has two neighbors, and an edge between them.
#[pyfunction]
pub(crate) fn triplet_count(g: &PyGraphView) -> usize {
    raphtory::algorithms::triplet_count::triplet_count(&g.graph)
}

/// Computes the global clustering coefficient of a graph. The global clustering coefficient is
/// defined as the number of triangles in the graph divided by the number of triplets in the graph.
#[pyfunction]
pub(crate) fn global_clustering_coefficient(g: &PyGraphView) -> f64 {
    raphtory::algorithms::clustering_coefficient::clustering_coefficient(&g.graph)
}
