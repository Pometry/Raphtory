//! Provides functionality for generating graphs for testing and benchmarking.
//! Allows us to generate graphs using the preferential attachment model and
//! the random attachment model.

use crate::graph::PyGraph;
use pyo3::prelude::*;
use raphtory::graphgen::preferential_attachment::ba_preferential_attachment as pa;
use raphtory::graphgen::random_attachment::random_attachment as ra;

/// Generates a graph using the random attachment model
///
/// This function is a graph generation model based upon:
/// Callaway, Duncan S., et al. "Are randomly grown graphs really random?."
/// Physical Review E 64.4 (2001): 041902.
///
/// Arguments:
///   g: The graph you wish to add vertices and edges to
///   vertices_to_add: The amount of vertices you wish to add to the graph (steps)
///   edges_per_step: The amount of edges a joining vertex should add to the graph
///
/// Returns:
///  None
#[pyfunction]
pub(crate) fn random_attachment(g: &PyGraph, vertices_to_add: usize, edges_per_step: usize) {
    ra(&g.graph, vertices_to_add, edges_per_step);
}

/// Generates a graph using the preferential attachment model.
///
/// Given a graph this function will add a user defined number of vertices, each with a user
/// defined number of edges.
/// This is an iterative algorithm where at each `step` a vertex is added and its neighbours are
/// chosen from the pool of nodes already within the network.
/// For this model the neighbours are chosen proportionally based upon their degree, favouring
/// nodes with higher degree (more connections).
/// This sampling is conducted without replacement.
///
/// **Note:**  If the provided graph doesnt have enough nodes/edges for the initial sample,
/// the min number of both will be added before generation begins.
///
/// Arguments:
///    g: The graph you wish to add vertices and edges to
///    vertices_to_add: The amount of vertices you wish to add to the graph (steps)
///    edges_per_step: The amount of edges a joining vertex should add to the graph
///
/// Returns:
///
/// None
#[pyfunction]
pub(crate) fn ba_preferential_attachment(
    g: &PyGraph,
    vertices_to_add: usize,
    edges_per_step: usize,
) {
    pa(&g.graph, vertices_to_add, edges_per_step);
}
