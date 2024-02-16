//! Provides functionality for generating graphs for testing and benchmarking.
//! Allows us to generate graphs using the preferential attachment model and
//! the random attachment model.
use crate::{
    graphgen::{
        preferential_attachment::ba_preferential_attachment as pa,
        random_attachment::random_attachment as ra,
    },
    python::graph::graph::PyGraph,
};
use pyo3::prelude::*;

/// Generates a graph using the random attachment model
///
/// This function is a graph generation model based upon:
/// Callaway, Duncan S., et al. "Are randomly grown graphs really random?."
/// Physical Review E 64.4 (2001): 041902.
///
/// Arguments:
///   g: The graph you wish to add nodes and edges to
///   nodes_to_add: The amount of nodes you wish to add to the graph (steps)
///   edges_per_step: The amount of edges a joining node should add to the graph
///   seed: The seed used in rng, an array of length 32 containing ints (ints must have a max size of u8)
///
/// Returns:
///  None
#[pyfunction]
#[pyo3[signature = (g, nodes_to_add, edges_per_step, seed=None)]]
pub fn random_attachment(
    g: &PyGraph,
    nodes_to_add: usize,
    edges_per_step: usize,
    seed: Option<[u8; 32]>,
) {
    ra(&g.graph, nodes_to_add, edges_per_step, seed);
}

/// Generates a graph using the preferential attachment model.
///
/// Given a graph this function will add a user defined number of nodes, each with a user
/// defined number of edges.
/// This is an iterative algorithm where at each `step` a node is added and its neighbours are
/// chosen from the pool of nodes already within the network.
/// For this model the neighbours are chosen proportionally based upon their degree, favouring
/// nodes with higher degree (more connections).
/// This sampling is conducted without replacement.
///
/// **Note:**  If the provided graph doesnt have enough nodes/edges for the initial sample,
/// the min number of both will be added before generation begins.
///
/// Arguments:
///    g: The graph you wish to add nodes and edges to
///    nodes_to_add: The amount of nodes you wish to add to the graph (steps)
///    edges_per_step: The amount of edges a joining node should add to the graph
///    seed: The seed used in rng, an array of length 32 containing ints (ints must have a max size of u8)
///
/// Returns:
///
/// None
#[pyfunction]
#[pyo3[signature = (g, nodes_to_add, edges_per_step, seed=None)]]
pub fn ba_preferential_attachment(
    g: &PyGraph,
    nodes_to_add: usize,
    edges_per_step: usize,
    seed: Option<[u8; 32]>,
) {
    pa(&g.graph, nodes_to_add, edges_per_step, seed);
}
