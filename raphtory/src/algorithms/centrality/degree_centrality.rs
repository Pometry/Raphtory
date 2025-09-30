use crate::{
    db::api::{state::NodeState, view::StaticGraphViewOps},
    prelude::*,
};
use rayon::prelude::*;

/// Computes the degree centrality of all nodes in the graph. The values are normalized
/// by dividing each result with the maximum possible degree. Graphs with self-loops can have
/// values of centrality greater than 1.
///
/// # Arguments
///
/// - `g`: A reference to the graph.
///
/// # Returns
///
/// An [AlgorithmResult] containing the degree centrality of each node.
pub fn degree_centrality<G: StaticGraphViewOps>(g: &G) -> NodeState<'static, f64, G> {
    let max_degree = match g.nodes().degree().max() {
        None => return NodeState::new_empty(g.clone()),
        Some(v) => v,
    };

    let values: Vec<_> = g
        .nodes()
        .degree()
        .into_par_iter_values()
        .map(|v| (v as f64) / max_degree as f64)
        .collect();

    NodeState::new_from_values(g.clone(), values)
}
