use crate::{
    db::api::{
        state::{GenericNodeState, TypedNodeState},
        view::StaticGraphViewOps,
    },
    prelude::*,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct CentralityScore {
    #[serde(rename = "degree_centrality")]
    score: f64,
}

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
pub fn degree_centrality<G: StaticGraphViewOps>(
    g: &G,
) -> TypedNodeState<'static, CentralityScore, G> {
    // NodeState<'static, f64, G> {
    let max_degree = match g.nodes().degree().max() {
        None => return TypedNodeState::new(GenericNodeState::new_empty(g.clone())),
        Some(v) => v,
    };

    let values: Vec<CentralityScore> = g
        .nodes()
        .degree()
        .into_par_iter_values()
        .map(|v| CentralityScore {
            score: (v as f64) / max_degree as f64,
        })
        .collect();

    TypedNodeState::new(GenericNodeState::new_from_eval(g.clone(), values))

}
