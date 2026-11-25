use std::hash::RandomState;

use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::api::{
        state::{GenericNodeState, TypedNodeState},
        view::*,
    },
    errors::GraphError,
};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct LCCState {
    pub lcc: f64,
}

/// Errors with `NodeMissingError` if a node of `v` is not in `graph`.
fn calculate_lcc<G: StaticGraphViewOps, V: AsNodeRef>(
    graph: &G,
    v: Vec<V>,
) -> Result<TypedNodeState<'static, LCCState, G>, GraphError> {
    let state: std::collections::HashMap<VID, LCCState, RandomState> = v
        .par_iter()
        .map(|n| {
            let node = n.as_node_ref();
            let s = (&graph)
                .node(node)
                .ok_or_else(|| GraphError::node_missing(graph, node))?;
            let triangle_count = s
                .neighbours()
                .iter()
                .filter(|nbor| nbor.degree() > 1 && nbor.node != s.node)
                .combinations(2)
                .filter(|nb| graph.has_edge(nb[0], nb[1]) || graph.has_edge(nb[1], nb[0]))
                .count() as f64;
            let mut degree = s.degree() as f64;
            if graph.has_edge(s.node, s.node) {
                degree -= 1.0;
            }
            Ok((
                s.node,
                if degree <= 1.0 {
                    LCCState { lcc: 0.0 }
                } else {
                    LCCState {
                        lcc: (2.0 * triangle_count) / (degree * (degree - 1.0)),
                    }
                },
            ))
        })
        .collect::<Result<_, GraphError>>()?;

    Ok(TypedNodeState::new(GenericNodeState::new_from_map(
        graph.clone(),
        state,
        |value| value,
        None,
    )))
}
/// Local clustering coefficient (batch, intersection) - measures the degree to which one or multiple nodes in a graph tend to cluster together.
/// Uses path-counting for its triangle-counting step.
///
/// # Arguments
/// - `graph`: Raphtory graph, can be directed or undirected but will be treated as undirected.
/// - `v`: vec of node ids, if empty, will return results for every node in the graph (the view's nodes, so a filter is respected)
///
/// # Returns
/// the local clustering coefficient of node v in g, or `NodeMissingError` if a node of `v` is
/// not in `graph`.
pub fn local_clustering_coefficient_batch<G: StaticGraphViewOps, V: AsNodeRef>(
    graph: &G,
    v: Vec<V>,
) -> Result<TypedNodeState<'static, LCCState, G>, GraphError> {
    if v.is_empty() {
        // VIDs are handed out across storage segments and are not the dense range
        // 0..count_nodes(), so enumerate the nodes the view actually has
        let all: Vec<VID> = graph.nodes().iter().map(|n| n.node).collect();
        calculate_lcc(graph, all)
    } else {
        calculate_lcc(graph, v)
    }
}
