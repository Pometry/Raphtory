//! # Weight Accumulation
//!
//! This algorithm provides functionality to accumulate (or sum) weights on nodes
//! in a graph.
use crate::{
    db::{
        api::{
            state::{GenericNodeState, TypedNodeState},
            view::StaticGraphViewOps,
        },
        graph::node::NodeView,
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::Direction;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct BalanceState {
    pub balance: f64,
}

/// Computes the net sum of weights for a given node based on edge direction.
///
/// For every edge connected to the node, this function checks the source of the edge
/// against the node itself to determine the directionality. The weight can be treated
/// as negative or positive based on the edge's source and the specified direction:
///
/// - If the edge's source is the node itself and the direction is either `OUT` or `BOTH`,
///   the weight is treated as negative.
/// - If the edge's source is not the node and the direction is either `IN` or `BOTH`,
///   the weight is treated as positive.
/// - In all other cases, the weight contribution is zero.
///
/// # Arguments
/// - `v`: The node for which we want to compute the weight sum.
/// - `name`: The name of the property which holds the edge weight.
/// - `direction`: Specifies the direction of edges to consider (`IN`, `OUT`, or `BOTH`).
///
/// # Returns
/// An `f64` which is the net sum of weights for the node considering the specified direction.
fn balance_per_node<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
    v: &NodeView<'graph, G, GH>,
    prop_id: usize,
    direction: Direction,
) -> f64 {
    // let in_result = v.in_edges().properties().get(name.clone()).sum();
    // in_result - out_result
    match direction {
        Direction::IN => v
            .in_edges()
            .properties()
            .flat_map(|prop| {
                prop.temporal().get_by_id(prop_id).map(|val| {
                    val.values()
                        .map(|valval| valval.as_f64().unwrap_or(1.0f64))
                        .sum::<f64>()
                })
            })
            .sum::<f64>(),
        Direction::OUT => -v
            .out_edges()
            .properties()
            .flat_map(|prop| {
                prop.temporal().get_by_id(prop_id).map(|val| {
                    val.values()
                        .map(|valval| valval.as_f64().unwrap_or(1.0f64))
                        .sum::<f64>()
                })
            })
            .sum::<f64>(),
        Direction::BOTH => {
            let in_res = balance_per_node(v, prop_id, Direction::IN);
            let out_res = balance_per_node(v, prop_id, Direction::OUT);
            in_res + out_res
        }
    }
}

/// Computes the sum of weights for all nodes in the graph.
///
/// This function iterates over all nodes and calculates the net sum of weights.
/// Incoming edges have a positive sum and outgoing edges have a negative sum
/// It uses a compute context and tasks to achieve this.
///
/// # Arguments
/// - `graph`: The graph on which the operation is to be performed.
/// - `name`: The name of the property which holds the edge weight.
///
/// # Returns
/// An [AlgorithmResult] which maps each node to its corresponding net weight sum.
pub fn balance<G: StaticGraphViewOps>(
    graph: &G,
    name: String,
    direction: Direction,
) -> Result<TypedNodeState<'static, BalanceState, G>, GraphError> {
    if let Some((weight_id, weight_type)) = graph.edge_meta().get_prop_id_and_type(&name, false) {
        if !weight_type.is_numeric() {
            return Err(GraphError::InvalidProperty {
                reason: "Edge property {name} is not numeric".to_string(),
            });
        }
        let values: Vec<_> = graph
            .nodes()
            .par_iter()
            .map(|n| balance_per_node(&n, weight_id, direction))
            .collect();

        Ok(TypedNodeState::new(GenericNodeState::new_from_eval(
            graph.clone(),
            values,
        )))
    } else {
        Err(GraphError::InvalidProperty {
            reason: "Edge property {name} does not exist".to_string(),
        })
    }
}
