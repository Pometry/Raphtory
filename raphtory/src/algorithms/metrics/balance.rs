//! # Weight Accumulation
//!
//! This algorithm provides functionality to accumulate (or sum) weights on nodes
//! in a graph.
use crate::{
    core::{utils::errors::GraphError, Direction},
    db::{
        api::{state::NodeState, view::StaticGraphViewOps},
        graph::node::NodeView,
    },
    prelude::*,
};
use rayon::prelude::*;

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
    name: &str,
    direction: Direction,
) -> f64 {
    // let in_result = v.in_edges().properties().get(name.clone()).sum();
    // in_result - out_result
    match direction {
        Direction::IN => v
            .in_edges()
            .properties()
            .flat_map(|prop| {
                prop.temporal().get(name).map(|val| {
                    val.values()
                        .into_iter()
                        .map(|valval| valval.as_f64().unwrap_or(1.0f64))
                        .sum::<f64>()
                })
            })
            .sum::<f64>(),
        Direction::OUT => -v
            .out_edges()
            .properties()
            .flat_map(|prop| {
                prop.temporal().get(name).map(|val| {
                    val.values()
                        .into_iter()
                        .map(|valval| valval.as_f64().unwrap_or(1.0f64))
                        .sum::<f64>()
                })
            })
            .sum::<f64>(),
        Direction::BOTH => {
            let in_res = balance_per_node(v, name, Direction::IN);
            let out_res = balance_per_node(v, name, Direction::OUT);
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
) -> Result<NodeState<'static, f64, G>, GraphError> {
    let weight_type = match graph.edge_meta().temporal_prop_meta().get_id(&name) {
        Some(weight_id) => graph.edge_meta().temporal_prop_meta().get_dtype(weight_id),
        None => graph
            .edge_meta()
            .const_prop_meta()
            .get_id(&name)
            .map(|weight_id| {
                graph
                    .edge_meta()
                    .const_prop_meta()
                    .get_dtype(weight_id)
                    .unwrap()
            }),
    };
    match weight_type {
        None => {
            return Err(GraphError::InvalidProperty {
                reason: "Edge property {name} does not exist".to_string(),
            })
        }
        Some(weight_type) => {
            if !weight_type.is_numeric() {
                return Err(GraphError::InvalidProperty {
                    reason: "Edge property {name} is not numeric".to_string(),
                });
            }
        }
    }

    let values: Vec<_> = graph
        .nodes()
        .par_iter()
        .map(|n| balance_per_node(&n, &name, direction))
        .collect();

    Ok(NodeState::new_from_values(graph.clone(), values))
}

#[cfg(test)]
mod sum_weight_test {
    use crate::{
        algorithms::metrics::balance::balance,
        core::{Direction, Prop},
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::GraphViewOps,
        test_storage,
    };
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    #[test]
    fn test_sum_float_weights() {
        let graph = Graph::new();

        let vs = vec![
            ("1", "2", 10, 1),
            ("1", "4", 20, 2),
            ("2", "3", 5, 3),
            ("3", "2", 2, 4),
            ("3", "1", 1, 5),
            ("4", "3", 10, 6),
            ("4", "1", 5, 7),
            ("1", "5", 2, 8),
        ];

        for (src, dst, val, time) in &vs {
            graph
                .add_edge(
                    *time,
                    *src,
                    *dst,
                    [("value_dec".to_string(), Prop::I32(*val))],
                    None,
                )
                .expect("Couldnt add edge");
        }

        test_storage!(&graph, |graph| {
            let res = balance(graph, "value_dec".to_string(), Direction::BOTH).unwrap();
            let node_one = graph.node("1").unwrap();
            let node_two = graph.node("2").unwrap();
            let node_three = graph.node("3").unwrap();
            let node_four = graph.node("4").unwrap();
            let node_five = graph.node("5").unwrap();
            let expected = HashMap::from([
                (node_one.clone(), -26.0),
                (node_two.clone(), 7.0),
                (node_three.clone(), 12.0),
                (node_four.clone(), 5.0),
                (node_five.clone(), 2.0),
            ]);
            assert_eq!(res, expected);

            let res = balance(graph, "value_dec".to_string(), Direction::IN).unwrap();
            let expected = HashMap::from([
                (node_one.clone(), 6.0),
                (node_two.clone(), 12.0),
                (node_three.clone(), 15.0),
                (node_four.clone(), 20.0),
                (node_five.clone(), 2.0),
            ]);
            assert_eq!(res, expected);

            let res = balance(graph, "value_dec".to_string(), Direction::OUT).unwrap();
            let expected = HashMap::from([
                (node_one, -32.0),
                (node_two, -5.0),
                (node_three, -3.0),
                (node_four, -15.0),
                (node_five, 0.0),
            ]);
            assert_eq!(res, expected);
        });
    }
}
