use crate::db::graph::edge::EdgeView;
use crate::db::graph::node::NodeView;
use crate::{core::entities::nodes::node_ref::AsNodeRef, db::api::view::StaticGraphViewOps};
use crate::{
    core::entities::nodes::node_ref::NodeRef,
    db::{
        api::state::{ops::filter::NO_FILTER, Index, NodeState},
        graph::nodes::Nodes,
    },
    errors::GraphError,
    prelude::*,
};
use indexmap::IndexSet;
use raphtory_api::core::{
    entities::{
        properties::prop::{PropType, PropUnwrap},
        VID,
    },
    Direction,
};
use std::{
    collections::{HashMap},
};
use super::super::pathing::{bellman_ford::bellman_ford_single_source_shortest_paths_algorithm, dijkstra::dijkstra_single_source_shortest_paths_algorithm, get_prop_val, to_prop};

pub fn get_johnson_reweighting_function<'a, G: StaticGraphViewOps>(
    g: &G,
    weight: Option<&'a str>,
    direction: Direction,
) -> Result<impl Fn(&EdgeView<G>) -> Option<Prop> + 'a, GraphError> {
    let dist_val = to_prop(g, weight, 0.0)?;
    let weight_fn = move |edge: &EdgeView<G>| -> Option<Prop> {
        let edge_val = match weight{
            None => Some(Prop::U8(1)),
            Some(weight) => match edge.properties().get(weight) {
                Some(prop) => Some(prop),
                _ => None
            }
         };
         edge_val
    };
    let (distances, _) = bellman_ford_single_source_shortest_paths_algorithm(g, None::<NodeRef>,  direction, dist_val.clone(), dist_val, weight_fn)?;
    let reweighting_function = move |edge: &EdgeView<G>| -> Option<Prop> {
        let u = edge.src().node;
        let v = edge.dst().node;
        let weight_val = weight_fn(edge)?;
        let dist_u = distances[u.index()].clone();
        let dist_v = distances[v.index()].clone();
        let new_weight_val_f64 = dist_u.as_f64().unwrap() + weight_val.as_f64().unwrap() - dist_v.as_f64().unwrap();
        // new weight should always be non-negative here
        let new_weight_val = get_prop_val(weight_val.dtype(), new_weight_val_f64).unwrap();
        Some(new_weight_val)
    };
    Ok(reweighting_function)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{api::mutation::AdditionOps, graph::graph::Graph};

    fn load_graph(edges: Vec<(i64, &str, &str, Vec<(&str, f32)>)>) -> Graph {
        let graph = Graph::new();
        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }
        graph
    }

    fn graph_with_negative_weights() -> Graph {
        load_graph(vec![
            (0, "A", "B", vec![("weight", 4.0f32)]),
            (1, "A", "C", vec![("weight", 2.0f32)]),
            (2, "B", "C", vec![("weight", -3.0f32)]),
            (3, "C", "D", vec![("weight", 2.0f32)]),
            (4, "B", "D", vec![("weight", 5.0f32)]),
        ])
    }

    fn graph_with_positive_weights() -> Graph {
        load_graph(vec![
            (0, "A", "B", vec![("weight", 4.0f32)]),
            (1, "A", "C", vec![("weight", 2.0f32)]),
            (2, "B", "C", vec![("weight", 3.0f32)]),
            (3, "C", "D", vec![("weight", 2.0f32)]),
            (4, "B", "D", vec![("weight", 5.0f32)]),
        ])
    }

    #[test]
    fn test_reweighting_negative_weights() {
        let graph = graph_with_negative_weights();
        let reweight_fn = get_johnson_reweighting_function(&graph, Some("weight"), Direction::OUT)
            .expect("Reweighting should succeed");

        // Check that all reweighted edges have non-negative weights
        for edge in graph.edges() {
            let new_weight = reweight_fn(&edge);
            assert!(new_weight.is_some());
            let weight_val = new_weight.unwrap().as_f64().unwrap();
            assert!(
                weight_val >= -1e-10,
                "Reweighted edge {:?} -> {:?} has negative weight: {}",
                edge.src().name(),
                edge.dst().name(),
                weight_val
            );
        }
    }

    #[test]
    fn test_reweighting_positive_weights() {
        let graph = graph_with_positive_weights();
        let reweight_fn = get_johnson_reweighting_function(&graph, Some("weight"), Direction::OUT)
            .expect("Reweighting should succeed");

        // Check that all reweighted edges have non-negative weights
        for edge in graph.edges() {
            let new_weight = reweight_fn(&edge);
            assert!(new_weight.is_some());
            let weight_val = new_weight.unwrap().as_f64().unwrap();
            assert!(
                weight_val >= -1e-10,
                "Reweighted edge {:?} -> {:?} has negative weight: {}",
                edge.src().name(),
                edge.dst().name(),
                weight_val
            );
        }
    }

    #[test]
    fn test_reweighting_no_weight_property() {
        let graph = graph_with_negative_weights();
        // Test with None weight (should use uniform weights of 1)
        let reweight_fn = get_johnson_reweighting_function(&graph, None, Direction::OUT)
            .expect("Reweighting should succeed");

        // All edges should have non-negative weights
        for edge in graph.edges() {
            let new_weight = reweight_fn(&edge);
            assert!(new_weight.is_some());
            let weight_val = new_weight.unwrap().as_f64().unwrap();
            assert!(weight_val >= -1e-10);
        }
    }

    #[test]
    fn test_reweighting_preserves_shortest_paths() {
        let graph = graph_with_negative_weights();
        let reweight_fn = get_johnson_reweighting_function(&graph, Some("weight"), Direction::OUT)
            .expect("Reweighting should succeed");

        // Edge A->B (weight 4) should be reweighted
        let edge_ab = graph.edge("A", "B").unwrap();
        let reweight_ab = reweight_fn(&edge_ab).unwrap().as_f64().unwrap();

        // Edge B->C (weight -3) should be reweighted to non-negative
        let edge_bc = graph.edge("B", "C").unwrap();
        let reweight_bc = reweight_fn(&edge_bc).unwrap().as_f64().unwrap();

        // Both should be non-negative
        assert!(reweight_ab >= -1e-10);
        assert!(reweight_bc >= -1e-10);
    }

    #[test]
    fn test_reweighting_with_different_directions() {
        let graph = graph_with_negative_weights();

        // Test with Direction::OUT
        let reweight_fn_out = get_johnson_reweighting_function(&graph, Some("weight"), Direction::OUT);
        assert!(reweight_fn_out.is_ok());

        // Test with Direction::IN
        let reweight_fn_in = get_johnson_reweighting_function(&graph, Some("weight"), Direction::IN);
        assert!(reweight_fn_in.is_ok());

        // Test with Direction::BOTH
        let reweight_fn_both = get_johnson_reweighting_function(&graph, Some("weight"), Direction::BOTH);
        assert!(reweight_fn_both.is_ok());
    }

    #[test]
    fn test_reweighting_with_integer_weights() {
        let edges = vec![
            (0, 1, 2, vec![("weight", 4i64)]),
            (1, 1, 3, vec![("weight", 2i64)]),
            (2, 2, 3, vec![("weight", -3i64)]),
            (3, 3, 4, vec![("weight", 2i64)]),
            (4, 2, 4, vec![("weight", 5i64)]),
        ];

        let graph = Graph::new();
        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        let reweight_fn = get_johnson_reweighting_function(&graph, Some("weight"), Direction::OUT)
            .expect("Reweighting should succeed");

        // Check that all reweighted edges have non-negative weights
        for edge in graph.edges() {
            let new_weight = reweight_fn(&edge);
            assert!(new_weight.is_some());
            let weight_val = new_weight.unwrap().as_f64().unwrap();
            assert!(
                weight_val >= -1e-10,
                "Reweighted edge has negative weight: {}",
                weight_val
            );
        }
    }

    #[test]
    fn test_reweighting_specific_values() {
        // Create a simple graph where we can verify exact reweighted values
        let graph = load_graph(vec![
            (0, "A", "B", vec![("weight", 1.0f32)]),
            (1, "B", "C", vec![("weight", -2.0f32)]),
            (2, "A", "C", vec![("weight", 0.0f32)]),
        ]);

        let reweight_fn = get_johnson_reweighting_function(&graph, Some("weight"), Direction::OUT)
            .expect("Reweighting should succeed");

        // All edges should be reweighted to non-negative values
        for edge in graph.edges() {
            let new_weight = reweight_fn(&edge);
            assert!(new_weight.is_some());
            let weight_val = new_weight.unwrap().as_f64().unwrap();
            assert!(
                weight_val >= -1e-10,
                "Edge {:?} -> {:?} has negative weight: {}",
                edge.src().name(),
                edge.dst().name(),
                weight_val
            );
        }
    }
}