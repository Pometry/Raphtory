//! # Single Source Shortest Path (SSSP) Algorithm
//!
//! This module provides an implementation of the Single Source Shortest Path algorithm.
//! It finds the shortest paths from a given source node to all other nodes in a graph.
use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::state::{Index, NodeState},
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::*,
};
use std::{collections::HashMap, mem};

/// Calculates the single source shortest paths from a given source node.
///
/// # Arguments
///
/// - `g: &G`: A reference to the graph. Must implement `GraphViewOps`.
/// - `source: T`: The source node. Must implement `InputNode`.
/// - `cutoff: Option<usize>`: An optional cutoff level. The algorithm will stop if this level is reached.
///
/// # Returns
///
/// Returns an `AlgorithmResult<String, Vec<String>>` containing the shortest paths from the source to all reachable nodes.
///
pub fn single_source_shortest_path<'graph, G: GraphViewOps<'graph>, T: AsNodeRef>(
    g: &G,
    source: T,
    cutoff: Option<usize>,
) -> NodeState<'graph, Nodes<'graph, G>, G> {
    let mut paths: HashMap<VID, Vec<VID>> = HashMap::new();
    if let Some(source_node) = g.node(source) {
        let node_internal_id = source_node.node;
        let mut level = 0;
        let mut nextlevel = vec![node_internal_id];
        paths.insert(node_internal_id, vec![node_internal_id]);
        let mut thislevel = vec![];

        while !nextlevel.is_empty() {
            if Some(level) == cutoff {
                break;
            }
            mem::swap(&mut thislevel, &mut nextlevel);
            nextlevel.clear();
            for v in thislevel.iter() {
                let node = NodeView::new_internal(g, *v);
                for w in node.neighbours() {
                    if !paths.contains_key(&w.node) {
                        let mut new_path = paths.get(v).unwrap().clone();
                        new_path.push(w.node);
                        paths.insert(w.node, new_path);
                        nextlevel.push(w.node);
                    }
                }
            }
            level += 1;
        }
    }
    NodeState::new_from_map(g.clone(), paths, |v| {
        Nodes::new_filtered(g.clone(), g.clone(), Some(Index::from_iter(v)), None)
    })
}

#[cfg(test)]
mod sssp_tests {
    use super::*;
    use crate::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        test_storage,
    };
    use raphtory_api::core::utils::logging::global_info_logger;

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();
        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    #[test]
    fn test_sssp_1() {
        global_info_logger();
        let graph = load_graph(vec![
            (0, 1, 2),
            (1, 1, 3),
            (2, 1, 4),
            (3, 2, 3),
            (4, 2, 4),
            (5, 3, 4),
            (6, 4, 4),
            (7, 4, 5),
            (8, 5, 6),
        ]);

        test_storage!(&graph, |graph| {
            let results = single_source_shortest_path(graph, 1, Some(4));
            let expected: HashMap<String, Vec<String>> = HashMap::from([
                ("1".to_string(), vec!["1".to_string()]),
                ("2".to_string(), vec!["1".to_string(), "2".to_string()]),
                ("3".to_string(), vec!["1".to_string(), "3".to_string()]),
                ("4".to_string(), vec!["1".to_string(), "4".to_string()]),
                (
                    "5".to_string(),
                    vec!["1".to_string(), "4".to_string(), "5".to_string()],
                ),
                (
                    "6".to_string(),
                    vec![
                        "1".to_string(),
                        "4".to_string(),
                        "5".to_string(),
                        "6".to_string(),
                    ],
                ),
            ]);
            assert_eq!(expected.len(), results.len());
            for (node, values) in expected {
                assert_eq!(results.get_by_node(node).unwrap().name(), values);
            }
            let _ = single_source_shortest_path(graph, 5, Some(4));
        });
    }
}
