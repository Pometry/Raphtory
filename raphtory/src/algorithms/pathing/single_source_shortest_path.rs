//! # Single Source Shortest Path (SSSP) Algorithm
//!
//! This module provides an implementation of the Single Source Shortest Path algorithm.
//! It finds the shortest paths from a given source node to all other nodes in a graph.

use raphtory_api::core::entities::AsNodeRef;

use crate::{
    algorithms::algorithm_result::AlgorithmResult, core::entities::VID, db::graph::node::NodeView,
    prelude::*,
};
use std::collections::HashMap;

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
) -> AlgorithmResult<G, Vec<String>, Vec<String>> {
    let results_type = std::any::type_name::<Vec<String>>();
    let mut paths: HashMap<usize, Vec<String>> = HashMap::new();
    if let Some(source_node) = g.node(source) {
        let node_internal_id = source_node.node.0;
        let mut level = 0;
        let mut nextlevel: HashMap<usize, String> = HashMap::new();
        nextlevel.insert(node_internal_id, "1".to_string());

        paths.insert(node_internal_id, vec![source_node.name()]);

        if let Some(0) = cutoff {
            return AlgorithmResult::new(
                g.clone(),
                "Single Source Shortest Path",
                results_type,
                paths,
            );
        }

        while !nextlevel.is_empty() {
            let thislevel: HashMap<usize, String> = nextlevel.clone();
            nextlevel.clear();
            for v in thislevel.keys() {
                let node = NodeView::new_internal(g.clone(), VID::from(*v));
                for w in node.neighbours() {
                    if !paths.contains_key(&w.node.0) {
                        let mut new_path = paths.get(v).unwrap().clone();
                        new_path.push(w.name());
                        paths.insert(w.node.0, new_path);
                        nextlevel.insert(w.node.0, "1".to_string());
                    }
                }
            }
            level += 1;
            if let Some(c) = cutoff {
                if c <= level {
                    break;
                }
            }
        }
    }
    AlgorithmResult::new(
        g.clone(),
        "Single Source Shortest Path",
        results_type,
        paths,
    )
}

#[cfg(test)]
mod sssp_tests {
    use super::*;
    use crate::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        test_storage,
    };
    use raphtory_api::core::utils::logging::global_info_logger;
    use tracing::info;

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
            let binding = single_source_shortest_path(graph, 1, Some(4));
            let results = binding.get_all_with_names();
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
            assert_eq!(results, expected);
            let binding = single_source_shortest_path(graph, 5, Some(4));
            info!("{:?}", binding.get_all_with_names());
        });
    }
}
