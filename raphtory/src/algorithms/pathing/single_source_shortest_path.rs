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
        Nodes::new_filtered(g.clone(), g.clone(), Index::from_iter(v), None)
    })
}
