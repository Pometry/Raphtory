//! # Single Source Shortest Path (SSSP) Algorithm
//!
//! This module provides an implementation of the Single Source Shortest Path algorithm.
//! It finds the shortest paths from a given source node to all other nodes in a graph.
use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::state::{GenericNodeState, Index, TypedNodeState},
        graph::node::NodeView,
    },
    prelude::*,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, mem};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct PathState {
    pub path: Vec<VID>,
}

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
) -> TypedNodeState<'graph, PathState, G> {
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
    let (targets, paths): (Vec<_>, Vec<_>) = paths.into_iter().unzip();
    TypedNodeState::new(GenericNodeState::new_from_eval_with_index_mapped(
        g.clone(),
        g.clone(),
        paths,
        Some(Index::from_iter(targets)),
        |value| PathState { path: value },
        None,
    ))
}
