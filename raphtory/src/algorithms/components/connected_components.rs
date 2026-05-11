use crate::{
    db::api::{
        state::{GenericNodeState, Index, TypedNodeState},
        view::{NodeViewOps, StaticGraphViewOps},
    },
    prelude::GraphViewOps,
};
// use disjoint_sets::AUnionFind;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    sync::atomic::{AtomicUsize, Ordering},
};

/// Keeps track of node assignments to weakly-connected components
///
/// `labels` tracks the assignment of nodes to labels
#[derive(Debug)]
pub struct AUnionFind {
    labels: Vec<AtomicUsize>,
}

impl AUnionFind {
    pub fn new(n: usize) -> Self {
        let labels = (0..n).map(AtomicUsize::new).collect();
        Self { labels }
    }

    /// Link two chunks `chunk_id_1` and `chunk_id_2` such that they will be part of the same
    /// component in the final result.
    ///
    /// The components always link from larger id to smaller id. If while linking, we find that
    /// the component was already linked, we rewire that link as well (the implementation is effectively
    /// the same as calling this function recursively)
    pub fn union(&self, label_1: usize, label_2: usize) {
        let mut src = label_1.max(label_2);
        let mut dst = label_1.min(label_2);
        while src != dst {
            let old_label = self.labels[src].fetch_min(dst, Ordering::Relaxed);
            if old_label > dst {
                src = old_label
            } else {
                src = dst;
                dst = old_label
            }
        }
    }

    pub fn find(&self, index: usize) -> usize {
        let mut component_id = self.labels[index].load(Ordering::Relaxed);
        let mut current_id = index;
        while component_id != current_id {
            let new_component_id = self.labels[component_id].load(Ordering::Relaxed);
            self.labels[current_id].fetch_min(component_id, Ordering::Relaxed); // shortcut
            current_id = component_id;
            component_id = new_component_id;
        }
        if component_id != index {
            self.labels[index].fetch_min(component_id, Ordering::Relaxed); // global shortcut for starting point
        }
        component_id
    }

    pub fn to_vec(self) -> Vec<usize> {
        (0..self.labels.len()).into_par_iter().for_each(|i| {
            self.find(i);
        });
        self.labels
            .into_iter()
            .map(|label| label.into_inner())
            .collect()
    }
}

#[derive(Copy, Clone, PartialEq, Serialize, Deserialize, Debug, Default, Hash, Eq)]
pub struct ConnectedComponent {
    pub component_id: usize,
}

impl PartialEq<usize> for ConnectedComponent {
    fn eq(&self, other: &usize) -> bool {
        self.component_id == *other
    }
}

/// Computes the connected community_detection of a graph using the Simple Connected Components algorithm
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `iter_count` - The number of iterations to run
/// * `threads` - Number of threads to use
///
/// # Returns
///
/// An [NodeState] containing the mapping from each node to its component ID
///
pub fn weakly_connected_components<G>(g: &G) -> TypedNodeState<'static, ConnectedComponent, G>
where
    G: StaticGraphViewOps,
{
    let index = Index::for_graph(g.clone());
    let dss = AUnionFind::new(index.len());
    g.nodes().par_iter().for_each(|node| {
        let src_node: usize = index.index(&node.node).unwrap();
        node.out_neighbours().iter().for_each(|nbor| {
            dss.union(src_node, index.index(&nbor.node).unwrap());
        })
    });
    let result = TypedNodeState::new(GenericNodeState::new_from_eval_with_index_mapped(
        g.clone(),
        dss.to_vec(),
        index,
        |value| ConnectedComponent {
            component_id: value,
        },
        None,
    ));
    result
}
