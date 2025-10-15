use crate::{
    db::{
        api::{
            state::{GenericNodeState, TypedNodeState},
            view::{internal::GraphView, NodeViewOps, StaticGraphViewOps},
        },
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::VID;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Formatter},
    mem,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default, Hash, Eq)]
pub struct ConnectedComponent {
    pub component_id: usize,
}

/// Keeps track of node assignments to weakly-connected components
///
/// `node_labels` tracks the assignment of nodes to labels
/// `chunk_labels` tracks the mapping from node labels to final components
struct ComponentState<'graph, G> {
    chunk_labels: Vec<AtomicUsize>,
    node_labels: Vec<AtomicUsize>,
    next_start: AtomicUsize,
    next_chunk: AtomicUsize,
    graph: &'graph G,
}

impl<'graph, G> Debug for ComponentState<'graph, G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentState")
            .field("chunk_labels", &self.chunk_labels)
            .field("node_labels", &self.node_labels)
            .field("next_start", &self.next_start)
            .finish_non_exhaustive()
    }
}

impl<'graph, G: GraphView + 'graph> ComponentState<'graph, G> {
    fn new(graph: &'graph G) -> Self {
        let num_nodes = graph.unfiltered_num_nodes();
        let chunk_labels = (0..num_nodes)
            .map(|_| AtomicUsize::new(usize::MAX))
            .collect();
        let node_labels = (0..num_nodes)
            .map(|_| AtomicUsize::new(usize::MAX))
            .collect();
        let next_start = AtomicUsize::new(0);
        let next_chunk = AtomicUsize::new(0);
        Self {
            chunk_labels,
            node_labels,
            next_start,
            next_chunk,
            graph,
        }
    }

    /// Link two chunks `chunk_id_1` and `chunk_id_2` such that they will be part of the same
    /// component in the final result.
    ///
    /// The components always link from larger id to smaller id. If while linking, we find that
    /// the component was already linked, we rewire that link as well (the implementation is effectively
    /// the same as calling this function recursively)
    fn link_chunks(&self, chunk_id_1: usize, chunk_id_2: usize) {
        let mut src = chunk_id_1.max(chunk_id_2);
        let mut dst = chunk_id_1.min(chunk_id_2);
        while src != dst {
            let old_label = self.chunk_labels[src].fetch_min(dst, Ordering::Relaxed);
            if old_label != usize::MAX {
                if old_label > dst {
                    src = old_label
                } else {
                    src = dst;
                    dst = old_label
                }
            }
        }
    }

    /// Find the next valid starting node for a task
    ///
    /// This function takes care to make sure that there will never be more tasks than
    /// unfiltered nodes in the graph!
    fn next_start(&self) -> Option<(usize, VID)> {
        let mut next_start = self.next_start.fetch_add(1, Ordering::Relaxed);
        if next_start >= self.node_labels.len() {
            return None;
        }
        // only increment this if we still have a potentially valid node to avoid getting too many chunks
        let chunk_id = self.next_chunk_label();
        let old_label = self.chunk_labels[chunk_id].fetch_min(chunk_id, Ordering::Relaxed);
        if old_label != usize::MAX {
            // some other thread managed to initialise our chunk (this seems theoretically possible)
            self.link_chunks(chunk_id, old_label);
        }
        loop {
            //
            if self.node_labels[next_start]
                .compare_exchange(usize::MAX, chunk_id, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                if self.graph.has_node(VID(next_start)) {
                    return Some((chunk_id, VID(next_start)));
                }
            }
            next_start = self.next_start.fetch_add(1, Ordering::Relaxed);
            if next_start >= self.node_labels.len() {
                return None;
            }
        }
    }

    fn next_chunk_label(&self) -> usize {
        self.next_chunk.fetch_add(1, Ordering::Relaxed)
    }

    /// Find a new starting point and run breadth-first search
    ///
    /// If we find a node that was already handled by another chunk, update the label assignments
    /// accordingly using `link_chunks`
    fn run_chunk(&self) -> Option<()> {
        let (chunk_id, start) = self.next_start()?;
        let mut min_label = chunk_id;
        let mut new_frontier = vec![start];
        let mut frontier = vec![];
        while !new_frontier.is_empty() {
            mem::swap(&mut new_frontier, &mut frontier);
            for node_id in frontier.drain(..) {
                for neighbour in NodeView::new_internal(self.graph, node_id).neighbours() {
                    let node_id = neighbour.node;
                    let old_label =
                        self.node_labels[node_id.index()].fetch_min(min_label, Ordering::Relaxed);
                    if old_label != usize::MAX {
                        self.link_chunks(chunk_id, old_label);
                        min_label = min_label.min(old_label);
                    } else {
                        new_frontier.push(node_id);
                    }
                }
            }
        }
        Some(())
    }

    /// Runs breadth-first search in parallel from different starting points until the graph is covered
    ///
    /// During the first phase, the `node_labels` contain the id of the first task that visited that node
    /// and the `chunk_labels` maintain a mapping from task to the minimum over other tasks that also
    /// reached a node first discovered by this task.
    ///
    /// In the second phase, starting from each task, we follow the relabelling map until we hit a root task (i.e., one where the label is mapped to itself)
    /// and simplify the chunk_labels
    ///
    /// Finally, the simplified chunk_labels are used to update the `node_labels` for the final output.
    fn run(self) -> Vec<usize> {
        let num_tasks = rayon::current_num_threads();
        (0..num_tasks)
            .into_par_iter()
            .for_each(|_| while self.run_chunk().is_some() {});
        let num_chunks = self.next_chunk.load(Ordering::Relaxed);
        self.chunk_labels[0..num_chunks]
            .par_iter()
            .enumerate()
            .for_each(|(chunk_id, label)| {
                let mut component_id = label.load(Ordering::Relaxed);
                let mut current_chunk = chunk_id;
                while component_id != current_chunk {
                    current_chunk = component_id;
                    component_id = self.chunk_labels[current_chunk].load(Ordering::Relaxed);
                }
                if component_id != chunk_id {
                    self.chunk_labels[chunk_id].fetch_min(component_id, Ordering::Relaxed);
                }
            });
        self.node_labels
            .into_iter()
            .map(|l| {
                let l = l.into_inner();
                if l != usize::MAX {
                    self.chunk_labels[l].load(Ordering::Relaxed)
                } else {
                    l
                }
            })
            .collect()
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
    // read-lock the graph
    let _cg = g.core_graph().lock();
    let state = ComponentState::new(g);
    let result = state.run();
    TypedNodeState::new(GenericNodeState::new_from_eval_mapped(
        g.clone(),
        result,
        |value| ConnectedComponent {
            component_id: value,
        },
    ))
}
