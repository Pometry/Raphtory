use crate::{
    db::{
        api::{
            state::NodeState,
            view::{internal::GraphView, NodeViewOps, StaticGraphViewOps},
        },
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::VID;
use rayon::prelude::*;
use std::{
    fmt::{Debug, Formatter},
    mem,
    sync::atomic::{AtomicUsize, Ordering},
};

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
            let old_label = self.chunk_labels[src].fetch_min(dst, Ordering::Acquire);
            if old_label > dst {
                src = old_label
            } else {
                src = dst;
                dst = old_label
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
        self.chunk_labels[chunk_id].fetch_min(chunk_id, Ordering::Relaxed);
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
        self.next_chunk.fetch_add(1, Ordering::Release) // synchronises with `Acquire` in `run` to ensure all updates from the chunk task are visible
    }

    /// Find a new starting point and run breadth-first search
    ///
    /// If we find a node that was already handled by another chunk, update the label assignments
    /// accordingly using `link_chunks`
    fn run_chunk(&self) -> Option<()> {
        let (chunk_id, start) = self.next_start()?;

        let mut new_frontier = vec![start];
        let mut frontier = vec![];
        while !new_frontier.is_empty() {
            mem::swap(&mut new_frontier, &mut frontier);
            for node_id in frontier.drain(..) {
                for neighbour in NodeView::new_internal(self.graph, node_id).neighbours() {
                    let node_id = neighbour.node;
                    match self.node_labels[node_id.index()].compare_exchange(
                        usize::MAX,
                        chunk_id,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            // node not visited previously
                            new_frontier.push(node_id);
                        }
                        Err(old_label) => {
                            self.link_chunks(chunk_id, old_label);
                        }
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
        let num_chunks = self.next_chunk.load(Ordering::Acquire); // synchronises with `Release` in `next_chunk_label` to ensure all updates from the chunk task are visible
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
                    self.chunk_labels[chunk_id].fetch_min(component_id, Ordering::Release);
                    // synchronise with `Acquire` below such that we are guaranteed to have the final label
                }
            });
        self.node_labels
            .into_iter()
            .map(|l| {
                let l = l.into_inner();
                if l != usize::MAX {
                    self.chunk_labels[l].load(Ordering::Acquire) // synchronise with `Release` above such that we are guaranteed to have the final label
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
pub fn weakly_connected_components<G>(g: &G) -> NodeState<'static, usize, G>
where
    G: StaticGraphViewOps,
{
    // read-lock the graph
    let _cg = g.core_graph().lock();
    let state = ComponentState::new(g);
    let result = state.run();
    NodeState::new_from_eval(g.clone(), result)
}

#[cfg(test)]
mod cc_test {
    use super::*;
    use crate::{db::api::mutation::AdditionOps, prelude::*, test_storage};
    use ahash::HashSet;
    use itertools::*;
    use quickcheck_macros::quickcheck;
    use std::{collections::BTreeSet, iter::once};

    fn assert_same_partition<G: GraphView, ID: Into<GID>>(
        left: NodeState<usize, G>,
        right: impl IntoIterator<Item = impl IntoIterator<Item = ID>>,
    ) {
        let left_groups: HashSet<BTreeSet<_>> = left
            .groups()
            .into_iter_groups()
            .map(|(_, nodes)| nodes.id().collect())
            .collect();
        let right_groups: HashSet<BTreeSet<_>> = right
            .into_iter()
            .map(|inner| inner.into_iter().map(|id| id.into()).collect())
            .collect();
        assert_eq!(left_groups, right_groups);
    }

    #[test]
    fn run_loop_simple_connected_components() {
        let graph = Graph::new();

        let edges = vec![
            (1, 2, 1),
            (2, 3, 2),
            (3, 4, 3),
            (3, 5, 4),
            (6, 5, 5),
            (7, 8, 6),
            (8, 7, 7),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            for _ in 0..1000 {
                let results = weakly_connected_components(graph);
                assert_same_partition(results, [1..=6, 7..=8]);
            }
        });
    }

    #[test]
    fn simple_connected_components_2() {
        let graph = Graph::new();

        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = weakly_connected_components(graph);
            assert_same_partition(results, [1..=11]);
        });
    }

    #[test]
    fn test_multiple_components() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (2, 2, 1),
            (3, 3, 1),
            (1, 10, 11),
            (2, 20, 21),
            (3, 30, 31),
        ];
        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        for _ in 0..1000 {
            let result = weakly_connected_components(&graph);
            assert_same_partition(
                result,
                [vec![1, 2, 3], vec![10, 11], vec![20, 21], vec![30, 31]],
            )
        }
    }

    // connected community_detection on a graph with 1 node and a self loop
    #[test]
    fn simple_connected_components_3() {
        let graph = Graph::new();

        let edges = vec![(1, 1, 1)];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            for _ in 0..1000 {
                // loop to test for weird non-deterministic behaviour
                let results = weakly_connected_components(graph);
                assert_same_partition(results, [[1]]);
            }
        });
    }

    #[test]
    fn windowed_connected_components() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).expect("add edge");
        graph.add_edge(0, 2, 1, NO_PROPS, None).expect("add edge");
        graph.add_edge(9, 3, 4, NO_PROPS, None).expect("add edge");
        graph.add_edge(9, 4, 3, NO_PROPS, None).expect("add edge");

        test_storage!(&graph, |graph| {
            let results = weakly_connected_components(graph);
            assert_same_partition(results, [[1, 2], [3, 4]]);

            let wg = graph.window(0, 2);
            let results = weakly_connected_components(&wg);
            assert_same_partition(results, [[1, 2]]);
        });
    }

    #[quickcheck]
    fn circle_graph_edges(vs: Vec<u64>) {
        if !vs.is_empty() {
            let vs = vs.into_iter().unique().collect::<Vec<u64>>();

            let _smallest = vs.iter().min().unwrap();

            let first = vs[0];
            // pairs of nodes from vs one after the next
            let edges = vs
                .iter()
                .zip(chain!(vs.iter().skip(1), once(&first)))
                .map(|(a, b)| (*a, *b))
                .collect::<Vec<(u64, u64)>>();

            assert_eq!(edges[0].0, first);
            assert_eq!(edges.last().unwrap().1, first);
        }
    }

    #[quickcheck]
    fn circle_graph_the_smallest_value_is_the_cc(vs: Vec<u64>) {
        if !vs.is_empty() {
            let graph = Graph::new();

            let vs = vs.into_iter().unique().collect::<Vec<u64>>();

            let first = vs[0];

            // pairs of nodes from vs one after the next
            let edges = vs
                .iter()
                .zip(chain!(vs.iter().skip(1), once(&first)))
                .map(|(a, b)| (*a, *b))
                .collect::<Vec<(u64, u64)>>();

            for (src, dst) in edges.iter() {
                graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
            }

            test_storage!(&graph, |graph| {
                // now we do connected community_detection over window 0..1
                let res = weakly_connected_components(graph);
                assert_same_partition(res, [vs.clone()]);
            });
        }
    }
}
