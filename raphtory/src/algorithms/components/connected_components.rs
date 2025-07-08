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
use parking_lot::Mutex;
use raphtory_api::core::entities::VID;
use rayon::prelude::*;
use std::{
    mem,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct ComponentState<'graph, G> {
    chunk_labels: Vec<AtomicUsize>,
    node_labels: Vec<AtomicUsize>,
    next_start: AtomicUsize,
    next_chunk: AtomicUsize,
    chunks: Mutex<Vec<Vec<VID>>>,
    graph: &'graph G,
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
        let chunks = Mutex::new(Vec::new());
        Self {
            chunk_labels,
            node_labels,
            next_start,
            next_chunk,
            chunks,
            graph,
        }
    }

    fn next_start(&self, chunk_id: usize) -> Option<VID> {
        loop {
            let next_start = self.next_start.fetch_add(1, Ordering::Relaxed);
            if next_start >= self.node_labels.len() {
                return None;
            }
            let old_chunk_id = self.node_labels[next_start].fetch_min(chunk_id, Ordering::Relaxed);
            if old_chunk_id == usize::MAX {
                if self.graph.has_node(VID(next_start)) {
                    return Some(VID(next_start));
                }
            }
        }
    }

    fn next_chunk_label(&self) -> usize {
        self.next_chunk.fetch_add(1, Ordering::Release)
    }

    fn run_chunk(&self) -> Option<(usize, Vec<VID>)> {
        let chunk_id = self.next_chunk_label();
        let start = self.next_start(chunk_id)?;
        self.chunk_labels[chunk_id].fetch_min(chunk_id, Ordering::Release);
        let mut result = vec![start];
        let mut new_frontier = vec![start];
        let mut frontier = vec![];
        while !new_frontier.is_empty() {
            mem::swap(&mut new_frontier, &mut frontier);
            for node_id in frontier.drain(..) {
                for neighbour in NodeView::new_internal(self.graph, node_id).neighbours() {
                    let node_id = neighbour.node;
                    let old_label =
                        self.node_labels[node_id.index()].fetch_min(chunk_id, Ordering::Relaxed);
                    if old_label < chunk_id {
                        self.chunk_labels[chunk_id].fetch_min(old_label, Ordering::Relaxed);
                    } else if old_label != usize::MAX {
                        self.chunk_labels[old_label].fetch_min(chunk_id, Ordering::Relaxed);
                    } else {
                        // node not visited previously
                        result.push(node_id);
                        new_frontier.push(node_id);
                    }
                }
            }
        }
        Some((chunk_id, result))
    }

    fn run(self) -> Vec<usize> {
        let num_tasks = rayon::current_num_threads();
        (0..num_tasks).into_par_iter().for_each(|_| {
            while let Some((chunk_id, result)) = self.run_chunk() {
                let mut chunks = self.chunks.lock();
                if chunks.len() <= chunk_id {
                    chunks.resize(chunk_id + 1, vec![]);
                }
                chunks[chunk_id] = result;
            }
        });
        self.next_chunk.load(Ordering::Acquire);
        self.chunks
            .lock()
            .par_iter()
            .enumerate()
            .for_each(|(chunk_id, chunk)| {
                if !chunk.is_empty() {
                    let mut component_id = self.chunk_labels[chunk_id].load(Ordering::Acquire);
                    let mut current_chunk = chunk_id;
                    while component_id != current_chunk {
                        current_chunk = component_id;
                        component_id = self.chunk_labels[current_chunk].load(Ordering::Acquire);
                    }
                    if component_id != chunk_id {
                        self.chunk_labels[chunk_id].fetch_min(component_id, Ordering::Relaxed);
                        for node in chunk {
                            self.node_labels[node.index()].store(component_id, Ordering::Relaxed);
                        }
                    }
                }
            });
        self.node_labels
            .into_iter()
            .map(|l| l.into_inner())
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
    use std::{
        cmp::Reverse,
        collections::{BTreeSet, HashMap},
        iter::once,
    };

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
            let results = weakly_connected_components(graph);
            assert_same_partition(results, [1..=6, 7..=8]);
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

    // connected community_detection on a graph with 1 node and a self loop
    #[test]
    fn simple_connected_components_3() {
        let graph = Graph::new();

        let edges = vec![(1, 1, 1)];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = weakly_connected_components(graph);
            assert_same_partition(results, [[1]]);
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
