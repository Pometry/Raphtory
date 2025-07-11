use crate::{
    core::{entities::VID, state::compute_state::ComputeStateVec},
    db::{
        api::{
            state::{Index, NodeState},
            view::{NodeViewOps, StaticGraphViewOps},
        },
        graph::{node::NodeView, nodes::Nodes},
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::GraphViewOps,
};
use indexmap::IndexSet;
use itertools::Itertools;
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};

#[derive(Clone, Debug, Default)]
struct OutState {
    out_components: Vec<VID>,
}

/// Computes the out components of each node in the graph
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `threads` - Number of threads to use
///
/// # Returns
///
/// An [AlgorithmResult] containing the mapping from each node to a vector of node ids (the nodes out component)
///
pub fn out_components<G>(g: &G, threads: Option<usize>) -> NodeState<'static, Nodes<'static, G>, G>
where
    G: StaticGraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = g.into();
    let step1 = ATask::new(move |vv: &mut EvalNodeView<_, OutState>| {
        let mut out_components = HashSet::new();
        let mut to_check_stack = Vec::new();
        vv.out_neighbours().iter().for_each(|node| {
            let id = node.node;
            if out_components.insert(id) {
                to_check_stack.push(id);
            }
        });
        while let Some(neighbour_id) = to_check_stack.pop() {
            if let Some(neighbour) = vv.graph().node(neighbour_id) {
                neighbour.out_neighbours().iter().for_each(|node| {
                    let id = node.node;
                    if out_components.insert(id) {
                        to_check_stack.push(id);
                    }
                });
            }
        }

        let state: &mut OutState = vv.get_mut();
        state.out_components = out_components.into_iter().collect();
        Step::Done
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<OutState>| {
            NodeState::new_from_eval_mapped(g.clone(), local, |v| {
                Nodes::new_filtered(
                    g.clone(),
                    g.clone(),
                    Some(Index::from_iter(v.out_components)),
                    None,
                )
            })
        },
        threads,
        1,
        None,
        None,
    )
}

/// Computes the out-component of a given node in the graph
///
/// # Arguments:
///
/// - `node` - The node whose out-component we wish to calculate
///
/// # Returns:
///
/// Nodes in the out-component with their distances from the starting node.
///
pub fn out_component<'graph, G: GraphViewOps<'graph>>(
    node: NodeView<'graph, G>,
) -> NodeState<'graph, usize, G> {
    let mut out_components = HashMap::new();
    let mut to_check_stack = VecDeque::new();
    node.out_neighbours().iter().for_each(|node| {
        let id = node.node;
        out_components.insert(id, 1usize);
        to_check_stack.push_back((id, 1usize));
    });
    while let Some((neighbour_id, d)) = to_check_stack.pop_front() {
        let d = d + 1;
        if let Some(neighbour) = (&&node.graph).node(neighbour_id) {
            neighbour.out_neighbours().iter().for_each(|node| {
                let id = node.node;
                if let Entry::Vacant(entry) = out_components.entry(id) {
                    entry.insert(d);
                    to_check_stack.push_back((id, d));
                }
            });
        }
    }

    let (nodes, distances): (IndexSet<_, ahash::RandomState>, Vec<_>) =
        out_components.into_iter().sorted().unzip();
    NodeState::new(
        node.graph.clone(),
        node.graph.clone(),
        distances.into(),
        Some(Index::new(nodes)),
    )
}

#[cfg(test)]
mod components_test {
    use super::*;
    use crate::{db::api::mutation::AdditionOps, prelude::*, test_storage};
    use std::collections::HashMap;

    fn check_node(graph: &Graph, node_id: u64, mut correct: Vec<(u64, usize)>) {
        let mut results: Vec<_> = out_component(graph.node(node_id).unwrap())
            .iter()
            .map(|(n, d)| (n.id().as_u64().unwrap(), *d))
            .collect();
        results.sort();
        correct.sort();
        assert_eq!(results, correct);
    }

    #[test]
    fn out_component_test() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (1, 1, 3),
            (1, 2, 3),
            (1, 2, 4),
            (1, 2, 5),
            (1, 5, 4),
            (1, 4, 6),
            (1, 4, 7),
            (1, 5, 8),
        ];

        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        check_node(
            &graph,
            1,
            vec![(2, 1), (3, 1), (4, 2), (5, 2), (6, 3), (7, 3), (8, 3)],
        );
        check_node(
            &graph,
            2,
            vec![(3, 1), (4, 1), (5, 1), (6, 2), (7, 2), (8, 2)],
        );
        check_node(&graph, 3, vec![]);
        check_node(&graph, 4, vec![(6, 1), (7, 1)]);
        check_node(&graph, 5, vec![(4, 1), (6, 2), (7, 2), (8, 1)]);
        check_node(&graph, 6, vec![]);
        check_node(&graph, 7, vec![]);
        check_node(&graph, 8, vec![]);
    }

    #[test]
    fn test_distances() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 4, NO_PROPS, None).unwrap();
        graph.add_edge(0, 4, 5, NO_PROPS, None).unwrap();
        graph.add_edge(0, 5, 3, NO_PROPS, None).unwrap();

        check_node(&graph, 1, vec![(2, 1), (3, 2), (4, 1), (5, 2)]);
    }

    #[test]
    fn out_components_test() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (1, 1, 3),
            (1, 2, 4),
            (1, 2, 5),
            (1, 5, 4),
            (1, 4, 6),
            (1, 4, 7),
            (1, 5, 8),
        ];

        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = out_components(graph, None);
            let mut correct = HashMap::new();
            correct.insert("1".to_string(), vec![2, 3, 4, 5, 6, 7, 8]);
            correct.insert("2".to_string(), vec![4, 5, 6, 7, 8]);
            correct.insert("3".to_string(), vec![]);
            correct.insert("4".to_string(), vec![6, 7]);
            correct.insert("5".to_string(), vec![4, 6, 7, 8]);
            correct.insert("6".to_string(), vec![]);
            correct.insert("7".to_string(), vec![]);
            correct.insert("8".to_string(), vec![]);
            let map: HashMap<String, Vec<u64>> = results
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.name(),
                        v.id()
                            .into_iter_values()
                            .filter_map(|v| v.as_u64())
                            .sorted()
                            .collect(),
                    )
                })
                .collect();
            assert_eq!(map, correct);
        });
    }
}
