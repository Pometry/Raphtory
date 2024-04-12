use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use itertools::Itertools;

use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::{entities::VID, state::compute_state::ComputeStateVec},
    db::{
        api::view::StaticGraphViewOps,
        graph::node::NodeView,
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::*,
};

fn tarjan<'graph, G>(
    node: NodeView<&'graph G>,
    index: &'graph mut u64,
    stack: &'graph mut Vec<VID>,
    indices: &'graph mut HashMap<VID, u64>,
    lowlink: &'graph mut HashMap<VID, u64>,
    on_stack: &'graph mut HashSet<VID>,
    result: &'graph mut Vec<Vec<VID>>,
) where
    G: StaticGraphViewOps,
{
    *index += 1;
    indices.insert(node.node, *index);
    lowlink.insert(node.node, *index);
    stack.push(node.node);
    on_stack.insert(node.node);

    for neighbor in node.out_neighbours() {
        if !indices.contains_key(&neighbor.node) {
            tarjan(
                neighbor.clone(),
                index,
                stack,
                indices,
                lowlink,
                on_stack,
                result,
            );
            lowlink.insert(node.node, lowlink[&node.node].min(lowlink[&neighbor.node]));
        } else if on_stack.contains(&neighbor.node) {
            lowlink.insert(node.node, lowlink[&node.node].min(indices[&neighbor.node]));
        }
    }

    if indices[&node.node] == lowlink[&node.node] {
        let mut component = Vec::new();
        let mut top = stack.pop().unwrap();
        on_stack.remove(&top);
        component.push(top);
        while top != node.node {
            top = stack.pop().unwrap();
            on_stack.remove(&top);
            component.push(top);
        }
        result.push(component);
    }
}

fn tarjan_scc<G>(graph: &G) -> Vec<Vec<VID>>
where
    G: StaticGraphViewOps,
{
    let mut index = 0;
    let mut stack = Vec::new();
    let mut indices: HashMap<VID, u64> = HashMap::new();
    let mut lowlink: HashMap<VID, u64> = HashMap::new();
    let mut on_stack: HashSet<VID> = HashSet::new();
    let mut result: Vec<Vec<VID>> = Vec::new();

    for node in (&graph).nodes() {
        if !indices.contains_key(&node.node) {
            tarjan(
                node,
                &mut index,
                &mut stack,
                &mut indices,
                &mut lowlink,
                &mut on_stack,
                &mut result,
            );
        }
    }
    result
}

pub fn strongly_connected_components<G>(
    graph: &G,
    threads: Option<usize>,
) -> AlgorithmResult<G, usize>
where
    G: StaticGraphViewOps + Debug,
{
    #[derive(Clone, Debug, Default)]
    struct SCCNode {
        is_scc_node: bool,
    }

    let ctx: Context<G, ComputeStateVec> = graph.into();
    let step1 = ATask::new(move |vv: &mut EvalNodeView<G, SCCNode>| {
        let id = vv.id();
        let mut out_components = HashSet::new();
        let mut to_check_stack = Vec::new();
        vv.out_neighbours().id().for_each(|id| {
            out_components.insert(id);
            to_check_stack.push(id);
        });

        while let Some(neighbour_id) = to_check_stack.pop() {
            if let Some(neighbour) = vv.graph().node(neighbour_id) {
                neighbour.out_neighbours().id().for_each(|id| {
                    if !out_components.contains(&id) {
                        out_components.insert(id);
                        to_check_stack.push(id);
                    }
                });
            }
        }

        let state: &mut SCCNode = vv.get_mut();
        state.is_scc_node = out_components.into_iter().contains(&id);
        Step::Done
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let local = runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<SCCNode>| local,
        threads,
        1,
        None,
        None,
    );
    let sub_graph = graph.subgraph(
        local
            .iter()
            .enumerate()
            .filter(|(_, state)| state.is_scc_node)
            .map(|(vid, _)| VID(vid)),
    );
    let results_type = std::any::type_name::<usize>();
    let groups = tarjan_scc(&sub_graph);

    let mut id = groups.len();
    let mut res = HashMap::new();
    for (id, group) in groups.into_iter().enumerate() {
        for VID(node) in group {
            res.insert(node, id);
        }
    }
    for (node, state) in local.into_iter().enumerate() {
        if !state.is_scc_node {
            res.insert(node, id);
            id += 1;
        }
    }

    AlgorithmResult::new(
        graph.clone(),
        "Strongly-connected Components",
        results_type,
        res,
    )
}

#[cfg(test)]
mod strongly_connected_components_tests {
    use crate::{
        algorithms::components::scc::strongly_connected_components,
        prelude::{AdditionOps, Graph, NO_PROPS},
    };
    use itertools::Itertools;
    use std::collections::HashSet;

    #[test]
    fn scc_test() {
        let graph = Graph::new();
        let edges = vec![
            (1, 1, 2),
            (1, 2, 3),
            (1, 2, 5),
            (1, 3, 4),
            (1, 5, 6),
            (1, 6, 4),
            (1, 6, 7),
            (1, 7, 8),
            (1, 8, 6),
            (1, 6, 2),
        ];

        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        let scc_nodes: HashSet<_> = strongly_connected_components(&graph, None)
            .group_by()
            .into_values()
            .map(|mut v| {
                v.sort();
                v
            })
            .collect();

        let expected: HashSet<Vec<String>> = [
            vec!["2", "5", "6", "7", "8"],
            vec!["1"],
            vec!["3"],
            vec!["4"],
        ]
        .into_iter()
        .map(|v| v.into_iter().map(|s| s.to_owned()).collect())
        .collect();
        assert_eq!(scc_nodes, expected);
    }

    #[test]
    fn scc_test_multiple_components() {
        let graph = Graph::new();
        let edges = [
            (1, 2),
            (2, 3),
            (2, 8),
            (3, 4),
            (3, 7),
            (4, 5),
            (5, 3),
            (5, 6),
            (7, 4),
            (7, 6),
            (8, 1),
            (8, 7),
        ];
        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        let scc_nodes: HashSet<_> = strongly_connected_components(&graph, None)
            .group_by()
            .into_values()
            .map(|mut v| {
                v.sort();
                v
            })
            .collect();

        let expected: HashSet<Vec<String>> =
            [vec!["3", "4", "5", "7"], vec!["1", "2", "8"], vec!["6"]]
                .into_iter()
                .map(|v| v.into_iter().map(|s| s.to_owned()).collect())
                .collect();
        assert_eq!(scc_nodes, expected);
    }

    #[test]
    fn scc_test_multiple_components_2() {
        let graph = Graph::new();
        let edges = [(1, 2), (1, 3), (1, 4), (4, 2), (3, 4), (2, 3)];
        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        let scc_nodes: HashSet<_> = strongly_connected_components(&graph, None)
            .group_by()
            .into_values()
            .map(|mut v| {
                v.sort();
                v
            })
            .collect();

        let expected: HashSet<Vec<String>> = [vec!["2", "3", "4"], vec!["1"]]
            .into_iter()
            .map(|v| v.into_iter().map(|s| s.to_owned()).collect())
            .collect();
        assert_eq!(scc_nodes, expected);
    }

    #[test]
    fn scc_test_all_singletons() {
        let graph = Graph::new();
        let edges = [
            (0, 1),
            (1, 2),
            (1, 3),
            (2, 4),
            (2, 5),
            (3, 4),
            (3, 5),
            (4, 6),
        ];
        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        let scc_nodes: HashSet<_> = strongly_connected_components(&graph, None)
            .group_by()
            .into_values()
            .map(|mut v| {
                v.sort();
                v
            })
            .collect();

        let expected: HashSet<Vec<String>> = [
            vec!["0"],
            vec!["1"],
            vec!["2"],
            vec!["3"],
            vec!["4"],
            vec!["5"],
            vec!["6"],
        ]
        .into_iter()
        .map(|v| v.into_iter().map(|s| s.to_owned()).collect())
        .collect();
        assert_eq!(scc_nodes, expected);
    }
}
