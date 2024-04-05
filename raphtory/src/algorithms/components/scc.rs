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
use itertools::Itertools;
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
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

    let sub_graph = runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<SCCNode>| {
            graph.subgraph(
                local
                    .into_iter()
                    .enumerate()
                    .filter(|(_, state)| state.is_scc_node)
                    .map(|(vid, _)| VID(vid)),
            )
        },
        threads,
        1,
        None,
        None,
    );
    let results_type = std::any::type_name::<usize>();
    let groups = tarjan_scc(&sub_graph);
    let mut res = HashMap::new();
    for (id, group) in groups.into_iter().enumerate() {
        for VID(node) in group {
            res.insert(node, id);
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

        let mut scc_nodes = strongly_connected_components(&graph, None)
            .group_by()
            .into_values()
            .collect_vec();

        scc_nodes[0].sort();
        assert_eq!(scc_nodes, [["2", "5", "6", "7", "8"]]);
    }
}
