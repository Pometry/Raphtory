use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::{
        entities::{vertices::vertex_ref::VertexRef, VID},
        state::compute_state::ComputeStateVec,
    },
    db::task::{
        context::Context,
        task::{ATask, Job, Step},
        task_runner::TaskRunner,
        vertex::eval_vertex::EvalVertexView,
    },
    prelude::{GraphViewOps, VertexViewOps},
};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};

fn tarjan<G>(
    node: u64,
    graph: &G,
    index: &mut u64,
    stack: &mut Vec<u64>,
    indices: &mut HashMap<u64, u64>,
    lowlink: &mut HashMap<u64, u64>,
    on_stack: &mut HashSet<u64>,
    result: &mut Vec<Vec<u64>>,
) where
    G: GraphViewOps,
{
    *index += 1;
    indices.insert(node, *index);
    lowlink.insert(node, *index);
    stack.push(node);
    on_stack.insert(node);

    let out_neighbours = graph
        .vertex(node)
        .map(|vv| vv.out_neighbours().iter().map(|vv| vv.id()).collect_vec());

    if let Some(neighbors) = out_neighbours {
        for neighbor in neighbors {
            if !indices.contains_key(&neighbor) {
                tarjan(
                    neighbor, graph, index, stack, indices, lowlink, on_stack, result,
                );
                lowlink.insert(node, lowlink[&node].min(lowlink[&neighbor]));
            } else if on_stack.contains(&neighbor) {
                lowlink.insert(node, lowlink[&node].min(indices[&neighbor]));
            }
        }
    }

    if indices[&node] == lowlink[&node] {
        let mut component = Vec::new();
        let mut top = stack.pop().unwrap();
        on_stack.remove(&top);
        component.push(top);
        while top != node {
            top = stack.pop().unwrap();
            on_stack.remove(&top);
            component.push(top);
        }
        result.push(component);
    }
}

fn tarjan_scc<G>(graph: &G) -> Vec<Vec<u64>>
where
    G: GraphViewOps,
{
    let mut index = 0;
    let mut stack = Vec::new();
    let mut indices: HashMap<u64, u64> = HashMap::new();
    let mut lowlink: HashMap<u64, u64> = HashMap::new();
    let mut on_stack: HashSet<u64> = HashSet::new();
    let mut result: Vec<Vec<u64>> = Vec::new();

    let vertices = graph.vertices().id().collect::<Vec<u64>>();

    for node in vertices {
        if !indices.contains_key(&node) {
            tarjan(
                node,
                graph,
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

pub fn strongly_connected_components<G>(graph: &G, threads: Option<usize>) -> Vec<Vec<u64>>
where
    G: GraphViewOps,
{
    #[derive(Clone, Debug, Default)]
    struct SCCNode {
        is_scc_node: bool,
    }

    let ctx: Context<G, ComputeStateVec> = graph.into();
    let step1 = ATask::new(move |vv: &mut EvalVertexView<'_, G, _, _>| {
        let id = vv.id();
        let mut out_components = HashSet::new();
        let mut to_check_stack = Vec::new();
        vv.out_neighbours().id().for_each(|id| {
            out_components.insert(id);
            to_check_stack.push(id);
        });

        while let Some(neighbour_id) = to_check_stack.pop() {
            if let Some(neighbour) = vv.graph.vertex(neighbour_id) {
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
    let results_type = std::any::type_name::<u64>();

    let res = runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<SCCNode>| {
            let layers: crate::core::entities::LayerIds = graph.layer_ids();
            let edge_filter = graph.edge_filter();
            local
                .iter()
                .enumerate()
                .filter_map(|(v_ref_id, state)| {
                    let v_ref = VID(v_ref_id);
                    graph
                        .has_vertex_ref(VertexRef::Internal(v_ref), &layers, edge_filter)
                        .then_some((v_ref_id, state.is_scc_node.clone()))
                })
                .collect::<HashMap<_, _>>()
        },
        threads,
        1,
        None,
        None,
    );

    let algo_res: AlgorithmResult<G, bool, bool> =
        AlgorithmResult::new(graph.clone(), "Cycle nodes", results_type, res);
    let res = algo_res.get_all_with_names();
    let cycle_nodes = res
        .into_iter()
        .filter(|(_, is_cycle_node)| is_cycle_node.unwrap_or(false))
        .map(|(v, _)| v)
        .collect_vec();

    let sub_graph = graph.subgraph(cycle_nodes.clone());

    return tarjan_scc(&sub_graph);
}

#[cfg(test)]
mod strongly_connected_components_tests {
    use crate::{
        algorithms::community_detection::scc::strongly_connected_components,
        prelude::{AdditionOps, Graph, NO_PROPS},
    };

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

        let scc_nodes = strongly_connected_components(&graph, None);

        assert_eq!(scc_nodes, vec![vec![8, 7, 5, 2, 6]]);
    }
}
