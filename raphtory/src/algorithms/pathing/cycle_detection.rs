use super::*;
use crate::algorithms::algorithm_result::AlgorithmResult;
use crate::algorithms::community_detection::out_components::out_components;
use crate::core::entities::vertices::vertex_ref::VertexRef;
use crate::core::entities::VID;
use crate::core::state::compute_state::ComputeStateVec;
use crate::db::api::mutation::AdditionOps;
use crate::db::api::view::internal::{Base, BoxableGraphView, InheritViewOps};
use crate::db::task::context::Context;
use crate::db::task::task::{ATask, Job, Step};
use crate::db::task::task_runner::TaskRunner;
use crate::db::task::vertex::eval_vertex::EvalVertexView;
use crate::prelude::*;
use crate::prelude::{GraphViewOps, VertexViewOps};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
struct CycleNode {
    is_cycle_node: bool,
}

pub fn cycle_detection<G>(graph: &G, threads: Option<usize>) -> Vec<String>
where
    G: GraphViewOps,
{
    // let results = out_components(graph, None).get_all_with_names();
    // println!("{:?}", results)

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

        let state: &mut CycleNode = vv.get_mut();
        state.is_cycle_node = out_components.into_iter().contains(&id);
        Step::Done
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    let results_type = std::any::type_name::<u64>();

    let res = runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<CycleNode>| {
            let layers: crate::core::entities::LayerIds = graph.layer_ids();
            let edge_filter = graph.edge_filter();
            local
                .iter()
                .enumerate()
                .filter_map(|(v_ref_id, state)| {
                    let v_ref = VID(v_ref_id);
                    graph
                        .has_vertex_ref(VertexRef::Internal(v_ref), &layers, edge_filter)
                        .then_some((v_ref_id, state.is_cycle_node.clone()))
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

    // let sub_graph = graph.subgraph(cycle_nodes.clone());

    return cycle_nodes;
}

#[cfg(test)]
mod cycle_detection_tests {
    use crate::algorithms::pathing::cycle_detection::cycle_detection;
    use crate::prelude::{AdditionOps, Graph, NO_PROPS};
    use itertools::Itertools;

    #[test]
    fn cycle_detection_test() {
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

        println!("{:?}", cycle_detection(&graph, None));
    }
}
