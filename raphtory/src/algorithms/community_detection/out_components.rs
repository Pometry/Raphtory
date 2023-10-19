use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::{
        entities::{vertices::vertex_ref::VertexRef, VID},
        state::compute_state::ComputeStateVec,
    },
    db::{
        api::view::{GraphViewOps, VertexViewOps},
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
            vertex::eval_vertex::EvalVertexView,
        },
    },
};
use std::{
    cmp,
    collections::{HashMap, HashSet},
};

#[derive(Clone, Debug, Default)]
struct OutState {
    out_components: Vec<u64>,
}

/// Computes the out components of each vertex in the graph
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `threads` - Number of threads to use
///
/// Returns:
///
/// An AlgorithmResult containing the mapping from vertex to a vector of vertex ids (the nodes out component)
///
pub fn out_components<G>(graph: &G, threads: Option<usize>) -> AlgorithmResult<G, Vec<u64>, Vec<u64>>
    where
        G: GraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = graph.into();
    let step1 = ATask::new(move |vv: &mut EvalVertexView<'_, G, _, _>| {
        let mut out_components = HashSet::new();
        let id = vv.id();
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

        let state: &mut OutState = vv.get_mut();
        state.out_components = out_components.into_iter().collect();
        Step::Done
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    let results_type = std::any::type_name::<u64>();

    let res = runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<OutState>| {
            let layers: crate::core::entities::LayerIds = graph.layer_ids();
            let edge_filter = graph.edge_filter();
            local
                .iter()
                .enumerate()
                .filter_map(|(v_ref_id, state)| {
                    let v_ref = VID(v_ref_id);
                    graph
                        .has_vertex_ref(VertexRef::Internal(v_ref), &layers, edge_filter)
                        .then_some((v_ref_id, state.out_components.clone()))
                })
                .collect::<HashMap<_, _>>()
        },
        threads,
        1,
        None,
        None,
    );
    AlgorithmResult::new(graph.clone(), "Out Components", results_type, res)
}

#[cfg(test)]
mod components_test {
    use crate::prelude::*;

    use super::*;
    use crate::db::api::mutation::AdditionOps;

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
        let results = out_components(&graph, None).get_all_with_names();
        let mut correct = HashMap::new();
        correct.insert("1".to_string(), Some(vec![2,3,4,5,6,7,8]));
        correct.insert("2".to_string(), Some(vec![4,5,6,7,8]));
        correct.insert("3".to_string(), Some(vec![]));
        correct.insert("4".to_string(), Some(vec![6,7]));
        correct.insert("5".to_string(), Some(vec![4,6,7,8]));
        correct.insert("6".to_string(), Some(vec![]));
        correct.insert("7".to_string(), Some(vec![]));
        correct.insert("8".to_string(), Some(vec![]));
        let mut map: HashMap<String, Option<Vec<u64>>> = results
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    v.map(|mut vec| {
                        vec.sort();
                        vec
                    }),
                )
            })
            .collect();
        assert_eq!(map, correct);
    }
}
