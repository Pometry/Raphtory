use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::{entities::VID, state::compute_state::ComputeStateVec},
    db::{
        api::view::{NodeViewOps, StaticGraphViewOps},
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
};
use raphtory_api::core::entities::GID;
use rayon::prelude::*;
use std::collections::HashSet;

#[derive(Clone, Debug, Default)]
struct OutState {
    out_components: Vec<VID>,
}

/// Computes the out components of each node in the graph
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `threads` - Number of threads to use
///
/// Returns:
///
/// An AlgorithmResult containing the mapping from node to a vector of node ids (the nodes out component)
///
pub fn out_components<G>(
    graph: &G,
    threads: Option<usize>,
) -> AlgorithmResult<G, Vec<GID>, Vec<GID>>
where
    G: StaticGraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = graph.into();
    let step1 = ATask::new(move |vv: &mut EvalNodeView<G, OutState>| {
        let mut out_components = HashSet::new();
        let mut to_check_stack = Vec::new();
        vv.out_neighbours().iter().for_each(|node| {
            let id = node.node;
            out_components.insert(id);
            to_check_stack.push(id);
        });
        while let Some(neighbour_id) = to_check_stack.pop() {
            if let Some(neighbour) = vv.graph().node(neighbour_id) {
                neighbour.out_neighbours().iter().for_each(|node| {
                    let id = node.node;
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
            graph
                .nodes()
                .par_iter()
                .map(|node| {
                    let VID(id) = node.node;
                    let comps = local[id]
                        .out_components
                        .iter()
                        .map(|vid| graph.node_id(*vid))
                        .collect();
                    (id, comps)
                })
                .collect()
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
    use super::*;
    use crate::{db::api::mutation::AdditionOps, prelude::*, test_storage};
    use std::collections::HashMap;

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
            let results = out_components(graph, None).get_all_with_names();
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
                .map(|(k, mut v)| {
                    v.sort();
                    (k, v.into_iter().filter_map(|v| v.as_u64()).collect())
                })
                .collect();
            assert_eq!(map, correct);
        });
    }
}
