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
use std::{collections::HashSet, mem};

#[derive(Clone, Debug, Default)]
struct InState {
    in_components: Vec<u64>,
}

/// Computes the in components of each node in the graph
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `threads` - Number of threads to use
///
/// Returns:
///
/// An AlgorithmResult containing the mapping from node to a vector of node ids (the nodes in component)
///
pub fn in_components<G>(graph: &G, threads: Option<usize>) -> AlgorithmResult<G, Vec<u64>, Vec<u64>>
where
    G: StaticGraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = graph.into();
    let step1 = ATask::new(move |vv: &mut EvalNodeView<G, InState>| {
        let mut in_components = HashSet::new();
        let mut to_check_stack = Vec::new();
        vv.in_neighbours().id().for_each(|id| {
            in_components.insert(id);
            to_check_stack.push(id);
        });
        while let Some(neighbour_id) = to_check_stack.pop() {
            if let Some(neighbour) = vv.graph().node(neighbour_id) {
                neighbour.in_neighbours().id().for_each(|id| {
                    if !in_components.contains(&id) {
                        in_components.insert(id);
                        to_check_stack.push(id);
                    }
                });
            }
        }

        let state: &mut InState = vv.get_mut();
        state.in_components = in_components.into_iter().collect();
        Step::Done
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    let results_type = std::any::type_name::<u64>();

    let res = runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, mut local: Vec<InState>| {
            graph
                .nodes()
                .iter()
                .map(|node| {
                    let VID(id) = node.node;
                    (id, mem::take(&mut local[id].in_components))
                })
                .collect()
        },
        threads,
        1,
        None,
        None,
    );
    AlgorithmResult::new(graph.clone(), "In Components", results_type, res)
}

#[cfg(test)]
mod components_test {
    use super::*;
    use crate::{db::api::mutation::AdditionOps, prelude::*};
    use std::collections::HashMap;

    #[test]
    fn in_components_test() {
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
        let results = in_components(&graph, None).get_all_with_names();
        let mut correct = HashMap::new();
        correct.insert("1".to_string(), vec![]);
        correct.insert("2".to_string(), vec![1]);
        correct.insert("3".to_string(), vec![1]);
        correct.insert("4".to_string(), vec![1, 2, 5]);
        correct.insert("5".to_string(), vec![1, 2]);
        correct.insert("6".to_string(), vec![1, 2, 4, 5]);
        correct.insert("7".to_string(), vec![1, 2, 4, 5]);
        correct.insert("8".to_string(), vec![1, 2, 5]);
        let map: HashMap<String, Vec<u64>> = results
            .into_iter()
            .map(|(k, mut v)| {
                v.sort();
                (k, v)
            })
            .collect();
        assert_eq!(map, correct);
    }
}
