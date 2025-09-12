use crate::{
    core::state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    db::{
        api::{
            state::{GenericNodeState, TypedNodeState},
            view::StaticGraphViewOps,
        },
        task::{
            context::{Context, GlobalState},
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::*,
};
use raphtory_api::core::entities::GID;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
struct LabelPropState {
    #[serde(skip)]
    nbors: HashMap<usize, usize>,
    community_id: usize,
}

/// Computes components using a label propagation algorithm
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `iter_count` - Number of iterations
/// - `seed` - (Optional) Array of 32 bytes of u8 which is set as the rng seed
/// - `threads` - (Optional) Number of threads to use
///
/// # Returns
///
/// A vector of hashsets each containing nodes
///
pub fn label_propagation<G>(
    g: &G,
    iter_count: usize,
    seed: Option<[u8; 32]>,
    threads: Option<usize>,
) -> TypedNodeState<'static, HashMap<String, Option<Prop>>, G>
where
    G: StaticGraphViewOps,
{
    let mut ctx: Context<G, ComputeStateVec> = g.into();
    let global_diff = accumulators::sum::<usize>(2);
    ctx.global_agg_reset(global_diff);

    let step1 = ATask::new(move |s| {
        let id = s.node.index();
        let state: &mut LabelPropState = s.get_mut();
        state.community_id = id;
        state.nbors.insert(id, 1);
        Step::Continue
    });

    let step2 = ATask::new(move |s: &mut EvalNodeView<G, LabelPropState>| {
        let prev_id = s.prev().community_id;
        let nbor_iter = s.neighbours();
        let state: &mut LabelPropState = s.get_mut();
        // get labels from neighbors
        for nbor in nbor_iter {
            let nbor_id = nbor.prev().community_id;
            state
                .nbors
                .insert(nbor_id, *state.nbors.get(&nbor_id).unwrap_or(&(1usize)));
        }
        // get max label (use usize ID to resolve tie)
        if let Some((label, _)) = state.nbors.iter().max_by_key(|&(v1, v2)| (v1, v2)) {
            state.community_id = *label;
            if *label != prev_id {
                s.global_update(&global_diff, 1);
            }
        }
        Step::Continue
    });

    let step3 = Job::Check(Box::new(move |state: &GlobalState<ComputeStateVec>| {
        if state.read(&global_diff) > 0 {
            Step::Continue
        } else {
            Step::Done
        }
    }));

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    runner.run(
        vec![Job::new(step1)],
        vec![Job::new(step2), step3],
        None,
        |_, _, _, local| GenericNodeState::new_from_eval(g.clone(), local).transform(),
        threads,
        iter_count,
        None,
        None,
    )
}

#[cfg(test)]
mod lpa_tests {
    /*
    use super::*;
    use crate::test_storage;

    #[test]
    fn lpa_test() {
        let graph: Graph = Graph::new();
        let edges = vec![
            (1, "R1", "R2"),
            (1, "R2", "R3"),
            (1, "R3", "G"),
            (1, "G", "B1"),
            (1, "G", "B3"),
            (1, "B1", "B2"),
            (1, "B2", "B3"),
            (1, "B2", "B4"),
            (1, "B3", "B4"),
            (1, "B3", "B5"),
            (1, "B4", "B5"),
        ];
        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let seed = Some([5; 32]);
            let result = label_propagation(graph, seed).unwrap();

            let expected = vec![
                HashSet::from([
                    graph.node("R1").unwrap(),
                    graph.node("R2").unwrap(),
                    graph.node("R3").unwrap(),
                ]),
                HashSet::from([
                    graph.node("G").unwrap(),
                    graph.node("B1").unwrap(),
                    graph.node("B2").unwrap(),
                    graph.node("B3").unwrap(),
                    graph.node("B4").unwrap(),
                    graph.node("B5").unwrap(),
                ]),
            ];
            for hashset in expected {
                assert!(result.contains(&hashset));
            }
        });
    }
    */
}
