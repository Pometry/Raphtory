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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct LabelPropState {
    #[serde(skip)]
    nbors: HashMap<usize, usize>,
    pub community_id: usize,
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
    _seed: Option<[u8; 32]>,
    threads: Option<usize>,
) -> TypedNodeState<'static, LabelPropState, G>
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
        Step::Continue
    });

    let step2 = ATask::new(move |s: &mut EvalNodeView<G, LabelPropState>| {
        let prev_id = s.prev().community_id;
        let nbor_iter = s.neighbours();
        let state: &mut LabelPropState = s.get_mut();
        state.nbors = HashMap::from([(prev_id, 1)]);
        // get labels from neighbors
        for nbor in nbor_iter {
            let nbor_id = nbor.prev().community_id;
            state
                .nbors
                .insert(nbor_id, *state.nbors.get(&nbor_id).unwrap_or(&(0)) + 1);
        }
        // get max label (use usize ID to resolve tie)
        if let Some((&label, _)) = state
            .nbors
            .iter()
            .max_by(|(k1, v1), (k2, v2)| v1.cmp(v2).then(k1.cmp(k2)))
        {
            state.community_id = label;
        }
        if state.community_id != prev_id {
            s.global_update(&global_diff, 1);
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
        |_, _, _, local| {
            TypedNodeState::new(GenericNodeState::new_from_eval(g.clone(), local, None))
        },
        threads,
        iter_count,
        None,
        None,
    )
}
