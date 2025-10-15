use crate::{
    core::state::{
        accumulator_id::accumulators::{max, sum},
        compute_state::ComputeStateVec,
    },
    db::{
        api::{
            state::{GenericNodeState, TypedNodeState},
            view::{NodeViewOps, StaticGraphViewOps},
        },
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
};
use num_traits::abs;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
struct Hits {
    hub_score: f32,
    auth_score: f32,
}

impl Default for Hits {
    fn default() -> Self {
        Self {
            hub_score: 1f32,
            auth_score: 1f32,
        }
    }
}

/// HITS (Hubs and Authority) Algorithm:
/// AuthScore of a node (A) = Sum of HubScore of all nodes pointing at node (A) from previous iteration /
///     Sum of HubScore of all nodes in the current iteration
///
/// HubScore of a node (A) = Sum of AuthScore of all nodes pointing away from node (A) from previous iteration /
///     Sum of AuthScore of all nodes in the current iteration
///
/// # Arguments
///
/// - `g`: A reference to the graph.
/// - `iter_count` - The number of iterations to run
/// - `threads` - Number of threads to use
///
/// # Returns
///
/// An [AlgorithmResult] object containing the mapping from node ID to the hub and authority score of the node
pub fn hits<G: StaticGraphViewOps>(
    g: &G,
    iter_count: usize,
    threads: Option<usize>,
) -> TypedNodeState<'static, Hits, G> {
    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let recv_hub_score = sum::<f32>(2);
    let recv_auth_score = sum::<f32>(3);

    let total_hub_score = sum::<f32>(4);
    let total_auth_score = sum::<f32>(5);

    let max_diff_hub_score = max::<f32>(6);
    let max_diff_auth_score = max::<f32>(7);

    ctx.agg(recv_hub_score);
    ctx.agg(recv_auth_score);
    ctx.agg_reset(recv_hub_score);
    ctx.agg_reset(recv_auth_score);
    ctx.global_agg(total_hub_score);
    ctx.global_agg(total_auth_score);
    ctx.global_agg(max_diff_hub_score);
    ctx.global_agg(max_diff_auth_score);
    ctx.global_agg_reset(total_hub_score);
    ctx.global_agg_reset(total_auth_score);
    ctx.global_agg_reset(max_diff_hub_score);
    ctx.global_agg_reset(max_diff_auth_score);

    let step2 = ATask::new(move |evv: &mut EvalNodeView<G, Hits>| {
        let hub_score = evv.get().hub_score;
        let auth_score = evv.get().auth_score;
        for t in evv.out_neighbours() {
            t.update(&recv_hub_score, hub_score)
        }
        for t in evv.in_neighbours() {
            t.update(&recv_auth_score, auth_score)
        }
        Step::Continue
    });

    let step3 = ATask::new(move |evv: &mut EvalNodeView<G, Hits>| {
        let recv_hub_score = evv.read(&recv_hub_score);
        let recv_auth_score = evv.read(&recv_auth_score);

        evv.global_update(&total_hub_score, recv_hub_score);
        evv.global_update(&total_auth_score, recv_auth_score);
        Step::Continue
    });

    let step4 = ATask::new(move |evv: &mut EvalNodeView<G, Hits>| {
        let recv_hub_score = evv.read(&recv_hub_score);
        let recv_auth_score = evv.read(&recv_auth_score);

        evv.get_mut().auth_score =
            recv_hub_score / evv.read_global_state(&total_hub_score).unwrap();
        evv.get_mut().hub_score =
            recv_auth_score / evv.read_global_state(&total_auth_score).unwrap();

        let prev_hub_score = evv.prev().hub_score;
        let curr_hub_score = evv.get().hub_score;

        let md_hub_score = abs(prev_hub_score - curr_hub_score);
        evv.global_update(&max_diff_hub_score, md_hub_score);

        let prev_auth_score = evv.prev().auth_score;
        let curr_auth_score = evv.get().auth_score;
        let md_auth_score = abs(prev_auth_score - curr_auth_score);
        evv.global_update(&max_diff_auth_score, md_auth_score);

        Step::Continue
    });

    let max_diff_hs = 0.01f32;
    let max_diff_as = max_diff_hs;

    let step5 = Job::Check(Box::new(move |state| {
        if state.read(&max_diff_hub_score) <= max_diff_hs
            && state.read(&max_diff_auth_score) <= max_diff_as
        {
            Step::Done
        } else {
            Step::Continue
        }
    }));

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![],
        vec![Job::new(step2), Job::new(step3), Job::new(step4), step5],
        None,
        |_, _, _, local| {
            TypedNodeState::new(GenericNodeState::new_from_eval(g.clone(), local))
        },
        threads,
        iter_count,
        None,
        None,
    )
}
