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
use rand::Rng;
use raphtory_api::core::utils::hashing::calculate_hash;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tracing::debug;

/// Label carried by a node that has not (yet) been assigned a community.
///
/// Only used when `init_state` is supplied for unseeded nodes.
/// Safe as a sentinel because `usize::MAX` is already the codebase's "not a node"
/// marker (see `VID::is_initialised`), so it can never collide with a node index.
const NO_LABEL: usize = usize::MAX;

#[derive(Copy, Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct LabelPropState {
    pub community_id: usize,
    pub alternate_id: Option<usize>, // set to previous value when community_id has changed; None once settled
    #[serde(skip)]
    is_changed: bool, // derive(Default) initializes to false
}

// Per-thread scratch tally for `step3` vote counts. Keeping them out of `LabelPropState` spares
// the runner a deep `HashMap` clone per node per superstep.
thread_local! {
    static LABEL_COUNTS: RefCell<FxHashMap<usize, usize>> = RefCell::new(FxHashMap::default());
}

/// Deterministic pseudorandom rank for `label` as seen by `node`, used only to break vote-count
/// ties in `step3`.
///
/// `node` is the VID and the super-step is not mixed in, so a node's preference order is stable
/// across iterations: a standing tie resolves the same way each time and adds no churn to
/// `global_diff`.
fn tie_rank(seed: u64, node: usize, label: usize) -> u64 {
    calculate_hash(&(seed, node, label))
}

/// Computes components using a label propagation algorithm
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `iter_count` - Number of iterations
/// - `seed` - (Optional) Seeds the tie-break draw. The value used is printed in the stopping summary
///   and can be passed back here to reproduce a run.
/// - `threads` - (Optional) Number of threads to use
/// - `init_state` - (Optional) HashMap of node VID to community ID. When absent, every node starts
///   in its own community. When present, only the nodes it names are labelled and active; the rest
///   start unlabelled and may acquire a label from their neighbours. A map covering every node
///   reproduces a previous warm-start behaviour exactly.
/// - `rel_tol` - (Optional) Relative-improvement threshold to track convergence. An iteration counts
///   as progress only if its changed-node count drops below `best * (1 - rel_tol)`. Defaults to 3e-4.
/// - `patience` - (Optional) Stop after this many consecutive iterations without progress. Defaults to 10.
///
/// # Returns
///
/// A `TypedNodeState` mapping each node to its `LabelPropState`: its `community_id`, plus
/// `alternate_id` (the previous label it swaps with while oscillating; `None` once converged, and
/// also `None` until the node has been labeled a whole iteration.
pub fn label_propagation<G>(
    g: &G,
    iter_count: usize,
    seed: Option<u64>,
    threads: Option<usize>,
    init_state: Option<HashMap<usize, usize>>,
    rel_tol: Option<f64>,
    patience: Option<usize>,
) -> TypedNodeState<'static, LabelPropState, G>
where
    G: StaticGraphViewOps,
{
    let mut ctx: Context<G, ComputeStateVec> = g.into();
    let global_diff = accumulators::sum::<usize>(2);
    ctx.global_agg_reset(global_diff);

    let num_nodes = g.count_nodes();
    let active: Arc<Vec<AtomicBool>> =
        Arc::new((0..num_nodes).map(|_| AtomicBool::new(false)).collect());

    // Unseeded runs draw random number
    let tie_seed: u64 = seed.unwrap_or_else(|| rand::rng().random());

    let step1 = ATask::new(move |s| {
        let id = s.node.index();
        let state: &mut LabelPropState = s.get_mut();
        match init_state.as_ref() {
            // Unseeded
            None => {
                state.community_id = id;
                state.is_changed = true; // the actual initialization
            }
            // Seeded: only nodes named in the map get a label, and only they start live.
            Some(map) => {
                let seed = map.get(&id).copied();
                state.community_id = seed.unwrap_or(NO_LABEL);
                state.is_changed = seed.is_some();
            }
        }
        Step::Continue
    });

    let active_step2 = Arc::clone(&active);
    let step2 = ATask::new(move |s: &mut EvalNodeView<_, LabelPropState>| {
        if s.prev().is_changed {
            for nbor in s.neighbours() {
                active_step2[nbor.state_pos].store(true, Ordering::Relaxed);
            }
        }
        Step::Continue
    });

    let active_step3 = Arc::clone(&active);
    let step3 = ATask::new(move |s: &mut EvalNodeView<_, LabelPropState>| {
        // Gate: consume this node's activation flag atomically.
        if !active_step3[s.state_pos].swap(false, Ordering::AcqRel) {
            let state = s.get_mut();
            state.is_changed = false;
            state.alternate_id = None; // clear any stale value from a prior iter
                                       // NB: state.community_id unchanged
            return Step::Continue;
        }

        let id = s.node.index();
        let prev_label = s.prev().community_id;
        let winner_label = LABEL_COUNTS.with(|counts| {
            let mut counts = counts.borrow_mut();
            counts.clear();
            if prev_label != NO_LABEL {
                // initialised nodes vote for their own label
                counts.insert(prev_label, 1);
            }
            for nbor in s.neighbours() {
                let nbor_label = nbor.prev().community_id;
                if nbor_label == NO_LABEL {
                    continue; // unlabelled neighbours don't cast a vote
                }
                let count = counts.entry(nbor_label).or_insert(0);
                *count += 1;
            }
            counts
                .iter()
                // REMOVED: get max label (use usize ID to resolve tie)
                // .max_by(|(k1, v1), (k2, v2)| v1.cmp(v2).then(k1.cmp(k2)))
                // NEW BEHAVIOUR: a tie is settled by pseudorandom rank, which draws the winner uniformly
                // from all the top-tied labels.
                .max_by_key(|&(&label, &count)| (count, tie_rank(tie_seed, id, label)))
                .map(|(&label, _)| label)
        });

        let state: &mut LabelPropState = s.get_mut();
        // No votes at all (unlabelled node, no labelled neighbours) leaves community_id standing.
        if let Some(label) = winner_label {
            state.community_id = label;
        }
        state.is_changed = state.community_id != prev_label;
        if state.is_changed {
            state.alternate_id = (prev_label != NO_LABEL).then_some(prev_label);
            s.global_update(&global_diff, 1);
        } else {
            state.alternate_id = None;
        }
        Step::Continue
    });

    // Synchronous LPA never reaches global_diff == 0 on graphs with locally-bipartite pockets
    // (results in ~period-2 oscillations), so the stopping criterion we use is to wait for
    // `patience` iterations since the improvement was no better than `rel_tol`.
    let rel_tol = rel_tol.unwrap_or(3e-4);
    let patience = patience.unwrap_or(10);
    // (best, stale, n_iter): Check is Fn + called once/iter single-threaded, so the Mutex is uncontended
    let convergence_state = Arc::new(Mutex::new((usize::MAX, 0usize, 0usize)));
    let step4 = Job::Check(Box::new(move |state: &GlobalState<ComputeStateVec>| {
        let diff = state.read(&global_diff);
        let (best, stale, n_iter) = &mut *convergence_state.lock().unwrap();
        *n_iter += 1;
        // check for improvement
        let improved = (diff as f64) < (*best as f64) * (1.0 - rel_tol);
        *best = (*best).min(diff);
        *stale = if improved { 0 } else { *stale + 1 };
        // Stop once fully converged (diff == 0) or the changed-node count has plateaued.
        if diff == 0 || *stale >= patience {
            let pct = 100.0 * diff as f64 / num_nodes as f64;
            // println!("label_propagation: stopped after {n_iter} iters; diff={diff} ({pct:.2}%)");
            debug!(
                "label_propagation: stopped after {n_iter} iters; \
                 diff={diff} ({pct:.2}%); seed={tie_seed}"
            );
            Step::Done
        } else {
            Step::Continue
        }
    }));

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    runner.run(
        vec![Job::new(step1)],
        vec![Job::read_only(step2), Job::new(step3), step4],
        None,
        |_, _, _, local, index| {
            TypedNodeState::new(GenericNodeState::new_from_eval_with_index(
                g.clone(),
                local,
                index,
                None,
            ))
        },
        threads,
        iter_count,
        None,
        None,
    )
}
