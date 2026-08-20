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
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

/// Label carried by a node that has not (yet) been assigned a community.
///
/// Only used when `init_state` is supplied for unseeded nodes.
/// Safe as a sentinel because `usize::MAX` is already the codebase's "not a node"
/// marker (see `VID::is_initialised`), so it can never collide with a node index.
const NO_LABEL: usize = usize::MAX;

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct LabelPropState {
    #[serde(skip)]
    nbors: HashMap<usize, usize>,
    pub community_id: usize,
    pub alternate_id: Option<usize>, // set to previous value when community_id has changed; None once settled
    #[serde(skip)]
    is_changed: bool, // derive(Default) initializes to false
}

/// Computes components using a label propagation algorithm
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `iter_count` - Number of iterations
/// - `seed` - (Optional) Array of 32 bytes of u8 which is set as the rng seed
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
    _seed: Option<[u8; 32]>,
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

        let prev_id = s.prev().community_id;
        let nbor_iter = s.neighbours();
        let state: &mut LabelPropState = s.get_mut();
        // each node votes for its own label but only if it's initialised
        state.nbors = if prev_id != NO_LABEL {
            HashMap::from([(prev_id, 1)])
        } else {
            HashMap::new()
        };
        // get labels from neighbors
        for nbor in nbor_iter {
            let nbor_id = nbor.prev().community_id;
            if nbor_id == NO_LABEL {
                continue; // unlabelled neihbours don't cast a vote
            }
            // below could be written instead as:
            // *state.nbors.entry(nbor_id).or_insert(0) += 1;
            state
                .nbors
                .insert(nbor_id, *state.nbors.get(&nbor_id).unwrap_or(&0) + 1);
        }
        // get max label (use usize ID to resolve tie)
        if let Some((&label, _)) = state
            .nbors
            .iter()
            .max_by(|(k1, v1), (k2, v2)| v1.cmp(v2).then(k1.cmp(k2)))
        {
            state.community_id = label;
        }
        state.is_changed = state.community_id != prev_id;
        if state.is_changed {
            state.alternate_id = (prev_id != NO_LABEL).then_some(prev_id);
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
            println!("label_propagation: stopped after {n_iter} iters; diff={diff} ({pct:.2}%)");
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
