use crate::{
    core::state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    db::{
        api::{
            state::{GenericNodeState, Index, TypedNodeState},
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
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Copy, Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct LabelPropState {
    pub community_id: usize,
}

thread_local! {
    static LABEL_COUNTS: RefCell<FxHashMap<usize, usize>> = RefCell::new(FxHashMap::default());
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

    let step2 = ATask::new(move |s: &mut EvalNodeView<_, LabelPropState>| {
        let prev_id = s.prev().community_id;
        let mut best_count = 1usize;
        let mut best_label = prev_id;
        LABEL_COUNTS.with(|counts| {
            let mut counts = counts.borrow_mut();
            counts.clear();
            counts.insert(prev_id, 1);
            for nbor in s.neighbours() {
                let label = nbor.prev().community_id;
                let count = counts.entry(label).or_insert(0);
                *count += 1;
                let count = *count;
                // resolve ties towards the larger label
                if count > best_count || (count == best_count && label > best_label) {
                    best_count = count;
                    best_label = label;
                }
            }
        });
        s.get_mut().community_id = best_label;
        if best_label != prev_id {
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

/// Label propagation over flat label arrays, bypassing the task framework.
///
/// Same synchronous semantics as [`label_propagation`]: every sweep recomputes all labels
/// from the previous sweep's labels, ties resolve towards the larger label, and iteration
/// stops early once no label changes.
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `iter_count` - Maximum number of sweeps
/// - `seed` - (Optional) Array of 32 bytes of u8 which is set as the rng seed
/// - `threads` - (Optional) Number of threads to use
///
/// # Returns
///
/// A NodeState mapping each node to its community id
pub fn label_propagation_fast<G>(
    g: &G,
    iter_count: usize,
    _seed: Option<[u8; 32]>,
    threads: Option<usize>,
) -> TypedNodeState<'static, LabelPropState, G>
where
    G: StaticGraphViewOps,
{
    let index = Index::for_graph(g.clone());
    let n = index.len();
    let mut prev: Vec<AtomicUsize> = (0..n).map(AtomicUsize::new).collect();
    let mut cur: Vec<AtomicUsize> = (0..n).map(|_| AtomicUsize::new(0)).collect();
    let nodes = g.nodes();

    let sweep = |prev: &[AtomicUsize], cur: &[AtomicUsize]| -> usize {
        let changed = AtomicUsize::new(0);
        nodes.par_iter().for_each_init(
            // labels are dense index positions, so a flat count array beats a map;
            // the touched list keeps the per-node reset proportional to degree
            || (vec![0u32; n], Vec::<usize>::new()),
            |(counts, touched), node| {
                let i = index.index(&node.node).unwrap();
                let own = prev[i].load(Ordering::Relaxed);
                counts[own] = 1;
                touched.push(own);
                let mut best_count = 1u32;
                let mut best_label = own;
                for nbor in node.neighbours().iter() {
                    let label = prev[index.index(&nbor.node).unwrap()].load(Ordering::Relaxed);
                    let count = counts[label] + 1;
                    counts[label] = count;
                    if count == 1 {
                        touched.push(label);
                    }
                    if count > best_count || (count == best_count && label > best_label) {
                        best_count = count;
                        best_label = label;
                    }
                }
                for &t in touched.iter() {
                    counts[t] = 0;
                }
                touched.clear();
                cur[i].store(best_label, Ordering::Relaxed);
                if best_label != own {
                    changed.fetch_add(1, Ordering::Relaxed);
                }
            },
        );
        changed.into_inner()
    };

    let run = |prev: &mut Vec<AtomicUsize>, cur: &mut Vec<AtomicUsize>| {
        for _ in 0..iter_count {
            if sweep(prev, cur) == 0 {
                break;
            }
            std::mem::swap(prev, cur);
        }
    };
    match threads {
        Some(t) => rayon::ThreadPoolBuilder::new()
            .num_threads(t)
            .build()
            .expect("failed to build thread pool")
            .install(|| run(&mut prev, &mut cur)),
        None => run(&mut prev, &mut cur),
    }

    let labels: Vec<usize> = prev.into_iter().map(AtomicUsize::into_inner).collect();
    TypedNodeState::new(GenericNodeState::new_from_eval_with_index_mapped(
        g.clone(),
        labels,
        index,
        |community_id| LabelPropState { community_id },
        None,
    ))
}
