use std::sync::atomic::{AtomicBool, Ordering};
use crate::{
    core::state::{
        accumulator_id::accumulators::{max, sum},
        compute_state::ComputeStateVec,
    },
    db::{
        api::{
            state::{GenericNodeState, TypedNodeState},
            view::{GraphViewOps, NodeViewOps, StaticGraphViewOps},
        },
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
};
use atomic_float::AtomicF32;
use indexmap::IndexSet;
use num_traits::{Pow, abs};
use raphtory_core::entities::VID;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};

#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Copy)]
pub struct Hits {
    pub hub_score: f32,
    pub auth_score: f32,
    pub recv_hub_score: f32,
    pub recv_auth_score: f32,
}

impl Default for Hits {
    fn default() -> Self {
        Self {
            hub_score: 1f32,
            auth_score: 1f32,
            recv_hub_score: 1f32,
            recv_auth_score: 1f32,
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

    let total_hub_score = sum::<f32>(1);
    let total_auth_score = sum::<f32>(2);

    let max_diff_score = max::<f32>(3);

    ctx.global_agg(total_hub_score);
    ctx.global_agg(total_auth_score);
    ctx.global_agg(max_diff_score);
    ctx.global_agg_reset(total_hub_score);
    ctx.global_agg_reset(total_auth_score);
    ctx.global_agg_reset(max_diff_score);

    let max_diff = 0.01f32;

    let step2 = ATask::new(move |evv: &mut EvalNodeView<_, Hits>| {
        let recv_hub_score = evv.out_neighbours().iter().map(|n| n.prev().auth_score).sum();
        let recv_auth_score = evv.in_neighbours().iter().map(|n| n.prev().hub_score).sum();
        let s_state = evv.get_mut();
        s_state.recv_hub_score = recv_hub_score;
        s_state.recv_auth_score = recv_auth_score;
        evv.global_update(&total_hub_score, recv_hub_score); // * recv_hub_score
        evv.global_update(&total_auth_score, recv_auth_score); // * recv_auth_score
        Step::Continue
    });

    let step4 = ATask::new(move |evv: &mut EvalNodeView<_, Hits>| {
        let hits = *evv.get();

        let hub_score =
            hits.recv_hub_score / evv.read_global_state(&total_hub_score).unwrap();
        let auth_score =
            hits.recv_auth_score / evv.read_global_state(&total_auth_score).unwrap();

        let md_score = abs(evv.prev().auth_score - auth_score).max(abs(evv.prev().hub_score - hub_score));
        if md_score > max_diff {
            evv.global_update(&max_diff_score, md_score);
        }
        let s_state = evv.get_mut();
        s_state.auth_score = auth_score;
        s_state.hub_score = hub_score;

        Step::Continue
    });

    let step5 = Job::Check(Box::new(move |state| {
        if state.read(&max_diff_score) <= max_diff
        {
            Step::Done
        } else {
            Step::Continue
        }
    }));

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![],
        vec![Job::new(step2), Job::new(step4), step5],
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

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct OldHits {
    pub hub_score: f32,
    pub auth_score: f32,
}

impl Default for OldHits {
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
pub fn old_hits<G: StaticGraphViewOps>(
    g: &G,
    iter_count: usize,
    threads: Option<usize>,
) -> TypedNodeState<'static, OldHits, G> {
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

    let step2 = ATask::new(move |evv: &mut EvalNodeView<_, OldHits>| {
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

    let step3 = ATask::new(move |evv: &mut EvalNodeView<_, OldHits>| {
        let recv_hub_score = evv.read(&recv_hub_score);
        let recv_auth_score = evv.read(&recv_auth_score);

        evv.global_update(&total_hub_score, recv_hub_score);
        evv.global_update(&total_auth_score, recv_auth_score);
        Step::Continue
    });

    let step4 = ATask::new(move |evv: &mut EvalNodeView<_, OldHits>| {
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

#[repr(C)]
pub struct AtomicHits {
    pub hub_score: AtomicF32,
    pub auth_score: AtomicF32,
    pub recv_hub_score: AtomicF32,
    pub recv_auth_score: AtomicF32,
}

pub fn atomic_hits_from_mut_slice(v: &mut [Hits]) -> &mut [AtomicHits] {
    use std::mem::align_of;
    let [] = [(); align_of::<AtomicHits>() - align_of::<Hits>()];
    unsafe { &mut *(v as *mut [Hits] as *mut [AtomicHits]) }
}

// receive filtered graph
// get high degree vs low degree nodes
// build idx map
// build atomic buffers
// two paradigms for parallelization
    // src node 

pub fn new_hits<G: StaticGraphViewOps>(    
    g: &G,
    iter_count: usize) -> 
TypedNodeState<'static, OldHits, G> {
    let idx_map: IndexSet<VID> = IndexSet::from_iter(g.nodes().into_iter().map(|n| n.node));
    let mut hits_vec: Vec<Hits> = vec![Hits::default(); idx_map.len()];
    let hits: &mut [AtomicHits] = atomic_hits_from_mut_slice(&mut hits_vec[..]);
    let needs_more = AtomicBool::new(true);
    //let recv_hub_total = AtomicF32::new(0f32);
    //let recv_auth_total = AtomicF32::new(0f32);
    let max_diff = 0.01f32;
    let mut iter_count = iter_count;

    while iter_count > 0 && needs_more.load(Ordering::Relaxed) {
        needs_more.store(false, Ordering::Relaxed);
        g.nodes().par_iter().for_each(|v| {
            // collect recv scores
            let mut recv_sum: f32 = 0f32;
            let vid = idx_map.get_index_of(&v.node).unwrap();
            recv_sum = v.in_neighbours().into_iter().fold(0f32, |acc, n| acc + hits[idx_map.get_index_of(&n.node).unwrap()].hub_score.load(Ordering::Relaxed));
            hits[vid].recv_hub_score.store(recv_sum, Ordering::Relaxed);
            recv_sum = v.out_neighbours().into_iter().fold(0f32, |acc, n| acc + hits[idx_map.get_index_of(&n.node).unwrap()].auth_score.load(Ordering::Relaxed)); 
            hits[vid].recv_auth_score.store(recv_sum, Ordering::Relaxed);
        });

        let (recv_hub_total, recv_auth_total) = hits
        .par_iter()
        .fold(
            || (0f32, 0f32),
            |(hub_acc, auth_acc), h| (
                hub_acc + h.recv_hub_score.load(Ordering::Relaxed),
                auth_acc + h.recv_auth_score.load(Ordering::Relaxed),
            ),
        )
        .reduce(|| (0f32, 0f32), |(h1, a1), (h2, a2)| (h1 + h2, a1 + a2));
    
        hits.par_iter().for_each(|(hit)| {
            // normalize recv sums and calculate hub and auth scores 

            let new_hub  = hit.recv_auth_score.load(Ordering::Relaxed) / recv_auth_total;
            let new_auth = hit.recv_hub_score.load(Ordering::Relaxed) / recv_hub_total;
            
            if (hit.hub_score.load(Ordering::Relaxed) - new_hub).abs() > max_diff || (hit.auth_score.load(Ordering::Relaxed) - new_auth).abs() > max_diff {
                needs_more.store(true, Ordering::Relaxed);
            }

            hit.hub_score.store(new_hub, Ordering::Relaxed);
            hit.auth_score.store(new_auth, Ordering::Relaxed);
        });
        iter_count -= 1;
    }

    // construct vec of guys
    let result: Vec<OldHits> = hits_vec.par_iter().map(|h| OldHits {
        hub_score: h.hub_score,
        auth_score: h.auth_score,
    }).collect();

    TypedNodeState::new(GenericNodeState::new_from_eval_with_index(g.clone(), result, g.nodes().nodes, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{graphgen::random_attachment::random_attachment, prelude::Graph};
    use rayon::iter::ParallelIterator;
    use std::{sync::atomic::AtomicBool, time::Instant};

    #[test]
    #[ignore]
    fn profile_new_hits_phases() {
        let graph = Graph::new();
        let seed: [u8; 32] = [1; 32];
        random_attachment(&graph, 1_000_000, 4, Some(seed));

        hits(&graph, 20, None);
    }
}