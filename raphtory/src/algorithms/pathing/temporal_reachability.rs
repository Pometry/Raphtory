use crate::{
    core::{
        entities::nodes::node_ref::AsNodeRef,
        state::{
            accumulator_id::accumulators::{hash_set, min, or},
            compute_state::ComputeStateVec,
        },
    },
    db::{
        api::{
            state::{GenericNodeState, TypedNodeState},
            view::StaticGraphViewOps,
        },
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::*,
};
use itertools::Itertools;
use num_traits::Zero;
use raphtory_api::core::entities::VID;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Add};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct ReachabilityState {
    pub reachable_nodes: Vec<(i64, String)>,
}

#[derive(Eq, Hash, PartialEq, Clone, Debug, Default)]
pub struct TaintMessage {
    pub event_time: i64,
    pub src_node: String,
}

impl Add for TaintMessage {
    type Output = TaintMessage;

    fn add(self, rhs: Self) -> Self::Output {
        rhs
    }
}

impl Zero for TaintMessage {
    fn zero() -> Self {
        TaintMessage {
            event_time: -1,
            src_node: "".to_string(),
        }
    }

    fn set_zero(&mut self) {
        *self = Zero::zero();
    }

    fn is_zero(&self) -> bool {
        *self
            == TaintMessage {
                event_time: -1,
                src_node: "".to_string(),
            }
    }
}

/// Temporal Reachability starts from a set of seed nodes and propagates the taint to all nodes that are reachable
/// from the seed nodes within a given time window. The algorithm stops when all nodes that are reachable from the
/// seed nodes have been tainted or when the taint has propagated to all nodes in the graph.
///
/// Returns
///
/// * An AlgorithmResult object containing the mapping from node ID to a vector of tuples containing the time at which
/// the node was tainted and the ID of the node that tainted it
///
pub fn temporally_reachable_nodes<G: StaticGraphViewOps, T: AsNodeRef>(
    g: &G,
    threads: Option<usize>,
    max_hops: usize,
    start_time: i64,
    seed_nodes: Vec<T>,
    stop_nodes: Option<Vec<T>>,
) -> TypedNodeState<'static, ReachabilityState, G> {
    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let infected_nodes = seed_nodes
        .into_iter()
        .filter_map(|n| g.node(n))
        .map(|n| n.node)
        .collect_vec();
    let stop_nodes = stop_nodes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|n| g.node(n))
        .map(|n| n.node)
        .collect_vec();

    let taint_status = or(0);
    ctx.global_agg(taint_status);

    let taint_history = hash_set::<TaintMessage>(1);
    ctx.agg(taint_history);

    let recv_tainted_msgs = hash_set::<TaintMessage>(2);
    ctx.agg(recv_tainted_msgs);

    let earliest_taint_time = min::<i64>(3);
    ctx.agg(earliest_taint_time);

    let tainted_nodes = hash_set::<VID>(4);
    ctx.global_agg(tainted_nodes);

    let step1 = ATask::new(move |evv: &mut EvalNodeView<_, ()>| {
        if infected_nodes.contains(&evv.node) {
            evv.global_update(&tainted_nodes, evv.node);
            evv.update(&taint_status, true);
            evv.update(&earliest_taint_time, start_time);
            evv.update(
                &taint_history,
                TaintMessage {
                    event_time: start_time,
                    src_node: "start".to_string(),
                },
            );
            for eev in evv.window(start_time, i64::MAX).out_edges() {
                let dst = eev.dst();
                eev.history().t().collect().into_iter().for_each(|t| {
                    dst.update(&earliest_taint_time, t);
                    dst.update(
                        &recv_tainted_msgs,
                        TaintMessage {
                            event_time: t,
                            src_node: evv.name(),
                        },
                    )
                });
            }
        }
        Step::Continue
    });

    let step2 = ATask::new(move |evv| {
        let msgs = evv.read(&recv_tainted_msgs);

        if !msgs.is_empty() {
            evv.global_update(&tainted_nodes, evv.node);

            if !evv.read(&taint_status) {
                evv.update(&taint_status, true);
            }
            msgs.iter().for_each(|msg| {
                evv.update(&taint_history, msg.clone());
            });

            if stop_nodes.is_empty() || !stop_nodes.contains(&evv.node) {
                let earliest = evv.read(&earliest_taint_time);
                for eev in evv.window(earliest, i64::MAX).out_edges() {
                    let dst = eev.dst();
                    for t in eev.history().t().collect() {
                        dst.update(&earliest_taint_time, t);
                        dst.update(
                            &recv_tainted_msgs,
                            TaintMessage {
                                event_time: t,
                                src_node: evv.name(),
                            },
                        )
                    }
                }
            }
        }
        Step::Continue
    });

    let step3 = Job::Check(Box::new(move |state| {
        let prev_tainted_vs = state.read_prev(&tainted_nodes);
        let curr_tainted_vs = state.read(&tainted_nodes);
        let difference: Vec<_> = curr_tainted_vs
            .iter()
            .filter(|item| !prev_tainted_vs.contains(*item))
            .collect();
        if difference.is_empty() {
            Step::Done
        } else {
            Step::Continue
        }
    }));

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    let result: HashMap<usize, Vec<(i64, String)>> = runner.run(
        vec![Job::new(step1)],
        vec![Job::new(step2), step3],
        None,
        |_, ess, _, _| {
            ess.finalize(&taint_history, |taint_history| {
                let mut hist = taint_history
                    .into_iter()
                    .map(|tmsg| (tmsg.event_time, tmsg.src_node))
                    .collect_vec();
                hist.sort();
                hist
            })
        },
        threads,
        max_hops,
        None,
        None,
    );
    let result: FxHashMap<_, _> = result.into_iter().map(|(k, v)| (VID(k), v)).collect();
    TypedNodeState::new(GenericNodeState::new_from_map(
        g.clone(),
        result,
        |v| ReachabilityState { reachable_nodes: v },
        None,
    ))
}
