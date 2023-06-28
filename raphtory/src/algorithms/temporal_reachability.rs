use crate::core::state::accumulator_id::accumulators::{hash_set, min, or};
use crate::core::state::compute_state::ComputeStateVec;
use crate::core::vertex::InputVertex;
use crate::db::task::context::Context;
use crate::db::task::task::{ATask, Job, Step};
use crate::db::task::task_runner::TaskRunner;
use crate::db::view_api::*;
use itertools::Itertools;
use num_traits::Zero;
use std::collections::HashMap;
use std::ops::Add;

#[derive(Eq, Hash, PartialEq, Clone, Debug, Default)]
pub struct TaintMessage {
    pub event_time: i64,
    pub src_vertex: String,
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
            src_vertex: "".to_string(),
        }
    }

    fn set_zero(&mut self) {
        *self = Zero::zero();
    }

    fn is_zero(&self) -> bool {
        *self
            == TaintMessage {
                event_time: -1,
                src_vertex: "".to_string(),
            }
    }
}

pub fn temporally_reachable_nodes<G: GraphViewOps, T: InputVertex>(
    g: &G,
    threads: Option<usize>,
    max_hops: usize,
    start_time: i64,
    seed_nodes: Vec<T>,
    stop_nodes: Option<Vec<T>>,
) -> HashMap<String, Vec<(i64, String)>> {
    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let infected_nodes = seed_nodes.into_iter().map(|n| n.id()).collect_vec();
    let stop_nodes = stop_nodes
        .unwrap_or(vec![])
        .into_iter()
        .map(|n| n.id())
        .collect_vec();

    let taint_status = or(0);
    ctx.global_agg(taint_status);

    let taint_history = hash_set::<TaintMessage>(1);
    ctx.agg(taint_history);

    let recv_tainted_msgs = hash_set::<TaintMessage>(2);
    ctx.agg(recv_tainted_msgs);

    let earliest_taint_time = min::<i64>(3);
    ctx.agg(earliest_taint_time);

    let tainted_vertices = hash_set::<u64>(4);
    ctx.global_agg(tainted_vertices);

    let step1 = ATask::new(move |evv| {
        if infected_nodes.contains(&evv.id()) {
            evv.global_update(&tainted_vertices, evv.id());
            evv.update(&taint_status, true);
            evv.update(&earliest_taint_time, start_time);
            evv.update(
                &taint_history,
                TaintMessage {
                    event_time: start_time,
                    src_vertex: "start".to_string(),
                },
            );
            evv.window(start_time, i64::MAX)
                .out_edges()
                .for_each(|eev| {
                    let dst = eev.dst();
                    eev.history().into_iter().for_each(|t| {
                        dst.update(&earliest_taint_time, t);
                        dst.update(
                            &recv_tainted_msgs,
                            TaintMessage {
                                event_time: t,
                                src_vertex: evv.name(),
                            },
                        )
                    });
                });
        }
        Step::Continue
    });

    let step2 = ATask::new(move |evv| {
        let msgs = evv.read(&recv_tainted_msgs);

        // println!("v = {}, msgs = {:?}, taint_history = {:?}", evv.global_id(), msgs, evv.read(&taint_history));

        if !msgs.is_empty() {
            evv.global_update(&tainted_vertices, evv.id());

            if !evv.read(&taint_status) {
                evv.update(&taint_status, true);
            }
            msgs.iter().for_each(|msg| {
                evv.update(&taint_history, msg.clone());
            });

            // println!("v = {}, taint_history = {:?}", evv.global_id(), evv.read(&taint_history));

            if stop_nodes.is_empty() || !stop_nodes.contains(&evv.id()) {
                let earliest = evv.read(&earliest_taint_time);
                evv.window(earliest, i64::MAX).out_edges().for_each(|eev| {
                    let dst = eev.dst();
                    eev.history().into_iter().for_each(|t| {
                        dst.update(&earliest_taint_time, t);
                        dst.update(
                            &recv_tainted_msgs,
                            TaintMessage {
                                event_time: t,
                                src_vertex: evv.name(),
                            },
                        )
                    });
                });
            }
        }
        Step::Continue
    });

    let step3 = Job::Check(Box::new(move |state| {
        let prev_tainted_vs = state.read_prev(&tainted_vertices);
        let curr_tainted_vs = state.read(&tainted_vertices);
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

    runner.run(
        vec![Job::new(step1)],
        vec![Job::new(step2), step3],
        (),
        |_, ess, _, _| {
            ess.finalize(&taint_history, |taint_history| {
                taint_history
                    .into_iter()
                    .map(|tmsg| (tmsg.event_time, tmsg.src_vertex))
                    .collect_vec()
            })
        },
        threads,
        max_hops,
        None,
        None,
    )
}

#[cfg(test)]
mod generic_taint_tests {
    use super::*;
    use crate::db::graph::Graph;
    use crate::db::mutation_api::AdditionOps;

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, [], None).unwrap();
        }
        graph
    }

    fn test_generic_taint<T: InputVertex>(
        graph: Graph,
        iter_count: usize,
        start_time: i64,
        infected_nodes: Vec<T>,
        stop_nodes: Option<Vec<T>>,
    ) -> Vec<(String, Vec<(i64, String)>)> {
        let mut results: Vec<(String, Vec<(i64, String)>)> = temporally_reachable_nodes(
            &graph,
            None,
            iter_count,
            start_time,
            infected_nodes,
            stop_nodes,
        )
        .into_iter()
        .map(|(k, mut v)| {
            v.sort();
            (k, v)
        })
        .collect_vec();

        results.sort();
        results
    }

    #[test]
    fn test_generic_taint_1() {
        let graph = load_graph(vec![
            (10, 1, 3),
            (11, 1, 2),
            (12, 2, 4),
            (13, 2, 5),
            (14, 5, 5),
            (14, 5, 4),
            (5, 4, 6),
            (15, 4, 7),
            (10, 4, 7),
            (10, 5, 8),
        ]);

        let results = test_generic_taint(graph, 20, 11, vec![2], None);

        assert_eq!(
            results,
            Vec::from([
                ("1".to_string(), vec![]),
                ("2".to_string(), vec![(11, "start".to_string())]),
                ("3".to_string(), vec![]),
                (
                    "4".to_string(),
                    vec![(12, "2".to_string()), (14, "5".to_string())]
                ),
                (
                    "5".to_string(),
                    vec![(13, "2".to_string()), (14, "5".to_string())]
                ),
                ("6".to_string(), vec![]),
                ("7".to_string(), vec![(15, "4".to_string())]),
            ])
        );
    }

    #[test]
    fn test_generic_taint_1_multiple_start() {
        let graph = load_graph(vec![
            (10, 1, 3),
            (11, 1, 2),
            (12, 2, 4),
            (13, 2, 5),
            (14, 5, 5),
            (14, 5, 4),
            (5, 4, 6),
            (15, 4, 7),
            (10, 4, 7),
            (10, 5, 8),
        ]);

        let results = test_generic_taint(graph, 20, 11, vec![1, 2], None);

        assert_eq!(
            results,
            Vec::from([
                ("1".to_string(), vec![(11, "start".to_string())]),
                (
                    "2".to_string(),
                    vec![(11, "1".to_string()), (11, "start".to_string())]
                ),
                ("3".to_string(), vec![]),
                (
                    "4".to_string(),
                    vec![(12, "2".to_string()), (14, "5".to_string())]
                ),
                (
                    "5".to_string(),
                    vec![(13, "2".to_string()), (14, "5".to_string())]
                ),
                ("6".to_string(), vec![]),
                ("7".to_string(), vec![(15, "4".to_string())]),
            ])
        );
    }

    #[test]
    fn test_generic_taint_1_stop_nodes() {
        let graph = load_graph(vec![
            (10, 1, 3),
            (11, 1, 2),
            (12, 2, 4),
            (13, 2, 5),
            (14, 5, 5),
            (14, 5, 4),
            (5, 4, 6),
            (15, 4, 7),
            (10, 4, 7),
            (10, 5, 8),
        ]);

        let results = test_generic_taint(graph, 20, 11, vec![1, 2], Some(vec![4, 5]));

        assert_eq!(
            results,
            Vec::from([
                ("1".to_string(), vec![(11, "start".to_string())]),
                (
                    "2".to_string(),
                    vec![(11, "1".to_string()), (11, "start".to_string())]
                ),
                ("3".to_string(), vec![]),
                ("4".to_string(), vec![(12, "2".to_string())]),
                ("5".to_string(), vec![(13, "2".to_string())]),
            ])
        );
    }

    #[test]
    fn test_generic_taint_1_multiple_history_points() {
        let graph = load_graph(vec![
            (10, 1, 3),
            (11, 1, 2),
            (12, 1, 2),
            (9, 1, 2),
            (12, 2, 4),
            (13, 2, 5),
            (14, 5, 5),
            (14, 5, 4),
            (5, 4, 6),
            (15, 4, 7),
            (10, 4, 7),
            (10, 5, 8),
        ]);

        let results = test_generic_taint(graph, 20, 11, vec![1, 2], Some(vec![4, 5]));

        assert_eq!(
            results,
            Vec::from([
                ("1".to_string(), vec![(11, "start".to_string())]),
                (
                    "2".to_string(),
                    vec![
                        (11, "1".to_string()),
                        (11, "start".to_string()),
                        (12, "1".to_string())
                    ]
                ),
                ("3".to_string(), vec![]),
                ("4".to_string(), vec![(12, "2".to_string())]),
                ("5".to_string(), vec![(13, "2".to_string())]),
            ])
        );
    }
}
