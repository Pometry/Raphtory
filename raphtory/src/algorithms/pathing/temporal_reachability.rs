use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::{
        entities::vertices::input_vertex::InputVertex,
        state::{
            accumulator_id::accumulators::{hash_set, min, or},
            compute_state::ComputeStateVec,
        },
    },
    db::task::{
        context::Context,
        task::{ATask, Job, Step},
        task_runner::TaskRunner,
        vertex::eval_vertex::EvalVertexView,
    },
    prelude::*,
};
use itertools::Itertools;
use num_traits::Zero;
use std::{collections::HashMap, ops::Add};

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

/// Temporal Reachability starts from a set of seed nodes and propagates the taint to all nodes that are reachable
/// from the seed nodes within a given time window. The algorithm stops when all nodes that are reachable from the
/// seed nodes have been tainted or when the taint has propagated to all nodes in the graph.
///
/// Returns
///
/// * An AlgorithmResult object containing the mapping from vertex ID to a vector of tuples containing the time at which
/// the vertex was tainted and the ID of the vertex that tainted it
///
pub fn temporally_reachable_nodes<G: GraphViewOps, T: InputVertex>(
    g: &G,
    threads: Option<usize>,
    max_hops: usize,
    start_time: i64,
    seed_nodes: Vec<T>,
    stop_nodes: Option<Vec<T>>,
) -> AlgorithmResult<G, Vec<(i64, String)>, Vec<(i64, String)>> {
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

    let step1 = ATask::new(
        move |evv: &mut EvalVertexView<'_, G, ComputeStateVec, ()>| {
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
        },
    );

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
    let result: HashMap<usize, Vec<(i64, String)>> = runner.run(
        vec![Job::new(step1)],
        vec![Job::new(step2), step3],
        None,
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
    );

    let results_type = std::any::type_name::<Vec<(i64, String)>>();
    AlgorithmResult::new(g.clone(), "Temporal Reachability", results_type, result)
}

#[cfg(test)]
mod generic_taint_tests {
    use super::*;
    use crate::db::{api::mutation::AdditionOps, graph::graph::Graph};

    fn sort_inner_by_string(
        data: Vec<(String, Option<Vec<(i64, String)>>)>,
    ) -> Vec<(String, Option<Vec<(i64, String)>>)> {
        // Iterate over each tuple in the outer Vec
        let sorted_data = data
            .into_iter()
            .map(|(outer_str, opt_inner_vec)| {
                // Check if the Option is Some
                let sorted_opt_inner_vec = opt_inner_vec.map(|mut inner_vec| {
                    // Sort the inner Vec by the inner String
                    inner_vec.sort_by(|a, b| a.1.cmp(&b.1));
                    inner_vec
                });

                // Return the new tuple
                (outer_str, sorted_opt_inner_vec)
            })
            .collect::<Vec<_>>();

        // Return the new Vec
        sorted_data
    }

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    fn test_generic_taint<T: InputVertex>(
        graph: Graph,
        iter_count: usize,
        start_time: i64,
        infected_nodes: Vec<T>,
        stop_nodes: Option<Vec<T>>,
    ) -> Vec<(String, Option<Vec<(i64, String)>>)> {
        temporally_reachable_nodes(
            &graph,
            None,
            iter_count,
            start_time,
            infected_nodes,
            stop_nodes,
        )
        .sort_by_vertex(false)
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

        let results = sort_inner_by_string(test_generic_taint(graph, 20, 11, vec![2], None));
        let expected: Vec<(String, Option<Vec<(i64, String)>>)> = Vec::from([
            ("1".to_string(), Some(vec![])),
            ("2".to_string(), Some(vec![(11i64, "start".to_string())])),
            ("3".to_string(), Some(vec![])),
            (
                "4".to_string(),
                Some(vec![(12i64, "2".to_string()), (14i64, "5".to_string())]),
            ),
            (
                "5".to_string(),
                Some(vec![(13i64, "2".to_string()), (14i64, "5".to_string())]),
            ),
            ("6".to_string(), Some(vec![])),
            ("7".to_string(), Some(vec![(15i64, "4".to_string())])),
            ("8".to_string(), Some(vec![])),
        ]);
        assert_eq!(results, expected);
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

        let results = sort_inner_by_string(test_generic_taint(graph, 20, 11, vec![1, 2], None));
        let expected: Vec<(String, Option<Vec<(i64, String)>>)> = Vec::from([
            ("1".to_string(), Some(vec![(11i64, "start".to_string())])),
            (
                "2".to_string(),
                Some(vec![(11i64, "1".to_string()), (11i64, "start".to_string())]),
            ),
            ("3".to_string(), Some(vec![])),
            (
                "4".to_string(),
                Some(vec![(12i64, "2".to_string()), (14i64, "5".to_string())]),
            ),
            (
                "5".to_string(),
                Some(vec![(13i64, "2".to_string()), (14i64, "5".to_string())]),
            ),
            ("6".to_string(), Some(vec![])),
            ("7".to_string(), Some(vec![(15i64, "4".to_string())])),
            ("8".to_string(), Some(vec![])),
        ]);
        assert_eq!(results, expected);
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

        let results = sort_inner_by_string(test_generic_taint(
            graph,
            20,
            11,
            vec![1, 2],
            Some(vec![4, 5]),
        ));
        let expected: Vec<(String, Option<Vec<(i64, String)>>)> = Vec::from([
            ("1".to_string(), Some(vec![(11i64, "start".to_string())])),
            (
                "2".to_string(),
                Some(vec![(11i64, "1".to_string()), (11i64, "start".to_string())]),
            ),
            ("3".to_string(), Some(vec![])),
            ("4".to_string(), Some(vec![(12i64, "2".to_string())])),
            ("5".to_string(), Some(vec![(13i64, "2".to_string())])),
            ("6".to_string(), Some(vec![])),
            ("7".to_string(), Some(vec![])),
            ("8".to_string(), Some(vec![])),
        ]);
        assert_eq!(results, expected);
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

        let results = sort_inner_by_string(test_generic_taint(
            graph,
            20,
            11,
            vec![1, 2],
            Some(vec![4, 5]),
        ));
        let expected: Vec<(String, Option<Vec<(i64, String)>>)> = Vec::from([
            ("1".to_string(), Some(vec![(11i64, "start".to_string())])),
            (
                "2".to_string(),
                Some(vec![
                    (12i64, "1".to_string()),
                    (11i64, "1".to_string()),
                    (11i64, "start".to_string()),
                ]),
            ),
            ("3".to_string(), Some(vec![])),
            ("4".to_string(), Some(vec![(12i64, "2".to_string())])),
            ("5".to_string(), Some(vec![(13i64, "2".to_string())])),
            ("6".to_string(), Some(vec![])),
            ("7".to_string(), Some(vec![])),
            ("8".to_string(), Some(vec![])),
        ]);
        assert_eq!(results, expected);
    }
}
