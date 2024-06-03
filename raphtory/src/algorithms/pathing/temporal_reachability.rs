use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::{
        entities::nodes::input_node::InputNode,
        state::{
            accumulator_id::accumulators::{hash_set, min, or},
            compute_state::ComputeStateVec,
        },
    },
    db::{
        api::view::StaticGraphViewOps,
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
use std::{collections::HashMap, ops::Add};

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
pub fn temporally_reachable_nodes<G: StaticGraphViewOps, T: InputNode>(
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
        .unwrap_or_default()
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

    let tainted_nodes = hash_set::<u64>(4);
    ctx.global_agg(tainted_nodes);

    let step1 = ATask::new(move |evv: &mut EvalNodeView<G, ()>| {
        if infected_nodes.contains(&evv.id()) {
            evv.global_update(&tainted_nodes, evv.id());
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
                eev.history().into_iter().for_each(|t| {
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

        // println!("v = {}, msgs = {:?}, taint_history = {:?}", evv.global_id(), msgs, evv.read(&taint_history));

        if !msgs.is_empty() {
            evv.global_update(&tainted_nodes, evv.id());

            if !evv.read(&taint_status) {
                evv.update(&taint_status, true);
            }
            msgs.iter().for_each(|msg| {
                evv.update(&taint_history, msg.clone());
            });

            // println!("v = {}, taint_history = {:?}", evv.global_id(), evv.read(&taint_history));

            if stop_nodes.is_empty() || !stop_nodes.contains(&evv.id()) {
                let earliest = evv.read(&earliest_taint_time);
                for eev in evv.window(earliest, i64::MAX).out_edges() {
                    let dst = eev.dst();
                    for t in eev.history() {
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
                taint_history
                    .into_iter()
                    .map(|tmsg| (tmsg.event_time, tmsg.src_node))
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
    use tempfile::TempDir;

    fn sort_inner_by_string(
        data: HashMap<String, Vec<(i64, String)>>,
    ) -> Vec<(String, Vec<(i64, String)>)> {
        let mut vec: Vec<_> = data.into_iter().collect();
        vec.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, inner_vec) in &mut vec {
            inner_vec.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
        }
        vec
    }

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    fn test_generic_taint<T: InputNode, G: StaticGraphViewOps>(
        graph: &G,
        iter_count: usize,
        start_time: i64,
        infected_nodes: Vec<T>,
        stop_nodes: Option<Vec<T>>,
    ) -> HashMap<String, Vec<(i64, String)>> {
        temporally_reachable_nodes(
            graph,
            None,
            iter_count,
            start_time,
            infected_nodes,
            stop_nodes,
        )
        .get_all_with_names()
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

        let test_dir = TempDir::new().unwrap();
        #[cfg(feature = "storage")]
        let disk_graph = graph.persist_as_disk_graph(test_dir.path()).unwrap();

        fn test<G: StaticGraphViewOps>(graph: &G) {
            let results = sort_inner_by_string(test_generic_taint(graph, 20, 11, vec![2], None));
            let expected: Vec<(String, Vec<(i64, String)>)> = Vec::from([
                ("1".to_string(), vec![]),
                ("2".to_string(), vec![(11i64, "start".to_string())]),
                ("3".to_string(), vec![]),
                (
                    "4".to_string(),
                    vec![(12i64, "2".to_string()), (14i64, "5".to_string())],
                ),
                (
                    "5".to_string(),
                    vec![(13i64, "2".to_string()), (14i64, "5".to_string())],
                ),
                ("6".to_string(), vec![]),
                ("7".to_string(), vec![(15i64, "4".to_string())]),
                ("8".to_string(), vec![]),
            ]);
            assert_eq!(results, expected);
        }
        test(&graph);
        #[cfg(feature = "storage")]
        test(&disk_graph);
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

        let test_dir = TempDir::new().unwrap();
        #[cfg(feature = "storage")]
        let disk_graph = graph.persist_as_disk_graph(test_dir.path()).unwrap();

        fn test<G: StaticGraphViewOps>(graph: &G) {
            let results = sort_inner_by_string(test_generic_taint(graph, 20, 11, vec![1, 2], None));
            let expected: Vec<(String, Vec<(i64, String)>)> = Vec::from([
                ("1".to_string(), vec![(11i64, "start".to_string())]),
                (
                    "2".to_string(),
                    vec![(11i64, "start".to_string()), (11i64, "1".to_string())],
                ),
                ("3".to_string(), vec![]),
                (
                    "4".to_string(),
                    vec![(12i64, "2".to_string()), (14i64, "5".to_string())],
                ),
                (
                    "5".to_string(),
                    vec![(13i64, "2".to_string()), (14i64, "5".to_string())],
                ),
                ("6".to_string(), vec![]),
                ("7".to_string(), vec![(15i64, "4".to_string())]),
                ("8".to_string(), vec![]),
            ]);
            assert_eq!(results, expected);
        }
        test(&graph);
        #[cfg(feature = "storage")]
        test(&disk_graph);
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

        let test_dir = TempDir::new().unwrap();
        #[cfg(feature = "storage")]
        let disk_graph = graph.persist_as_disk_graph(test_dir.path()).unwrap();

        fn test<G: StaticGraphViewOps>(graph: &G) {
            let results = sort_inner_by_string(test_generic_taint(
                graph,
                20,
                11,
                vec![1, 2],
                Some(vec![4, 5]),
            ));
            let expected: Vec<(String, Vec<(i64, String)>)> = Vec::from([
                ("1".to_string(), vec![(11i64, "start".to_string())]),
                (
                    "2".to_string(),
                    vec![(11i64, "start".to_string()), (11i64, "1".to_string())],
                ),
                ("3".to_string(), vec![]),
                ("4".to_string(), vec![(12i64, "2".to_string())]),
                ("5".to_string(), vec![(13i64, "2".to_string())]),
                ("6".to_string(), vec![]),
                ("7".to_string(), vec![]),
                ("8".to_string(), vec![]),
            ]);
            assert_eq!(results, expected);
        }
        test(&graph);
        #[cfg(feature = "storage")]
        test(&disk_graph);
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

        let test_dir = TempDir::new().unwrap();
        #[cfg(feature = "storage")]
        let disk_graph = graph.persist_as_disk_graph(test_dir.path()).unwrap();

        fn test<G: StaticGraphViewOps>(graph: &G) {
            let results = sort_inner_by_string(test_generic_taint(
                graph,
                20,
                11,
                vec![1, 2],
                Some(vec![4, 5]),
            ));
            let expected: Vec<(String, Vec<(i64, String)>)> = Vec::from([
                ("1".to_string(), vec![(11i64, "start".to_string())]),
                (
                    "2".to_string(),
                    vec![
                        (11i64, "start".to_string()),
                        (11i64, "1".to_string()),
                        (12i64, "1".to_string()),
                    ],
                ),
                ("3".to_string(), vec![]),
                ("4".to_string(), vec![(12i64, "2".to_string())]),
                ("5".to_string(), vec![(13i64, "2".to_string())]),
                ("6".to_string(), vec![]),
                ("7".to_string(), vec![]),
                ("8".to_string(), vec![]),
            ]);
            assert_eq!(results, expected);
        }
        test(&graph);
        #[cfg(feature = "storage")]
        test(&disk_graph);
    }
}
