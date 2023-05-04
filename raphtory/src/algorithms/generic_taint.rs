use crate::algorithms::*;
use crate::core::agg::set::Set;
use crate::core::agg::*;
use crate::core::state::accumulator_id::def::{hash_set, val};
use crate::core::state::accumulator_id::AccId;
use crate::core::vertex::InputVertex;
use crate::db::program::*;
use crate::db::view_api::{GraphViewOps, VertexViewOps};
use itertools::Itertools;
use rustc_hash::FxHashSet;
use std::collections::{HashMap, HashSet};

#[derive(Eq, Hash, PartialEq, Clone, Debug, Default)]
pub struct TaintMessage {
    pub edge_id: usize,
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
            edge_id: 0,
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
                edge_id: 0,
                event_time: -1,
                src_vertex: "".to_string(),
            }
    }
}

struct GenericTaintS0 {
    start_time: i64,
    infected_nodes: Vec<u64>,
    taint_status: AccId<Bool, Bool, Bool, ValDef<Bool>>,
    taint_history:
        AccId<FxHashSet<TaintMessage>, TaintMessage, FxHashSet<TaintMessage>, Set<TaintMessage>>,
    recv_tainted_msgs:
        AccId<FxHashSet<TaintMessage>, TaintMessage, FxHashSet<TaintMessage>, Set<TaintMessage>>,
}

impl GenericTaintS0 {
    fn new(start_time: i64, infected_nodes: Vec<u64>) -> Self {
        Self {
            start_time,
            infected_nodes,
            taint_status: val(0),
            taint_history: hash_set(1),
            recv_tainted_msgs: hash_set(2),
        }
    }
}

impl Program for GenericTaintS0 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let taint_status = c.agg(self.taint_status);
        let taint_history = c.agg(self.taint_history.clone());
        let recv_tainted_msgs = c.agg(self.recv_tainted_msgs.clone());

        c.step(|evv| {
            if self.infected_nodes.contains(&evv.global_id()) {
                evv.update(&taint_status, Bool(true));
                evv.update(
                    &taint_history,
                    TaintMessage {
                        edge_id: 0,
                        event_time: self.start_time,
                        src_vertex: evv.name(),
                    },
                );
                evv.out_edges(self.start_time).for_each(|eev| {
                    let dst = eev.dst();
                    eev.history().into_iter().for_each(|t| {
                        dst.update(
                            &recv_tainted_msgs,
                            TaintMessage {
                                edge_id: eev.id(),
                                event_time: t,
                                src_vertex: evv.name(),
                            },
                        )
                    });
                });
            }
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg(self.taint_history.clone());
        let _ = c.agg(self.recv_tainted_msgs.clone());
        c.step(|_| true)
    }

    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct GenericTaintS1 {
    stop_nodes: Vec<u64>,
    taint_status: AccId<Bool, Bool, Bool, ValDef<Bool>>,
    taint_history:
        AccId<FxHashSet<TaintMessage>, TaintMessage, FxHashSet<TaintMessage>, Set<TaintMessage>>,
    recv_tainted_msgs:
        AccId<FxHashSet<TaintMessage>, TaintMessage, FxHashSet<TaintMessage>, Set<TaintMessage>>,
}

impl GenericTaintS1 {
    fn new(stop_nodes: Vec<u64>) -> Self {
        Self {
            stop_nodes,
            taint_status: val(0),
            taint_history: hash_set(1),
            recv_tainted_msgs: hash_set(2),
        }
    }
}

impl Program for GenericTaintS1 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let taint_status = c.agg(self.taint_status);
        let taint_history = c.agg(self.taint_history.clone());
        let recv_tainted_msgs = c.agg(self.recv_tainted_msgs.clone());

        // Check if vertices have received tainted message
        // If yes, if the vertex was not already tainted, update the accumulators else, update tainted history
        // Spread the taint messages if no stop_nodes provided

        c.step(|evv| {
            let msgs = evv.read(&recv_tainted_msgs);
            if !msgs.is_empty() {
                if !evv.read(&taint_status).0 {
                    evv.update(&taint_status, Bool(true));
                }
                msgs.iter().for_each(|msg| {
                    evv.update(&taint_history, msg.clone());
                });

                let earliest_taint_time = msgs.into_iter().map(|msg| msg.event_time).min().unwrap();

                if self.stop_nodes.is_empty() || !self.stop_nodes.contains(&evv.global_id()) {
                    evv.out_edges(earliest_taint_time).for_each(|eev| {
                        let dst = eev.dst();
                        eev.history().into_iter().for_each(|t| {
                            dst.update(
                                &recv_tainted_msgs,
                                TaintMessage {
                                    edge_id: eev.id(),
                                    event_time: t,
                                    src_vertex: evv.name(),
                                },
                            )
                        });
                    });
                }
            }
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg(self.taint_history.clone());
        let _ = c.agg(self.recv_tainted_msgs.clone());
        c.step(|_| true)
    }

    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

pub fn generic_taint<G: GraphViewOps, T: InputVertex>(
    g: &G,
    iter_count: usize,
    start_time: i64,
    infected_nodes: Vec<T>,
    stop_nodes: Vec<T>,
) -> HashMap<String, Vec<(usize, i64, String)>> {
    let mut c = GlobalEvalState::new(g.clone(), true);
    let gtaint_s0 = GenericTaintS0::new(
        start_time,
        infected_nodes.into_iter().map(|n| n.id()).collect_vec(),
    );
    let gtaint_s1 = GenericTaintS1::new(stop_nodes.into_iter().map(|n| n.id()).collect_vec());

    gtaint_s0.run_step(g, &mut c);

    // println!(
    //     "step0, taint_status = {:?}",
    //     c.read_vec_partitions(&val::<Bool>(0))
    // );
    // println!(
    //     "step0, taint_history = {:?}",
    //     c.read_vec_partitions(&hash_set::<TaintMessage>(1))
    // );
    // println!(
    //     "step0, recv_tainted_msgs = {:?}",
    //     c.read_vec_partitions(&hash_set::<TaintMessage>(2))
    // );
    // println!();

    let mut last_taint_list = HashSet::<u64>::new();
    let mut i = 0;
    loop {
        gtaint_s1.run_step(g, &mut c);

        let r = c.read_vec_partitions(&val::<Bool>(0));
        let taint_list: HashSet<_> = r
            .into_iter()
            .flat_map(|v| v.into_iter().flat_map(|c| c.into_iter().map(|(a, b)| a)))
            .collect();

        // println!(
        //     "step{}, taint_status = {:?}",
        //     i + 1,
        //     c.read_vec_partitions(&val::<Bool>(0))
        // );
        // println!("step{}, taint_list = {:?}", i + 1, taint_list);
        // println!(
        //     "step{}, taint_history = {:?}",
        //     i + 1,
        //     c.read_vec_partitions(&hash_set::<TaintMessage>(1))
        // );
        // println!(
        //     "step{}, recv_tainted_msgs = {:?}",
        //     i + 1,
        //     c.read_vec_partitions(&hash_set::<TaintMessage>(2))
        // );

        let difference: Vec<_> = taint_list
            .iter()
            .filter(|item| !last_taint_list.contains(*item))
            .collect();
        let converged = difference.is_empty();

        // println!("taint_list diff = {:?}", difference);
        // println!();

        if converged || i > iter_count {
            break;
        }

        last_taint_list = taint_list;

        if c.keep_past_state {
            c.ss += 1;
        }

        i += 1;
    }

    println!("Completed {} steps", i);

    let mut results: HashMap<String, Vec<(usize, i64, String)>> = HashMap::default();

    (0..g.num_shards())
        .into_iter()
        .fold(&mut results, |res, part_id| {
            c.fold_state(
                &hash_set::<TaintMessage>(1),
                part_id,
                res,
                |res, v_id, sc| {
                    res.insert(
                        g.vertex(*v_id).unwrap().name(),
                        // *v_id,
                        sc.into_iter()
                            .map(|msg| (msg.edge_id, msg.event_time, msg.src_vertex))
                            .collect_vec(),
                    );
                    res
                },
            )
        });

    results
}

#[cfg(test)]
mod generic_taint_tests {
    use super::*;
    use crate::db::graph::Graph;

    fn load_graph(n_shards: usize, edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new(n_shards);

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, &vec![], None).unwrap();
        }
        graph
    }

    fn test_generic_taint<T: InputVertex>(
        graph: Graph,
        iter_count: usize,
        start_time: i64,
        infected_nodes: Vec<T>,
        stop_nodes: Vec<T>,
    ) -> HashMap<String, Vec<(usize, i64, String)>> {
        let results: HashMap<String, Vec<(usize, i64, String)>> =
            generic_taint(&graph, iter_count, start_time, infected_nodes, stop_nodes)
                .into_iter()
                .collect();
        results
    }

    #[test]
    fn test_generic_taint_1() {
        let graph = load_graph(
            1,
            vec![
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
            ],
        );

        let results = test_generic_taint(graph, 20, 11, vec![2], vec![]);

        assert_eq!(
            results,
            HashMap::from([
                (
                    "5".to_string(),
                    vec![(4, 13, "2".to_string()), (5, 14, "5".to_string())]
                ),
                ("2".to_string(), vec![(0, 11, "2".to_string())]),
                ("7".to_string(), vec![(8, 15, "4".to_string())]),
                (
                    "4".to_string(),
                    vec![(3, 12, "2".to_string()), (6, 14, "5".to_string())]
                )
            ])
        );
    }

    #[test]
    fn test_generic_taint_1_multiple_start() {
        let graph = load_graph(
            1,
            vec![
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
            ],
        );

        let results = test_generic_taint(graph, 20, 11, vec![1, 2], vec![]);

        assert_eq!(
            results,
            HashMap::from([
                (
                    "4".to_string(),
                    vec![(3, 12, "2".to_string()), (6, 14, "5".to_string())]
                ),
                ("1".to_string(), vec![(0, 11, "1".to_string())]),
                (
                    "5".to_string(),
                    vec![(4, 13, "2".to_string()), (5, 14, "5".to_string())]
                ),
                ("7".to_string(), vec![(8, 15, "4".to_string())]),
                (
                    "2".to_string(),
                    vec![(2, 11, "1".to_string()), (0, 11, "2".to_string())]
                ),
            ])
        );
    }

    #[test]
    fn test_generic_taint_1_stop_nodes() {
        let graph = load_graph(
            1,
            vec![
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
            ],
        );

        let results = test_generic_taint(graph, 20, 11, vec![1, 2], vec![4, 5]);

        assert_eq!(
            results,
            HashMap::from([
                ("5".to_string(), vec![(4, 13, "2".to_string())]),
                (
                    "2".to_string(),
                    vec![(2, 11, "1".to_string()), (0, 11, "2".to_string())]
                ),
                ("1".to_string(), vec![(0, 11, "1".to_string())]),
                ("4".to_string(), vec![(3, 12, "2".to_string())]),
            ])
        );
    }

    #[test]
    fn test_generic_taint_1_multiple_history_points() {
        let graph = load_graph(
            1,
            vec![
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
            ],
        );

        let results = test_generic_taint(graph, 20, 11, vec![1, 2], vec![4, 5]);

        assert_eq!(
            results,
            HashMap::from([
                ("1".to_string(), vec![(0, 11, "1".to_string())]),
                ("5".to_string(), vec![(6, 13, "2".to_string())]),
                (
                    "2".to_string(),
                    vec![
                        (2, 12, "1".to_string()),
                        (2, 11, "1".to_string()),
                        (0, 11, "2".to_string())
                    ]
                ),
                ("4".to_string(), vec![(5, 12, "2".to_string())]),
            ])
        );
    }
}
