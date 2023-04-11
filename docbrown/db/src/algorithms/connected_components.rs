use crate::view_api::GraphViewOps;
use crate::{
    graph::Graph,
    program::{GlobalEvalState, LocalState, Program},
};
use docbrown_core::state;
use rustc_hash::FxHashMap;
use std::ops::Range;

/// Computes the connected components of a graph using the Simple Connected Components algorithm
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `window` - A range indicating the temporal window to consider
/// * `iter_count` - The number of iterations to run
///
/// # Returns
///
/// A hash map containing the mapping from component ID to the number of vertices in the component
///
pub fn weakly_connected_components(g: &Graph, iter_count: usize) -> FxHashMap<u64, u64> {
    let cc = WeaklyConnectedComponents {};

    let gs = cc.run(g, true, iter_count);

    cc.produce_output(g, &gs)
}

#[derive(Default)]
struct WeaklyConnectedComponents {}

impl Program for WeaklyConnectedComponents {
    type Out = FxHashMap<u64, u64>;

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let min = c.agg(state::def::min(0));

        c.step(|vv| {
            let g_id = vv.global_id();
            vv.update(&min, g_id);

            for n in vv.neighbours() {
                let my_min = vv.read(&min);
                n.update(&min, my_min);
            }
        })
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        // this will make the global state merge all the values for all partitions
        let min = c.agg(state::def::min::<u64>(0));

        c.step(|vv| {
            let current = vv.read(&min);
            let prev = vv.read_prev(&min);
            current != prev
        })
    }

    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
        let agg = state::def::min::<u64>(0);

        let mut results: FxHashMap<u64, u64> = FxHashMap::default();

        (0..g.num_shards())
            .into_iter()
            .fold(&mut results, |res, part_id| {
                gs.fold_state(&agg, part_id, res, |res, v_id, cc| {
                    res.insert(*v_id, cc);
                    res
                })
            });

        results
    }
}

#[cfg(test)]
mod cc_test {
    use super::*;
    use itertools::*;
    use std::{cmp::Reverse, iter::once};

    #[test]
    fn simple_connected_components() {
        let program = WeaklyConnectedComponents::default();

        let graph = Graph::new(2);

        let edges = vec![
            (1, 2, 1),
            (2, 3, 2),
            (3, 4, 3),
            (3, 5, 4),
            (6, 5, 5),
            (7, 8, 6),
            (8, 7, 7),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![], None).unwrap();
        }

        let mut gs = GlobalEvalState::new(graph.clone(), true);
        program.run_step(&graph, &mut gs);

        let agg = state::def::min::<u64>(0);

        let expected =             // output from the eval running on the first shard
            vec![
                vec![7, 1], // shard 0 (2, 4, 6, 8)
                vec![3, 7, 3, 1, 3, 2], // shard 1 (1, 3, 5, 7)
            ];

        let actual_part1 = &gs.read_vec_partitions(&agg)[0];
        let actual_part2 = &gs.read_vec_partitions(&agg)[1];

        // after one step all partitions have the same data since it's been merged and broadcasted
        assert_eq!(actual_part1, actual_part2);
        println!("ACTUAL: {:?}", actual_part1);
        assert_eq!(actual_part1, &expected);

        program.run_step(&graph, &mut gs);

        let expected =             // output from the eval running on the first shard
            vec![
                vec![7, 1], // shard 0 (2, 4, 6, 8)
                vec![2, 7, 2, 1, 3, 1], // shard 1 (1, 3, 5, 7)
            ];

        let actual_part1 = &gs.read_vec_partitions(&agg)[0];
        let actual_part2 = &gs.read_vec_partitions(&agg)[1];

        // after one step all partitions have the same data since it's been merged and broadcasted
        assert_eq!(actual_part1, actual_part2);
        println!("ACTUAL: {:?}", actual_part1);
        assert_eq!(actual_part1, &expected);

        program.run_step(&graph, &mut gs);

        let expected =             // output from the eval running on the first shard
            vec![
                vec![7, 1], // shard 0 (2, 4, 6, 8)
                vec![1, 7, 1, 1, 2, 1], // shard 1 (1, 3, 5, 7)
            ];

        let actual_part1 = &gs.read_vec_partitions(&agg)[0];
        let actual_part2 = &gs.read_vec_partitions(&agg)[1];

        // after one step all partitions have the same data since it's been merged and broadcasted
        assert_eq!(actual_part1, actual_part2);
        println!("ACTUAL: {:?}", actual_part1);
        assert_eq!(actual_part1, &expected);

        program.run_step(&graph, &mut gs);

        let expected =             // output from the eval running on the first shard
            vec![
                vec![7, 1], // shard 0 (2, 4, 6, 8)
                vec![1, 7, 1, 1, 1, 1], // shard 1 (1, 3, 5, 7)
            ];

        let actual_part1 = &gs.read_vec_partitions(&agg)[0];
        let actual_part2 = &gs.read_vec_partitions(&agg)[1];

        // after one step all partitions have the same data since it's been merged and broadcasted
        assert_eq!(actual_part1, actual_part2);
        println!("ACTUAL: {:?}", actual_part1);
        assert_eq!(actual_part1, &expected);
    }

    #[test]
    fn run_loop_simple_connected_components() {
        let graph = Graph::new(2);

        let edges = vec![
            (1, 2, 1),
            (2, 3, 2),
            (3, 4, 3),
            (3, 5, 4),
            (6, 5, 5),
            (7, 8, 6),
            (8, 7, 7),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![], None).unwrap();
        }

        let window = 0..10;

        let results: FxHashMap<u64, u64> = weakly_connected_components(&graph, usize::MAX)
            .into_iter()
            .map(|(k, v)| (k, v as u64))
            .collect();

        assert_eq!(
            results,
            vec![
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 1),
                (5, 1),
                (6, 1),
                (7, 7),
                (8, 7),
            ]
            .into_iter()
            .collect::<FxHashMap<u64, u64>>()
        );
    }

    #[test]
    fn simple_connected_components_2() {
        let graph = Graph::new(2);

        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![], None).unwrap();
        }

        let window = 0..25;

        let results: FxHashMap<u64, u64> = weakly_connected_components(&graph, usize::MAX)
            .into_iter()
            .map(|(k, v)| (k, v as u64))
            .collect();

        assert_eq!(
            results,
            vec![
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 1),
                (5, 1),
                (6, 1),
                (7, 1),
                (8, 1),
                (9, 1),
                (10, 1),
                (11, 1),
            ]
            .into_iter()
            .collect::<FxHashMap<u64, u64>>()
        );
    }

    // connected components on a graph with 1 node and a self loop
    #[test]
    fn simple_connected_components_3() {
        let graph = Graph::new(2);

        let edges = vec![(1, 1, 1)];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![], None).unwrap();
        }

        let window = 0..25;

        let results: FxHashMap<u64, u64> = weakly_connected_components(&graph, usize::MAX);

        assert_eq!(
            results,
            vec![(1, 1),].into_iter().collect::<FxHashMap<u64, u64>>()
        );
    }

    #[quickcheck]
    fn circle_graph_the_smallest_value_is_the_cc(vs: Vec<u64>) {
        if vs.len() > 0 {
            let vs = vs.into_iter().unique().collect::<Vec<u64>>();

            let smallest = vs.iter().min().unwrap();

            let first = vs[0];
            // pairs of vertices from vs one after the next
            let edges = vs
                .iter()
                .zip(chain!(vs.iter().skip(1), once(&first)))
                .map(|(a, b)| (*a, *b))
                .collect::<Vec<(u64, u64)>>();

            assert_eq!(edges[0].0, first);
            assert_eq!(edges.last().unwrap().1, first);

            let graph = Graph::new(2);

            for (src, dst) in edges.iter() {
                graph.add_edge(0, *src, *dst, &vec![], None).unwrap();
            }

            // now we do connected components over window 0..1

            let window = 0..1;

            let components: FxHashMap<u64, u64> = weakly_connected_components(&graph, usize::MAX);

            let actual = components
                .iter()
                .group_by(|(_, cc)| *cc)
                .into_iter()
                .map(|(cc, group)| (cc, Reverse(group.count())))
                .sorted_by(|l, r| l.1.cmp(&r.1))
                .map(|(cc, count)| (*cc, count.0))
                .take(1)
                .next();

            assert_eq!(
                actual,
                Some((*smallest, edges.len())),
                "actual: {:?}",
                actual
            );
        }
    }
}
