use crate::{
    core::state::{self, ComputeStateVec},
    db::{
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
        view_api::internal::GraphViewInternalOps,
    },
};
use rustc_hash::FxHashMap;

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
pub fn weakly_connected_components<G>(
    graph: &G,
    iter_count: usize,
    threads: Option<usize>,
) -> FxHashMap<u64, u64>
where
    G: GraphViewInternalOps + Send + Sync + Clone + 'static,
{
    let mut ctx: Context<G, ComputeStateVec> = graph.into();

    let min = state::def::min::<u64>(0);

    // setup the aggregator to be merged post execution
    ctx.agg(min.clone());

    let step1 = ATask::new(move |vv| {
        vv.update(&min, vv.global_id());

        for n in vv.neighbours() {
            let my_min = vv.read(&min);
            n.update(&min, my_min)
        }

        Step::Continue
    });

    let step2 = ATask::new(move |vv| {
        let current = vv.read(&min);
        let prev = vv.read_prev(&min);

        if current == prev {
            Step::Done
        } else {
            Step::Continue
        }
    });

    let tasks = vec![Job::new(step1), Job::read_only(step2)];
    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let (state, _) = runner.run(vec![], tasks, threads, iter_count, None, None);

    let mut map: FxHashMap<u64, u64> = FxHashMap::default();

    state.fold_state_internal(runner.ctx.ss(), &mut map, &min, |res, shard, pid, cc| {
        if let Some(v_ref) = graph.lookup_by_pid_and_shard(pid, shard) {
            res.insert(v_ref.g_id, cc);
        }
        res
    });

    map
}

#[cfg(test)]
mod cc_test {
    use crate::db::graph::Graph;

    use super::*;
    use itertools::*;
    use std::{cmp::Reverse, iter::once};

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

        let results: FxHashMap<u64, u64> = weakly_connected_components(&graph, usize::MAX, None);

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

        let results: FxHashMap<u64, u64> = weakly_connected_components(&graph, usize::MAX, None);

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

        let results: FxHashMap<u64, u64> = weakly_connected_components(&graph, usize::MAX, None);

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

            let components: FxHashMap<u64, u64> =
                weakly_connected_components(&graph, usize::MAX, None);

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
