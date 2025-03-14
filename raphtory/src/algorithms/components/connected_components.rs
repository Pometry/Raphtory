use crate::{
    core::state::compute_state::ComputeStateVec,
    db::{
        api::{
            state::NodeState,
            view::{NodeViewOps, StaticGraphViewOps},
        },
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
};

#[derive(Clone, Debug, Default)]
struct WccState {
    component: usize,
}

/// Computes the connected community_detection of a graph using the Simple Connected Components algorithm
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `iter_count` - The number of iterations to run
/// * `threads` - Number of threads to use
///
/// # Returns
///
/// An [NodeState] containing the mapping from each node to its component ID
///
pub fn weakly_connected_components<G>(
    g: &G,
    iter_count: usize,
    threads: Option<usize>,
) -> NodeState<'static, usize, G>
where
    G: StaticGraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = g.into();
    let step1 = ATask::new(move |vv| {
        let min_neighbour_id = vv.neighbours().iter().map(|n| n.node.0).min();
        let id = vv.node.0;
        let state: &mut WccState = vv.get_mut();
        state.component = min_neighbour_id.unwrap_or(id).min(id);
        Step::Continue
    });

    let step2 = ATask::new(move |vv: &mut EvalNodeView<G, WccState>| {
        let prev: usize = vv.prev().component;
        let current = vv
            .neighbours()
            .into_iter()
            .map(|n| n.prev().component)
            .min()
            .unwrap_or(prev);
        let state: &mut WccState = vv.get_mut();
        if current < prev {
            state.component = current;
            Step::Continue
        } else {
            Step::Done
        }
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    runner.run(
        vec![Job::new(step1)],
        vec![Job::read_only(step2)],
        None,
        |_, _, _, local: Vec<WccState>| {
            NodeState::new_from_eval_mapped(g.clone(), local, |v| v.component)
        },
        threads,
        iter_count,
        None,
        None,
    )
}

#[cfg(test)]
mod cc_test {
    use super::*;
    use crate::{db::api::mutation::AdditionOps, prelude::*, test_storage};
    use itertools::*;
    use quickcheck_macros::quickcheck;
    use std::{cmp::Reverse, collections::HashMap, iter::once};

    #[test]
    fn run_loop_simple_connected_components() {
        let graph = Graph::new();

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
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = weakly_connected_components(graph, usize::MAX, None);
            assert_eq!(
                results,
                vec![
                    ("1".to_string(), 0),
                    ("2".to_string(), 0),
                    ("3".to_string(), 0),
                    ("4".to_string(), 0),
                    ("5".to_string(), 0),
                    ("6".to_string(), 0),
                    ("7".to_string(), 6),
                    ("8".to_string(), 6),
                ]
                .into_iter()
                .collect::<HashMap<_, _>>()
            );
        });
    }

    #[test]
    fn simple_connected_components_2() {
        let graph = Graph::new();

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
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = weakly_connected_components(graph, usize::MAX, None);

            assert_eq!(
                results,
                vec![
                    ("1".to_string(), 0),
                    ("2".to_string(), 0),
                    ("3".to_string(), 0),
                    ("4".to_string(), 0),
                    ("5".to_string(), 0),
                    ("6".to_string(), 0),
                    ("7".to_string(), 0),
                    ("8".to_string(), 0),
                    ("9".to_string(), 0),
                    ("10".to_string(), 0),
                    ("11".to_string(), 0),
                ]
                .into_iter()
                .collect::<HashMap<_, _>>()
            );
        });
    }

    // connected community_detection on a graph with 1 node and a self loop
    #[test]
    fn simple_connected_components_3() {
        let graph = Graph::new();

        let edges = vec![(1, 1, 1)];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = weakly_connected_components(graph, usize::MAX, None);

            assert_eq!(
                results,
                vec![("1".to_string(), 0)]
                    .into_iter()
                    .collect::<HashMap<_, _>>()
            );
        });
    }

    #[test]
    fn windowed_connected_components() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).expect("add edge");
        graph.add_edge(0, 2, 1, NO_PROPS, None).expect("add edge");
        graph.add_edge(9, 3, 4, NO_PROPS, None).expect("add edge");
        graph.add_edge(9, 4, 3, NO_PROPS, None).expect("add edge");

        test_storage!(&graph, |graph| {
            let results = weakly_connected_components(graph, usize::MAX, None);
            let expected = vec![
                ("1".to_string(), 0),
                ("2".to_string(), 0),
                ("3".to_string(), 2),
                ("4".to_string(), 2),
            ]
            .into_iter()
            .collect::<HashMap<_, _>>();

            assert_eq!(results, expected);

            let wg = graph.window(0, 2);
            let results = weakly_connected_components(&wg, usize::MAX, None);

            let expected = HashMap::from([("1", 0), ("2", 0)]);

            assert_eq!(results, expected);
        });
    }

    #[quickcheck]
    fn circle_graph_edges(vs: Vec<u64>) {
        if !vs.is_empty() {
            let vs = vs.into_iter().unique().collect::<Vec<u64>>();

            let _smallest = vs.iter().min().unwrap();

            let first = vs[0];
            // pairs of nodes from vs one after the next
            let edges = vs
                .iter()
                .zip(chain!(vs.iter().skip(1), once(&first)))
                .map(|(a, b)| (*a, *b))
                .collect::<Vec<(u64, u64)>>();

            assert_eq!(edges[0].0, first);
            assert_eq!(edges.last().unwrap().1, first);
        }
    }

    #[quickcheck]
    fn circle_graph_the_smallest_value_is_the_cc(vs: Vec<u64>) {
        if !vs.is_empty() {
            let graph = Graph::new();

            let vs = vs.into_iter().unique().collect::<Vec<u64>>();

            let first = vs[0];

            // pairs of nodes from vs one after the next
            let edges = vs
                .iter()
                .zip(chain!(vs.iter().skip(1), once(&first)))
                .map(|(a, b)| (*a, *b))
                .collect::<Vec<(u64, u64)>>();

            for (src, dst) in edges.iter() {
                graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
            }

            test_storage!(&graph, |graph| {
                // now we do connected community_detection over window 0..1
                let res = weakly_connected_components(graph, usize::MAX, None).groups();

                let (cc, size) = res
                    .into_iter_groups()
                    .map(|(cc, group)| (cc, Reverse(group.len())))
                    .sorted_by(|l, r| l.1.cmp(&r.1))
                    .map(|(cc, count)| (cc, count.0))
                    .take(1)
                    .next()
                    .unwrap();

                assert_eq!(cc, 0);

                assert_eq!(size, edges.len());
            });
        }
    }
}
