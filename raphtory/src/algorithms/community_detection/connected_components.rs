use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::{
        entities::{vertices::vertex_ref::VertexRef, VID},
        state::compute_state::ComputeStateVec,
    },
    db::{
        api::view::{GraphViewOps, VertexViewOps},
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
            vertex::eval_vertex::EvalVertexView,
        },
    },
};
use std::{cmp, collections::HashMap};

#[derive(Clone, Debug, Default)]
struct WccState {
    component: u64,
}

/// Computes the connected community_detection of a graph using the Simple Connected Components algorithm
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `iter_count` - The number of iterations to run
/// * `threads` - Number of threads to use
///
/// Returns:
///
/// An AlgorithmResult containing the mapping from component ID to the number of vertices in the component
///
pub fn weakly_connected_components<G>(
    graph: &G,
    iter_count: usize,
    threads: Option<usize>,
) -> AlgorithmResult<G, u64, u64>
where
    G: GraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = graph.into();
    let step1 = ATask::new(move |vv| {
        let min_neighbour_id = vv.neighbours().id().min();
        let id = vv.id();
        let state: &mut WccState = vv.get_mut();
        state.component = cmp::min(min_neighbour_id.unwrap_or(id), id);
        Step::Continue
    });

    let step2 = ATask::new(
        move |vv: &mut EvalVertexView<'_, G, ComputeStateVec, WccState>| {
            let prev: u64 = vv.prev().component;
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
        },
    );

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);
    let results_type = std::any::type_name::<u64>();

    let res = runner.run(
        vec![Job::new(step1)],
        vec![Job::read_only(step2)],
        None,
        |_, _, _, local: Vec<WccState>| {
            let layers: crate::core::entities::LayerIds = graph.layer_ids();
            let edge_filter = graph.edge_filter();
            local
                .iter()
                .enumerate()
                .filter_map(|(v_ref_id, state)| {
                    let v_ref = VID(v_ref_id);
                    graph
                        .has_vertex_ref(VertexRef::Internal(v_ref), &layers, edge_filter)
                        .then_some((v_ref_id, state.component))
                })
                .collect::<HashMap<_, _>>()
        },
        threads,
        iter_count,
        None,
        None,
    );
    AlgorithmResult::new(graph.clone(), "Connected Components", results_type, res)
}

#[cfg(test)]
mod cc_test {
    use crate::prelude::*;
    use std::cmp::Reverse;

    use super::*;
    use crate::db::api::mutation::AdditionOps;
    use itertools::*;
    use std::iter::once;

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
        let results = weakly_connected_components(&graph, usize::MAX, None).get_all_with_names();
        assert_eq!(
            results,
            vec![
                ("1".to_string(), Some(1)),
                ("2".to_string(), Some(1)),
                ("3".to_string(), Some(1)),
                ("4".to_string(), Some(1)),
                ("5".to_string(), Some(1)),
                ("6".to_string(), Some(1)),
                ("7".to_string(), Some(7)),
                ("8".to_string(), Some(7)),
            ]
            .into_iter()
            .collect::<HashMap<String, Option<u64>>>()
        );
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

        let results = weakly_connected_components(&graph, usize::MAX, None).get_all_with_names();

        assert_eq!(
            results,
            vec![
                ("1".to_string(), Some(1)),
                ("2".to_string(), Some(1)),
                ("3".to_string(), Some(1)),
                ("4".to_string(), Some(1)),
                ("5".to_string(), Some(1)),
                ("6".to_string(), Some(1)),
                ("7".to_string(), Some(1)),
                ("8".to_string(), Some(1)),
                ("9".to_string(), Some(1)),
                ("10".to_string(), Some(1)),
                ("11".to_string(), Some(1)),
            ]
            .into_iter()
            .collect::<HashMap<String, Option<u64>>>()
        );
    }

    // connected community_detection on a graph with 1 node and a self loop
    #[test]
    fn simple_connected_components_3() {
        let graph = Graph::new();

        let edges = vec![(1, 1, 1)];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        let results = weakly_connected_components(&graph, usize::MAX, None).get_all_with_names();

        assert_eq!(
            results,
            vec![("1".to_string(), Some(1))]
                .into_iter()
                .collect::<HashMap<String, Option<u64>>>()
        );
    }

    #[test]
    fn windowed_connected_components() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, None).expect("add edge");
        graph.add_edge(0, 2, 1, NO_PROPS, None).expect("add edge");
        graph.add_edge(9, 3, 4, NO_PROPS, None).expect("add edge");
        graph.add_edge(9, 4, 3, NO_PROPS, None).expect("add edge");

        let results = weakly_connected_components(&graph, usize::MAX, None).get_all_with_names();
        let expected = vec![
            ("1".to_string(), Some(1)),
            ("2".to_string(), Some(1)),
            ("3".to_string(), Some(3)),
            ("4".to_string(), Some(3)),
        ]
        .into_iter()
        .collect::<HashMap<String, Option<u64>>>();

        assert_eq!(results, expected);

        let wg = graph.window(0, 2);
        let results = weakly_connected_components(&wg, usize::MAX, None).get_all_with_names();

        let expected = vec![("1", 1), ("2", 1)]
            .into_iter()
            .map(|(k, v)| (k.to_string(), Some(v)))
            .collect::<HashMap<String, Option<u64>>>();

        assert_eq!(results, expected);
    }

    #[quickcheck]
    fn circle_graph_edges(vs: Vec<u64>) {
        if !vs.is_empty() {
            let vs = vs.into_iter().unique().collect::<Vec<u64>>();

            let _smallest = vs.iter().min().unwrap();

            let first = vs[0];
            // pairs of vertices from vs one after the next
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

            let smallest = vs.iter().min().unwrap();

            let first = vs[0];

            // pairs of vertices from vs one after the next
            let edges = vs
                .iter()
                .zip(chain!(vs.iter().skip(1), once(&first)))
                .map(|(a, b)| (*a, *b))
                .collect::<Vec<(u64, u64)>>();

            for (src, dst) in edges.iter() {
                graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
            }

            // now we do connected community_detection over window 0..1
            let res = weakly_connected_components(&graph, usize::MAX, None).group_by();

            let actual = res
                .into_iter()
                .map(|(cc, group)| (cc, Reverse(group.len())))
                .sorted_by(|l, r| l.1.cmp(&r.1))
                .map(|(cc, count)| (cc, count.0))
                .take(1)
                .next()
                .unwrap();

            assert_eq!(actual, (*smallest, edges.len()));
        }
    }
}
