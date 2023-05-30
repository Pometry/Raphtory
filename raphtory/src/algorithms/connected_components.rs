use std::cmp;
use crate::db::view_api::VertexViewOps;
use crate::{
    core::state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    db::{
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
        view_api::GraphViewOps,
    },
};
use std::collections::HashMap;
use crate::db::task::eval_vertex::EvalVertexView;

#[derive(Clone, Debug)]
struct WccState {
    component: u64,
}

impl WccState {
    fn new() -> Self {
        Self {
            component: 0,
        }
    }
}

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
) -> HashMap<String, u64>
where
    G: GraphViewOps,
{
    let mut ctx: Context<G, ComputeStateVec> = graph.into();

    let step1 = ATask::new(move |vv| {
        let min_neighbour_id = vv.neighbours().id().min();
        let id = vv.id();
        let state: &mut WccState = vv.get_mut();
        state.component = cmp::min(min_neighbour_id.unwrap_or(id), id);
        Step::Continue
    });

    let step2 = ATask::new(move |vv: &mut EvalVertexView<'_, G,ComputeStateVec, WccState>| {
        let prev:u64 = vv.prev().component;
        let current = vv.neighbours().into_iter().map(|n|n.prev().component).min().unwrap_or(prev);
        let state: &mut WccState = vv.get_mut();
        if current<prev {
            state.component = current;
            Step::Continue
        }
        else {
            Step::Done
        }
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(step1)],
        vec![Job::read_only(step2)],
        WccState::new(),
        |g, _, _, local| {
            local
                .iter()
                .filter_map(|line| {
                    line.as_ref()
                        .map(|(v_ref, state)| (v_ref.clone(), state.component))
                })
                .collect::<HashMap<_, _>>()
        },
        threads,
        iter_count,
        None,
        None,
    ).into_iter()
        .map(|(k, v)| (graph.vertex_name(k), v))
        .collect()
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
        let results: HashMap<String, u64> = weakly_connected_components(&graph, usize::MAX, None);

        assert_eq!(
            results,
            vec![
                ("1".to_string(), 1),
                ("2".to_string(), 1),
                ("3".to_string(), 1),
                ("4".to_string(), 1),
                ("5".to_string(), 1),
                ("6".to_string(), 1),
                ("7".to_string(), 7),
                ("8".to_string(), 7),
            ]
            .into_iter()
            .collect::<HashMap<String, u64>>()
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

        let results: HashMap<String, u64> = weakly_connected_components(&graph, usize::MAX, None);

        assert_eq!(
            results,
            vec![
                ("1".to_string(), 1),
                ("2".to_string(), 1),
                ("3".to_string(), 1),
                ("4".to_string(), 1),
                ("5".to_string(), 1),
                ("6".to_string(), 1),
                ("7".to_string(), 1),
                ("8".to_string(), 1),
                ("9".to_string(), 1),
                ("10".to_string(), 1),
                ("11".to_string(), 1),
            ]
            .into_iter()
            .collect::<HashMap<String, u64>>()
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

        let results: HashMap<String, u64> = weakly_connected_components(&graph, usize::MAX, None);

        assert_eq!(
            results,
            vec![("1".to_string(), 1),]
                .into_iter()
                .collect::<HashMap<String, u64>>()
        );
    }

    #[quickcheck]
    fn circle_graph_the_smallest_value_is_the_cc(vs: Vec<u64>) {
        if !vs.is_empty() {
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

            let components: HashMap<String, u64> =
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
