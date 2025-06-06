use crate::{
    core::state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    db::{
        api::{
            state::NodeState,
            view::{NodeViewOps, StaticGraphViewOps},
        },
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::GraphViewOps,
};
use num_traits::abs;

#[derive(Clone, Debug, Default)]
struct PageRankState {
    score: f64,
    out_degree: usize,
}

impl PageRankState {
    fn new(num_nodes: usize) -> Self {
        Self {
            score: 1f64 / num_nodes as f64,
            out_degree: 0,
        }
    }

    fn reset(&mut self) {
        self.score = 0f64;
    }
}

/// PageRank Algorithm:
/// PageRank shows how important a node is in a graph.
///
/// # Arguments
///
/// - `g`: A GraphView object
/// - `iter_count`: Number of iterations to run the algorithm for
/// - `threads`: Number of threads to use for parallel execution
/// - `tol`: The tolerance value for convergence
/// - `use_l2_norm`: Whether to use L2 norm for convergence
/// - `damping_factor`: Probability of likelihood the spread will continue
///
/// # Returns
///
/// An [AlgorithmResult] object containing the mapping from node ID to the PageRank score of the node
///
pub fn unweighted_page_rank<G: StaticGraphViewOps>(
    g: &G,
    iter_count: Option<usize>,
    threads: Option<usize>,
    tol: Option<f64>,
    use_l2_norm: bool,
    damping_factor: Option<f64>,
) -> NodeState<'static, f64, G> {
    let n = g.count_nodes();

    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let tol: f64 = tol.unwrap_or(0.000001f64);
    let damp = damping_factor.unwrap_or(0.85);
    let iter_count = iter_count.unwrap_or(20);
    let teleport_prob = (1f64 - damp) / n as f64;
    let factor = damp / n as f64;

    let max_diff = accumulators::sum::<f64>(2);

    let total_sink_contribution = accumulators::sum::<f64>(4);

    ctx.global_agg_reset(max_diff);

    ctx.global_agg_reset(total_sink_contribution);

    let step1 = ATask::new(move |s| {
        let out_degree = s.out_degree();
        let state: &mut PageRankState = s.get_mut();
        state.out_degree = out_degree;
        Step::Continue
    });

    let step2: ATask<G, ComputeStateVec, PageRankState, _> = ATask::new(move |s| {
        // reset score
        {
            let state: &mut PageRankState = s.get_mut();
            state.reset();
        }

        for t in s.in_neighbours() {
            let prev = t.prev();

            s.get_mut().score += prev.score / prev.out_degree as f64;
        }

        s.get_mut().score *= damp;

        s.get_mut().score += teleport_prob;
        Step::Continue
    });

    let step3 = ATask::new(move |s| {
        let state: &mut PageRankState = s.get_mut();

        if state.out_degree == 0 {
            let curr = s.prev().score;

            let ts_contrib = factor * curr;
            s.global_update(&total_sink_contribution, ts_contrib);
        }
        Step::Continue
    });

    let step4 = ATask::new(move |s| {
        //read total sink contribution
        let total_sink_contribution = s
            .read_global_state(&total_sink_contribution)
            .unwrap_or_default();
        // update local score with total sink contribution
        let state: &mut PageRankState = s.get_mut();
        state.score += total_sink_contribution;

        // update global max diff

        let curr = state.score;
        let prev = s.prev().score;

        let md = if use_l2_norm {
            f64::powi(abs(prev - curr), 2)
        } else {
            abs(prev - curr)
        };

        s.global_update(&max_diff, md);
        Step::Continue
    });

    let step5 = Job::Check(Box::new(move |state| {
        let max_diff_val = state.read(&max_diff);
        let cont = if use_l2_norm {
            let sum_d = f64::sqrt(max_diff_val);
            (sum_d) > tol * n as f64
        } else {
            (max_diff_val) > tol * n as f64
        };
        if cont {
            Step::Continue
        } else {
            Step::Done
        }
    }));

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let num_nodes = g.count_nodes();

    runner.run(
        vec![Job::new(step1)],
        vec![Job::new(step2), Job::new(step3), Job::new(step4), step5],
        Some(vec![PageRankState::new(num_nodes); num_nodes]),
        |_, _, _, local| NodeState::new_from_eval_mapped(g.clone(), local, |v| v.score),
        threads,
        iter_count,
        None,
        None,
    )
}

#[cfg(test)]
pub mod page_rank_tests {
    use super::*;
    use crate::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::{NodeStateOps, NO_PROPS},
        test_storage,
    };
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use std::borrow::Borrow;

    fn load_graph() -> Graph {
        let graph = Graph::new();

        let edges = vec![(1, 2), (1, 4), (2, 3), (3, 1), (4, 1)];

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    #[test]
    fn test_page_rank() {
        let graph = load_graph();

        test_storage!(&graph, |graph| {
            let results = unweighted_page_rank(graph, Some(1000), Some(1), None, true, None);

            assert_eq_f64(results.get_by_node("1"), Some(&0.38694), 5);
            assert_eq_f64(results.get_by_node("2"), Some(&0.20195), 5);
            assert_eq_f64(results.get_by_node("4"), Some(&0.20195), 5);
            assert_eq_f64(results.get_by_node("3"), Some(&0.20916), 5);
        });
    }

    #[test]
    fn motif_page_rank() {
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

        let graph = Graph::new();

        for (src, dst, t) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = unweighted_page_rank(graph, Some(1000), Some(4), None, true, None);

            assert_eq_f64(results.get_by_node("10"), Some(&0.072082), 5);
            assert_eq_f64(results.get_by_node("8"), Some(&0.136473), 5);
            assert_eq_f64(results.get_by_node("3"), Some(&0.15484), 5);
            assert_eq_f64(results.get_by_node("6"), Some(&0.07208), 5);
            assert_eq_f64(results.get_by_node("11"), Some(&0.06186), 5);
            assert_eq_f64(results.get_by_node("2"), Some(&0.03557), 5);
            assert_eq_f64(results.get_by_node("1"), Some(&0.11284), 5);
            assert_eq_f64(results.get_by_node("4"), Some(&0.07944), 5);
            assert_eq_f64(results.get_by_node("7"), Some(&0.01638), 5);
            assert_eq_f64(results.get_by_node("9"), Some(&0.06186), 5);
            assert_eq_f64(results.get_by_node("5"), Some(&0.19658), 5);
        });
    }

    #[test]
    fn two_nodes_page_rank() {
        let edges = vec![(1, 2), (2, 1)];

        let graph = Graph::new();

        for (t, (src, dst)) in edges.into_iter().enumerate() {
            graph.add_edge(t as i64, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = unweighted_page_rank(graph, Some(1000), Some(4), None, false, None);

            assert_eq_f64(results.get_by_node("1"), Some(&0.5), 3);
            assert_eq_f64(results.get_by_node("2"), Some(&0.5), 3);
        });
    }

    #[test]
    fn three_nodes_page_rank_one_dangling() {
        let edges = vec![(1, 2), (2, 1), (2, 3)];

        let graph = Graph::new();

        for (t, (src, dst)) in edges.into_iter().enumerate() {
            graph.add_edge(t as i64, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let results = unweighted_page_rank(graph, Some(10), Some(4), None, false, None);

            assert_eq_f64(results.get_by_node("1"), Some(&0.303), 3);
            assert_eq_f64(results.get_by_node("2"), Some(&0.393), 3);
            assert_eq_f64(results.get_by_node("3"), Some(&0.303), 3);
        });
    }

    #[test]
    fn dangling_page_rank() {
        let edges = vec![
            (1, 2),
            (1, 3),
            (2, 3),
            (3, 1),
            (3, 2),
            (3, 4),
            // dangling from here
            (4, 5),
            (5, 6),
            (6, 7),
            (7, 8),
            (8, 9),
            (9, 10),
            (10, 11),
        ]
        .into_iter()
        .enumerate()
        .map(|(t, (src, dst))| (src, dst, t as i64))
        .collect_vec();

        let graph = Graph::new();

        for (src, dst, t) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let results = unweighted_page_rank(graph, Some(1000), Some(4), None, true, None);

            assert_eq_f64(results.get_by_node("1"), Some(&0.055), 3);
            assert_eq_f64(results.get_by_node("2"), Some(&0.079), 3);
            assert_eq_f64(results.get_by_node("3"), Some(&0.113), 3);
            assert_eq_f64(results.get_by_node("4"), Some(&0.055), 3);
            assert_eq_f64(results.get_by_node("5"), Some(&0.070), 3);
            assert_eq_f64(results.get_by_node("6"), Some(&0.083), 3);
            assert_eq_f64(results.get_by_node("7"), Some(&0.093), 3);
            assert_eq_f64(results.get_by_node("8"), Some(&0.102), 3);
            assert_eq_f64(results.get_by_node("9"), Some(&0.110), 3);
            assert_eq_f64(results.get_by_node("10"), Some(&0.117), 3);
            assert_eq_f64(results.get_by_node("11"), Some(&0.122), 3);
        });
    }

    pub fn assert_eq_f64<T: Borrow<f64> + PartialEq + std::fmt::Debug>(
        a: Option<T>,
        b: Option<T>,
        decimals: u8,
    ) {
        if a.is_none() || b.is_none() {
            assert_eq!(a, b);
        } else {
            let factor = 10.0_f64.powi(decimals as i32);
            match (a, b) {
                (Some(a), Some(b)) => {
                    let left = (a.borrow() * factor).round();
                    let right = (b.borrow() * factor).round();
                    assert_eq!(left, right,);
                }
                _ => unreachable!(),
            }
        }
    }
}
