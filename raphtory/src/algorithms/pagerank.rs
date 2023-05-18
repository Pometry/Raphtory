use crate::db::view_api::VertexViewOps;
use crate::{
    core::{
        agg::InitOneF64,
        state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    },
    db::{
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
        view_api::GraphViewOps,
    },
};
use num_traits::abs;
use std::collections::HashMap;

#[allow(unused_variables)]
pub fn unweighted_page_rank<G: GraphViewOps>(
    g: &G,
    iter_count: usize,
    threads: Option<usize>,
    tol: Option<f64>,
) -> HashMap<String, f32> {
    let total_vertices = g.num_vertices();
    let total_edges = g.num_edges();

    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let max_diff_val: f64 = tol.unwrap_or_else(|| 0.0000001f64);
    let damping_factor = 0.85;

    let score = accumulators::val::<f64>(0).init::<InitOneF64>();
    let recv_score = accumulators::sum::<f64>(1);
    let max_diff = accumulators::max::<f64>(2);
    let dangling = accumulators::sum::<f64>(3);

    ctx.agg_reset(recv_score);
    ctx.global_agg_reset(max_diff);
    ctx.global_agg_reset(dangling);

    let step1 = ATask::new(move |s| {
        s.update_local(&score, s.in_degree() as f64 / total_edges as f64 * total_vertices as f64);
        Step::Continue
    });

    let step2 = ATask::new(move |s| {
        let out_degree = s.out_degree();
        if out_degree > 0 {
            let new_score = s.read_local(&score) / out_degree as f64;
            for t in s.out_neighbours() {
                t.update(&recv_score, new_score)
            }
        } else {
            s.global_update(&dangling, s.read_local(&score) / total_vertices as f64);
        }
        Step::Continue
    });

    let step3 = ATask::new(move |s| {
        let dangling_v = s.read_global_state(&dangling).unwrap_or_default();

        s.update_local(
            &score,
            (1f64 - damping_factor) + (damping_factor * (s.read(&recv_score) + dangling_v)),
        );
        let prev = s.read_local_prev(&score);
        let curr = s.read_local(&score);

        let md = abs(prev - curr);

        s.global_update(&max_diff, md);
        Step::Continue
    });

    let step4 = Job::Check(Box::new(move |state| {
        let max_d = state.read(&max_diff);
        println!("max diff: {}", max_d);

        if (max_d / total_vertices as f64) > max_diff_val {
            Step::Continue
        } else {
            Step::Done
        }
    }));

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let num_vertices = g.num_vertices() as f64;

    let out = runner.run(
        vec![Job::new(step1)],
        vec![Job::new(step2), Job::new(step3), step4],
        |_, _, els| els.finalize(&score, |score| score),
        threads,
        iter_count,
        None,
        None,
    );

    out.into_iter()
        .map(|(k, v)| (k, v as f32 / num_vertices as f32))
        .collect()
}

#[cfg(test)]
mod page_rank_tests {
    use std::borrow::Borrow;

    use itertools::Itertools;
    use pretty_assertions::assert_eq;

    use crate::db::graph::Graph;

    use super::*;

    fn load_graph(n_shards: usize) -> Graph {
        let graph = Graph::new(n_shards);

        let edges = vec![(1, 2), (1, 4), (2, 3), (3, 1), (4, 1)];

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, &vec![], None).unwrap();
        }
        graph
    }

    fn test_page_rank(n_shards: usize) {
        let graph = load_graph(n_shards);

        let results: HashMap<String, f32> = unweighted_page_rank(&graph, 25, Some(1), None)
            .into_iter()
            .collect();

        assert_eq!(
            results,
            vec![
                ("2".to_string(), 0.20249715),
                ("4".to_string(), 0.20249715),
                ("1".to_string(), 0.38669053),
                ("3".to_string(), 0.20831521)
            ]
            .into_iter()
            .collect::<HashMap<String, f32>>()
        );
    }

    #[test]
    fn test_page_rank_1() {
        test_page_rank(1);
    }

    #[test]
    fn test_page_rank_2() {
        test_page_rank(2);
    }

    #[test]
    fn test_page_rank_3() {
        test_page_rank(3);
    }

    #[test]
    fn test_page_rank_4() {
        test_page_rank(4);
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

        let graph = Graph::new(4);

        for (src, dst, t) in edges {
            graph.add_edge(t, src, dst, &vec![], None).unwrap();
        }

        let results: HashMap<String, f32> =
            unweighted_page_rank(&graph, 1000, Some(4), None)
                .into_iter()
                .collect();

        let expected_2 = vec![
            ("10".to_string(), 0.07208286),
            ("11".to_string(), 0.061855234),
            ("5".to_string(), 0.19658245),
            ("4".to_string(), 0.07943771),
            ("9".to_string(), 0.061855234),
            ("3".to_string(), 0.15484008),
            ("8".to_string(), 0.136479),
            ("2".to_string(), 0.035566494),
            ("7".to_string(), 0.016384698),
            ("1".to_string(), 0.1128334),
            ("6".to_string(), 0.07208286),
        ];

        assert_eq!(
            results,
            expected_2.into_iter().collect::<HashMap<String, f32>>()
        );
    }

    #[test]
    fn two_nodes_page_rank() {
        let edges = vec![(1, 2), (2, 1)];

        let graph = Graph::new(4);

        for (t, (src, dst)) in edges.into_iter().enumerate() {
            graph.add_edge(t as i64, src, dst, &vec![], None).unwrap();
        }

        let results: HashMap<String, f32> =
            unweighted_page_rank(&graph, 1000, Some(4), None)
                .into_iter()
                .collect();

        assert_eq_f32(results.get("1"), Some(&0.5), 3);
        assert_eq_f32(results.get("2"), Some(&0.5), 3);
    }

    #[test]
    fn three_nodes_page_rank_one_dangling() {
        let edges = vec![(1, 2), (2, 1), (2, 3)];

        let graph = Graph::new(4);

        for (t, (src, dst)) in edges.into_iter().enumerate() {
            graph.add_edge(t as i64, src, dst, &vec![], None).unwrap();
        }

        let results: HashMap<String, f32> =
            unweighted_page_rank(&graph, 1000, Some(4), None)
                .into_iter()
                .collect();

        assert_eq_f32(results.get("1"), Some(&0.303), 3);
        assert_eq_f32(results.get("2"), Some(&0.394), 3);
        assert_eq_f32(results.get("3"), Some(&0.303), 3);
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

        let graph = Graph::new(4);

        for (src, dst, t) in edges {
            graph.add_edge(t, src, dst, &vec![], None).unwrap();
        }

        let results: HashMap<String, f32> =
            unweighted_page_rank(&graph, 1000, Some(4), None)
                .into_iter()
                .collect();

        assert_eq_f32(results.get("1"), Some(&0.055), 3);
        assert_eq_f32(results.get("2"), Some(&0.079), 3);
        assert_eq_f32(results.get("3"), Some(&0.113), 3);
        assert_eq_f32(results.get("4"), Some(&0.055), 3);
        assert_eq_f32(results.get("5"), Some(&0.070), 3);
        assert_eq_f32(results.get("6"), Some(&0.083), 3);
        assert_eq_f32(results.get("7"), Some(&0.093), 3);
        assert_eq_f32(results.get("8"), Some(&0.102), 3);
        assert_eq_f32(results.get("9"), Some(&0.110), 3);
        assert_eq_f32(results.get("10"), Some(&0.117), 3);
        assert_eq_f32(results.get("11"), Some(&0.122), 3);
    }

    fn assert_eq_f32<T: Borrow<f32> + PartialEq + std::fmt::Debug>(
        a: Option<T>,
        b: Option<T>,
        decimals: u8,
    ) {
        if a.is_none() || b.is_none() {
            assert_eq!(a, b);
        } else {
            let factor = 10.0_f32.powi(decimals as i32);
            match (a, b) {
                (Some(a), Some(b)) => {
                    assert_eq!(
                        (a.borrow() * factor).round(),
                        (b.borrow() * factor).round(),
                        "{:?} != {:?}",
                        a,
                        b
                    );
                }
                _ => unreachable!(),
            }
        }
    }
}
