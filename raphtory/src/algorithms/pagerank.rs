use crate::{
    core::{
        agg::InitOneF32,
        state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    },
    db::{
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
        view_api::{GraphViewOps, VertexViewOps},
    },
};
use num_traits::abs;
use rustc_hash::FxHashMap;

#[allow(unused_variables)]
pub fn unweighted_page_rank<G: GraphViewOps>(
    g: &G,
    iter_count: usize,
    threads: Option<usize>,
    tol: Option<f32>,
) -> FxHashMap<String, f32> {
    let total_vertices = g.num_vertices();

    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let max_diff_val: f32 = tol.unwrap_or_else(|| 0.01f32);
    let damping_factor = 0.85;

    let score = accumulators::val::<f32>(0).init::<InitOneF32>();
    let recv_score = accumulators::sum::<f32>(1);
    let max_diff = accumulators::max::<f32>(2);
    let dangling = accumulators::sum::<f32>(3);

    ctx.agg_reset(recv_score);
    ctx.global_agg_reset(max_diff);
    ctx.global_agg_reset(dangling);

    // let step1 = ATask::new(move |s| {
    //     if s.out_degree() == 0{
    //         s.global_update(&dangling, 1.0f32 / total_vertices as f32);
    //     }

    //     Step::Continue
    // });

    let step2 = ATask::new(move |s| {
        let out_degree = s.out_degree();
        if out_degree > 0 {
            let new_score = s.read_local(&score) / out_degree as f32;
            for t in s.neighbours_out() {
                t.update(&recv_score, new_score)
            }
        } else {
            s.global_update(&dangling, s.read_local(&score) / total_vertices as f32);
        }
        Step::Continue
    });

    let step3 = ATask::new(move |s| {

        let dangling_v = s.read_global_state(&dangling).unwrap_or_default();

        s.update_local(
            &score,
            (1f32 - damping_factor) + (damping_factor * ( s.read(&recv_score) + dangling_v )),
        );
        let prev = s.read_local_prev(&score);
        let curr = s.read_local(&score);

        let md = abs(prev - curr);
        s.global_update(&max_diff, md);
        Step::Continue
    });

    let step4 = Job::Check(Box::new(move |state| {
        if state.read(&max_diff) > max_diff_val {
            Step::Continue
        } else {
            Step::Done
        }
    }));

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let (_, _, local_states) = runner.run(
        vec![],
        vec![Job::new(step2), Job::new(step3), step4],
        threads,
        iter_count,
        None,
        None,
    );

    let mut map: FxHashMap<String, f32> = FxHashMap::default();

    let num_vertices = g.num_vertices() as f32;

    for state in local_states {
        if let Some(state) = state.as_ref() {
            state.fold_state_internal(
                runner.ctx.ss(),
                &mut map,
                &score,
                |res, shard, pid, score| {
                    if let Some(v_ref) = g.lookup_by_pid_and_shard(pid, shard) {
                        res.insert(g.vertex(v_ref.g_id).unwrap().name(), score / num_vertices);
                    }
                    res
                },
            );
        }
    }

    map
}

#[cfg(test)]
mod page_rank_tests {
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

        let results: FxHashMap<String, f32> = unweighted_page_rank(&graph, 25, Some(1), None)
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
            .collect::<FxHashMap<String, f32>>()
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

        let results: FxHashMap<String, f32> =
            unweighted_page_rank(&graph, 1000, Some(4), Some(0.00001))
                .into_iter()
                .collect();

        let expected_2 = vec![
            ("10".to_string(), 0.05999366),
            ("11".to_string(), 0.05147976),
            ("5".to_string(), 0.16361322),
            ("4".to_string(), 0.06611458),
            ("9".to_string(), 0.05147976),
            ("3".to_string(), 0.12887157),
            ("8".to_string(), 0.11358989),
            ("2".to_string(), 0.029600797),
            ("7".to_string(), 0.013636362),
            ("1".to_string(), 0.09390808),
            ("6".to_string(), 0.05999366),
        ];

        assert_eq!(
            results,
            expected_2.into_iter().collect::<FxHashMap<String, f32>>()
        );
    }

    #[test]
    fn two_nodes_page_rank() {
        let edges = vec![(1, 2), (2, 1)];

        let graph = Graph::new(4);

        for (t, (src, dst)) in edges.into_iter().enumerate() {
            graph.add_edge(t as i64, src, dst, &vec![], None).unwrap();
        }

        let results: FxHashMap<String, f32> =
            unweighted_page_rank(&graph, 1000, Some(4), Some(0.00001))
                .into_iter()
                .collect();

        assert_eq!(
            results,
            vec![("1".to_string(), 0.5), ("2".to_string(), 0.5)]
                .into_iter()
                .collect::<FxHashMap<String, f32>>()
        );
    }


    #[test]
    fn three_nodes_page_rank_one_dangling() {
        let edges = vec![(1, 2), (2, 1), (2, 3)];

        let graph = Graph::new(4);

        for (t, (src, dst)) in edges.into_iter().enumerate() {
            graph.add_edge(t as i64, src, dst, &vec![], None).unwrap();
        }

        let results: FxHashMap<String, f32> =
            unweighted_page_rank(&graph, 1000, Some(4), Some(0.0000001))
                .into_iter()
                .collect();

        assert_eq!(
            results,
            vec![("1".to_string(), 0.3032041), ("2".to_string(), 0.39363384), ("3".to_string(), 0.3032041)]
                .into_iter()
                .collect::<FxHashMap<String, f32>>()
        );
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

        let results: FxHashMap<String, f32> =
            unweighted_page_rank(&graph, 1000, Some(4), Some(0.00001))
                .into_iter()
                .collect();

        let expected = vec![
            ("10".to_string(), 0.06892874),
            ("11".to_string(), 0.07222628),
            ("5".to_string(), 0.041368324),
            ("4".to_string(), 0.032625638),
            ("9".to_string(), 0.06504937),
            ("3".to_string(), 0.06702048),
            ("8".to_string(), 0.060485493),
            ("2".to_string(), 0.046491623),
            ("7".to_string(), 0.055116292),
            ("1".to_string(), 0.032625638),
            ("6".to_string(), 0.048799634),
        ];

        assert_eq!(
            results,
            expected.into_iter().collect::<FxHashMap<String, f32>>()
        );
    }
}
