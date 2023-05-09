use crate::db::view_api::VertexViewOps;
use crate::{
    core::{
        agg::InitOneF32,
        state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    },
    db::{
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner
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

    ctx.agg_reset(recv_score);
    ctx.global_agg_reset(max_diff);

    let step1 = ATask::new(move |vv| {
        let initial_score = 1f32 / total_vertices as f32;
        vv.update_local(&score, initial_score);
        Step::Continue
    });

    let step2 = ATask::new(move |s| {
        let out_degree = s.out_degree();
        if out_degree > 0 {
            let new_score = s.read_local(&score) / out_degree as f32;
            for t in s.neighbours_out() {
                t.update(&recv_score, new_score)
            }
        }
        Step::Continue
    });

    let step3 = ATask::new(move |s| {
        s.update_local(
            &score,
            (1f32 - damping_factor) + (damping_factor * s.read(&recv_score)),
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
        vec![Job::new(step1)],
        vec![Job::new(step2), Job::new(step3), step4],
        threads,
        iter_count,
        None,
        None,
    );

    let mut map: FxHashMap<String, f32> = FxHashMap::default();

    for state in local_states {
        if let Some(state) = state.as_ref() {
            state.fold_state_internal(
                runner.ctx.ss(),
                &mut map,
                &score,
                |res, shard, pid, score| {
                    if let Some(v_ref) = g.lookup_by_pid_and_shard(pid, shard) {
                        res.insert(g.vertex(v_ref.g_id).unwrap().name(), score);
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
                ("2".to_string(), 0.78044075),
                ("4".to_string(), 0.78044075),
                ("1".to_string(), 1.4930439),
                ("3".to_string(), 0.8092761)
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
            ("10".to_string(), 0.6598998),
            ("7".to_string(), 0.14999998),
            ("4".to_string(), 0.72722703),
            ("1".to_string(), 1.0329459),
            ("11".to_string(), 0.5662594),
            ("8".to_string(), 1.2494258),
            ("5".to_string(), 1.7996559),
            ("2".to_string(), 0.32559997),
            ("9".to_string(), 0.5662594),
            ("6".to_string(), 0.6598998),
            ("3".to_string(), 1.4175149),
        ];

        // let expected = vec![
        //     (1, 1.2411863819664029),
        //     (2, 0.39123721383779864),
        //     (3, 1.7032272385548306),
        //     (4, 0.873814473224871),
        //     (5, 2.162387978524525),
        //     (6, 0.7929037468922092),
        //     (8, 1.5012556698522248),
        //     (7, 0.1802324126887131),
        //     (9, 0.6804255687831074),
        //     (10, 0.7929037468922092),
        //     (11, 0.6804255687831074),
        // ];

        assert_eq!(
            results,
            expected_2.into_iter().collect::<FxHashMap<String, f32>>()
        );
    }
}
