use crate::algorithms::*;
use crate::core::agg::*;
use crate::core::state::accumulator_id::accumulators::val;
use crate::core::state::accumulator_id::accumulators::{max, sum};
use crate::{
    core::{agg::InitOneF32, state::compute_state::ComputeStateVec},
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

// HITS (Hubs and Authority) Algorithm:
// AuthScore of a vertex (A) = Sum of HubScore of all vertices pointing at vertex (A) from previous iteration /
//     Sum of HubScore of all vertices in the current iteration
//
// HubScore of a vertex (A) = Sum of AuthScore of all vertices pointing away from vertex (A) from previous iteration /
//     Sum of AuthScore of all vertices in the current iteration

#[allow(unused_variables)]
pub fn hits<G: GraphViewOps>(
    g: &G,
    window: Range<i64>,
    iter_count: usize,
    threads: Option<usize>,
) -> FxHashMap<String, (f32, f32)> {
    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let hub_score = val::<f32>(0).init::<InitOneF32>();
    let auth_score = val::<f32>(1).init::<InitOneF32>();

    let recv_hub_score = sum::<f32>(2);
    let recv_auth_score = sum::<f32>(3);

    let total_hub_score = sum::<f32>(4);
    let total_auth_score = sum::<f32>(5);

    let max_diff_hub_score = max::<f32>(6);
    let max_diff_auth_score = max::<f32>(7);

    ctx.agg(recv_hub_score);
    ctx.agg(recv_auth_score);
    ctx.agg_reset(recv_hub_score);
    ctx.agg_reset(recv_auth_score);
    ctx.global_agg(total_hub_score);
    ctx.global_agg(total_auth_score);
    ctx.global_agg(max_diff_hub_score);
    ctx.global_agg(max_diff_auth_score);
    ctx.global_agg_reset(total_hub_score);
    ctx.global_agg_reset(total_auth_score);
    ctx.global_agg_reset(max_diff_hub_score);
    ctx.global_agg_reset(max_diff_auth_score);

    let step1 = ATask::new(move |evv| {
        evv.update_local(&hub_score, InitOneF32::init());
        evv.update_local(&auth_score, InitOneF32::init());
        Step::Continue
    });

    let step2 = ATask::new(move |evv| {
        let hub_score = evv.read_local(&hub_score);
        let auth_score = evv.read_local(&auth_score);
        for t in evv.neighbours_out() {
            t.update(&recv_hub_score, hub_score)
        }
        for t in evv.neighbours_in() {
            t.update(&recv_auth_score, auth_score)
        }
        Step::Continue
    });

    let step3 = ATask::new(move |evv| {
        let recv_hub_score = evv.read(&recv_hub_score);
        let recv_auth_score = evv.read(&recv_auth_score);

        evv.global_update(&total_hub_score, recv_hub_score);
        evv.global_update(&total_auth_score, recv_auth_score);
        Step::Continue
    });

    let step4 = ATask::new(move |evv| {
        let recv_hub_score = evv.read(&recv_hub_score);
        let recv_auth_score = evv.read(&recv_auth_score);

        evv.update_local(
            &auth_score,
            recv_hub_score / evv.read_global_state(&total_hub_score).unwrap(),
        );
        evv.update_local(
            &hub_score,
            recv_auth_score / evv.read_global_state(&total_auth_score).unwrap(),
        );

        let prev_hub_score = evv.read_local_prev(&hub_score);
        let curr_hub_score = evv.read_local(&hub_score);
        let md_hub_score = abs((prev_hub_score - curr_hub_score));
        evv.global_update(&max_diff_hub_score, md_hub_score);

        let prev_auth_score = evv.read_local_prev(&auth_score);
        let curr_auth_score = evv.read_local(&auth_score);
        let md_auth_score = abs((prev_auth_score - curr_auth_score));
        evv.global_update(&max_diff_auth_score, md_auth_score);

        Step::Continue
    });

    let max_diff_hs = 0.01f32;
    let max_diff_as = max_diff_hs;

    let step5 = Job::Check(Box::new(move |state| {
        if state.read(&max_diff_hub_score) <= max_diff_hs
            && state.read(&max_diff_auth_score) <= max_diff_as
        {
            Step::Done
        } else {
            Step::Continue
        }
    }));

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let (shard_states, global_state, local_states) = runner.run(
        vec![Job::new(step1)],
        vec![Job::new(step2), Job::new(step3), Job::new(step4), step5],
        threads,
        iter_count,
        None,
        None,
    );

    let mut results: FxHashMap<String, (f32, f32)> = FxHashMap::default();

    for state in local_states {
        if let Some(state) = state.as_ref() {
            state.fold_state_internal(
                runner.ctx.ss(),
                &mut results,
                &hub_score,
                |res, shard, pid, hub_score| {
                    // println!("v0 = {}, taint_history0 = {:?}", pid, taint_history);
                    if let Some(v_ref) = g.lookup_by_pid_and_shard(pid, shard) {
                        res.insert(g.vertex(v_ref.g_id).unwrap().name(), (hub_score, 0.0f32));
                    }
                    res
                },
            );
            state.fold_state_internal(
                runner.ctx.ss(),
                &mut results,
                &auth_score,
                |res, shard, pid, auth_score| {
                    // println!("v0 = {}, taint_history0 = {:?}", pid, taint_history);
                    if let Some(v_ref) = g.lookup_by_pid_and_shard(pid, shard) {
                        let (a, _) = res.get(&g.vertex(v_ref.g_id).unwrap().name()).unwrap();
                        res.insert(g.vertex(v_ref.g_id).unwrap().name(), (*a, auth_score));
                    }
                    res
                },
            );
        }
    }

    results
}

#[cfg(test)]
mod hits_tests {
    use super::*;
    use crate::db::graph::Graph;
    use itertools::Itertools;

    fn load_graph(n_shards: usize, edges: Vec<(u64, u64)>) -> Graph {
        let graph = Graph::new(n_shards);

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, &vec![], None).unwrap();
        }
        graph
    }

    fn test_hits(n_shards: usize) {
        let graph = load_graph(
            n_shards,
            vec![
                (1, 4),
                (2, 3),
                (2, 5),
                (3, 1),
                (4, 2),
                (4, 3),
                (5, 2),
                (5, 3),
                (5, 4),
                (5, 6),
                (6, 3),
                (6, 8),
                (7, 1),
                (7, 3),
                (8, 1),
            ],
        );

        let window = 0..10;

        let mut results: Vec<(String, (f32, f32))> =
            hits(&graph, window, 20, None).into_iter().collect_vec();

        results.sort_by_key(|k| (*k).0.clone());

        // NetworkX results
        // >>> G = nx.DiGraph()
        // >>> G.add_edge(1, 4)
        // >>> G.add_edge(2, 3)
        // >>> G.add_edge(2, 5)
        // >>> G.add_edge(3, 1)
        // >>> G.add_edge(4, 2)
        // >>> G.add_edge(4, 3)
        // >>> G.add_edge(5, 2)
        // >>> G.add_edge(5, 3)
        // >>> G.add_edge(5, 4)
        // >>> G.add_edge(5, 6)
        // >>> G.add_edge(6,3)
        // >>> G.add_edge(6,8)
        // >>> G.add_edge(7,1)
        // >>> G.add_edge(7,3)
        // >>> G.add_edge(8,1)
        // >>> nx.hits(G)
        // (
        //     (1, (0.04305010876408988, 0.08751958702900825)),
        //     (2, (0.14444089276992705, 0.18704574169397797)),
        //     (3, (0.02950848945012511, 0.3690360954887363)),
        //     (4, (0.1874910015340169, 0.12768284011810235)),
        //     (5, (0.26762580040598083, 0.05936290157587567)),
        //     (6, (0.144440892769927, 0.10998993251842377)),
        //     (7, (0.15393432485580819, 5.645162243895331e-17))
        //     (8, (0.02950848945012511, 0.05936290157587556)),
        // )

        assert_eq!(
            results,
            vec![
                ("1".to_string(), (0.0431365, 0.096625775)),
                ("2".to_string(), (0.14359662, 0.18366566)),
                ("3".to_string(), (0.030866561, 0.36886504)),
                ("4".to_string(), (0.1865414, 0.12442485)),
                ("5".to_string(), (0.26667944, 0.05943252)),
                ("6".to_string(), (0.14359662, 0.10755368)),
                ("7".to_string(), (0.15471625, 0.0)),
                ("8".to_string(), (0.030866561, 0.05943252))
            ]
        );
    }

    #[test]
    fn test_hits_11() {
        test_hits(1);
    }
}
