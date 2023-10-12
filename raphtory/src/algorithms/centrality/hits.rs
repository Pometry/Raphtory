use crate::{
    algorithms::algorithm_result_new::AlgorithmResultNew,
    core::{
        entities::vertices::vertex_ref::VertexRef,
        state::{
            accumulator_id::accumulators::{max, sum},
            compute_state::ComputeStateVec,
        },
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
use num_traits::abs;
use ordered_float::OrderedFloat;
use std::collections::HashMap;

#[derive(Debug, Clone)]
struct Hits {
    hub_score: f32,
    auth_score: f32,
}

impl Default for Hits {
    fn default() -> Self {
        Self {
            hub_score: 1f32,
            auth_score: 1f32,
        }
    }
}

/// HITS (Hubs and Authority) Algorithm:
/// AuthScore of a vertex (A) = Sum of HubScore of all vertices pointing at vertex (A) from previous iteration /
///     Sum of HubScore of all vertices in the current iteration
///
/// HubScore of a vertex (A) = Sum of AuthScore of all vertices pointing away from vertex (A) from previous iteration /
///     Sum of AuthScore of all vertices in the current iteration
///
/// Returns
///
/// * An AlgorithmResult object containing the mapping from vertex ID to the hub and authority score of the vertex
#[allow(unused_variables)]
pub fn hits<G: GraphViewOps>(
    g: &G,
    iter_count: usize,
    threads: Option<usize>,
) -> AlgorithmResultNew<G, (f32, f32), (OrderedFloat<f32>, OrderedFloat<f32>)> {
    let mut ctx: Context<G, ComputeStateVec> = g.into();

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

    let step2 = ATask::new(move |evv: &mut EvalVertexView<G, ComputeStateVec, Hits>| {
        let hub_score = evv.get().hub_score;
        let auth_score = evv.get().auth_score;
        for t in evv.out_neighbours() {
            t.update(&recv_hub_score, hub_score)
        }
        for t in evv.in_neighbours() {
            t.update(&recv_auth_score, auth_score)
        }
        Step::Continue
    });

    let step3 = ATask::new(move |evv: &mut EvalVertexView<G, ComputeStateVec, Hits>| {
        let recv_hub_score = evv.read(&recv_hub_score);
        let recv_auth_score = evv.read(&recv_auth_score);

        evv.global_update(&total_hub_score, recv_hub_score);
        evv.global_update(&total_auth_score, recv_auth_score);
        Step::Continue
    });

    let step4 = ATask::new(move |evv: &mut EvalVertexView<G, ComputeStateVec, Hits>| {
        let recv_hub_score = evv.read(&recv_hub_score);
        let recv_auth_score = evv.read(&recv_auth_score);

        evv.get_mut().auth_score =
            recv_hub_score / evv.read_global_state(&total_hub_score).unwrap();
        evv.get_mut().hub_score =
            recv_auth_score / evv.read_global_state(&total_auth_score).unwrap();

        let prev_hub_score = evv.prev().hub_score;
        let curr_hub_score = evv.get().hub_score;

        let md_hub_score = abs(prev_hub_score - curr_hub_score);
        evv.global_update(&max_diff_hub_score, md_hub_score);

        let prev_auth_score = evv.prev().auth_score;
        let curr_auth_score = evv.get().auth_score;
        let md_auth_score = abs(prev_auth_score - curr_auth_score);
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

    let (hub_scores, auth_scores) = runner.run(
        vec![],
        vec![Job::new(step2), Job::new(step3), Job::new(step4), step5],
        None,
        |_, _, els, local| {
            let mut hubs = HashMap::new();
            let mut auths = HashMap::new();
            let layers = g.layer_ids();
            let edge_filter = g.edge_filter();
            for (v_ref, hit) in local.iter().enumerate() {
                if g.has_vertex_ref(VertexRef::Internal(v_ref.into()), &layers, edge_filter) {
                    let v_gid = g.vertex_name(v_ref.into());
                    hubs.insert(v_gid.clone(), hit.hub_score);
                    auths.insert(v_gid, hit.auth_score);
                }
            }
            (hubs, auths)
        },
        threads,
        iter_count,
        None,
        None,
    );

    let mut results: HashMap<usize, (f32, f32)> = HashMap::new();

    hub_scores.into_iter().for_each(|(k, v)| {
        results.insert(g.vertex(k).unwrap().vertex.0, (v, 0.0));
    });

    auth_scores.into_iter().for_each(|(k, v)| {
        let vid = g.vertex(k).unwrap().vertex.0;
        let (a, _) = results.get(&vid).unwrap();
        results.insert(vid, (*a, v));
    });

    let results_type = std::any::type_name::<(f32, f32)>();

    AlgorithmResultNew::new(g.clone(), "Hits", results_type, results)
}

#[cfg(test)]
mod hits_tests {
    use super::*;
    use crate::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };

    fn load_graph(edges: Vec<(u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    #[test]
    fn test_hits() {
        let graph = load_graph(vec![
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
        ]);

        let results = hits(&graph, 20, None);

        assert_eq!(
            results.sort_by_vertex_id(false),
            vec![
                ("1".to_string(), Some((0.0431365, 0.096625775))),
                ("2".to_string(), Some((0.14359662, 0.18366566))),
                ("3".to_string(), Some((0.030866561, 0.36886504))),
                ("4".to_string(), Some((0.1865414, 0.12442485))),
                ("5".to_string(), Some((0.26667944, 0.05943252))),
                ("6".to_string(), Some((0.14359662, 0.10755368))),
                ("7".to_string(), Some((0.15471625, 0.0))),
                ("8".to_string(), Some((0.030866561, 0.05943252)))
            ]
        );
    }
}
