use crate::{
    core::state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    db::{
        api::{
            state::NodeState,
            view::{EdgeViewOps, NodeViewOps, StaticGraphViewOps},
        },
        task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::{GraphViewOps, PropertiesOps},
};
use num_traits::abs;
use raphtory_api::core::entities::properties::prop::PropUnwrap;

#[derive(Clone, Debug, Default)]
struct PageRankState {
    score: f64,
    weighted_out_degree: f64,
}

impl PageRankState {
    fn new(num_nodes: usize) -> Self {
        Self {
            score: 1f64 / num_nodes as f64,
            weighted_out_degree: 0f64,
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
/// - `weight`: Edge property key to use as weight. If None, all edges have weight 1.0.
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
pub fn page_rank<G: StaticGraphViewOps>(
    g: &G,
    weight: Option<&str>,
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

    let weight_key: Option<String> = weight.map(|s| s.to_string());

    let step1 = ATask::new({
        let weight_key = weight_key.clone();
        move |s| {
            let weighted_out_degree = s.out_edges().iter().fold(0.0f64, |acc, edge| {
                weight_key
                    .as_ref()
                    .and_then(|key| edge.properties().get(key))
                    .and_then(|p| p.as_f64())
                    .unwrap_or(1.0)
                    + acc
            });
            let state: &mut PageRankState = s.get_mut();
            state.weighted_out_degree = weighted_out_degree;
            Step::Continue
        }
    });

    let step2: ATask<G, ComputeStateVec, PageRankState, _> = ATask::new(move |s| {
        {
            let state: &mut PageRankState = s.get_mut();
            state.reset();
        }

        for edge in s.in_edges() {
            let w = weight_key
                .as_ref()
                .and_then(|key| edge.properties().get(key))
                .and_then(|p| p.as_f64())
                .unwrap_or(1.0);
            let nbr = edge.nbr();
            let prev = nbr.prev();
            if prev.weighted_out_degree > 0.0 {
                s.get_mut().score += prev.score * w / prev.weighted_out_degree;
            }
        }

        s.get_mut().score *= damp;
        s.get_mut().score += teleport_prob;
        Step::Continue
    });

    let step3 = ATask::new(move |s| {
        let state: &mut PageRankState = s.get_mut();
        if state.weighted_out_degree == 0.0 {
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
        |_, _, _, local, index| {
            NodeState::new_from_eval_mapped_with_index(g.clone(), local, index, |v| v.score)
        },
        threads,
        iter_count,
        None,
        None,
    )
}
