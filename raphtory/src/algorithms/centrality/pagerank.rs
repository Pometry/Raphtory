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
use raphtory_api::core::entities::VID;
use std::collections::HashMap;
use std::sync::Arc;

trait Teleport: Clone + Send + Sync + 'static {
    fn teleport_value(&self, vid_index: usize, damp: f64) -> f64;
    fn sink_contribution(&self, prev_score: f64) -> f64;
    fn distribute_sink(&self, total_sink: f64, vid_index: usize, damp: f64) -> f64;
}

#[derive(Clone)]
struct Uniform {
    teleport_prob: f64,
    factor: f64,
}

impl Teleport for Uniform {
    #[inline]
    fn teleport_value(&self, _vid_index: usize, _damp: f64) -> f64 {
        self.teleport_prob
    }
    #[inline]
    fn sink_contribution(&self, prev_score: f64) -> f64 {
        self.factor * prev_score
    }
    #[inline]
    fn distribute_sink(&self, total_sink: f64, _vid_index: usize, _damp: f64) -> f64 {
        total_sink
    }
}

#[derive(Clone)]
struct Personalized {
    weights: Arc<Vec<f64>>,
}

impl Teleport for Personalized {
    #[inline]
    fn teleport_value(&self, vid_index: usize, damp: f64) -> f64 {
        (1.0 - damp) * self.weights[vid_index]
    }
    #[inline]
    fn sink_contribution(&self, prev_score: f64) -> f64 {
        prev_score
    }
    #[inline]
    fn distribute_sink(&self, total_sink: f64, vid_index: usize, damp: f64) -> f64 {
        damp * total_sink * self.weights[vid_index]
    }
}

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
/// - `personalization`: Optional map from node VID to personalization weight.
///     When provided, the random walk teleports proportionally to these weights
///     instead of uniformly. Values are normalized to sum to 1.
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
    personalization: Option<HashMap<VID, f64>>,
) -> NodeState<'static, f64, G> {
    let n = g.count_nodes();
    let damp = damping_factor.unwrap_or(0.85);

    match personalization {
        Some(p) => {
            let total: f64 = p.values().sum();
            let mut weights = vec![0.0f64; n];
            for (&vid, &value) in &p {
                weights[vid.index()] = value / total;
            }
            run_pagerank(
                g,
                iter_count,
                threads,
                tol,
                use_l2_norm,
                damp,
                Personalized { weights: Arc::new(weights) },
            )
        }
        None => run_pagerank(
            g,
            iter_count,
            threads,
            tol,
            use_l2_norm,
            damp,
            Uniform {
                teleport_prob: (1f64 - damp) / n as f64,
                factor: damp / n as f64,
            },
        ),
    }
}

fn run_pagerank<G: StaticGraphViewOps, T: Teleport>(
    g: &G,
    iter_count: Option<usize>,
    threads: Option<usize>,
    tol: Option<f64>,
    use_l2_norm: bool,
    damp: f64,
    teleport: T,
) -> NodeState<'static, f64, G> {
    let n = g.count_nodes();

    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let tol: f64 = tol.unwrap_or(0.000001f64);
    let iter_count = iter_count.unwrap_or(20);

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

    let step2: ATask<G, ComputeStateVec, PageRankState, _> = ATask::new({
        let teleport = teleport.clone();
        move |s| {
            {
                let state: &mut PageRankState = s.get_mut();
                state.reset();
            }

            for t in s.in_neighbours() {
                let prev = t.prev();

                s.get_mut().score += prev.score / prev.out_degree as f64;
            }

            s.get_mut().score *= damp;

            s.get_mut().score += teleport.teleport_value(s.node.index(), damp);
            Step::Continue
        }
    });

    let step3 = ATask::new({
        let teleport = teleport.clone();
        move |s| {
            let state: &mut PageRankState = s.get_mut();

            if state.out_degree == 0 {
                let curr = s.prev().score;

                s.global_update(
                    &total_sink_contribution,
                    teleport.sink_contribution(curr),
                );
            }
            Step::Continue
        }
    });

    let step4 = ATask::new(move |s| {
        //read total sink contribution
        let total_sink_contribution = s
            .read_global_state(&total_sink_contribution)
            .unwrap_or_default();
        // update local score with total sink contribution
        let vid_index = s.node.index();
        let state: &mut PageRankState = s.get_mut();
        state.score +=
            teleport.distribute_sink(total_sink_contribution, vid_index, damp);

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
