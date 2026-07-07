use crate::{
    core::state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    db::{
        api::{
            state::{GenericNodeState, TypedNodeState},
            view::{EdgeViewOps, NodeViewOps, StaticGraphViewOps},
        },
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::{GraphViewOps, PropertiesOps},
};
use num_traits::abs;
use raphtory_api::core::entities::properties::prop::PropUnwrap;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct PageRankState {
    #[serde(rename = "pagerank_score")]
    pub score: f64,
    #[serde(skip)]
    weighted_out_degree: f64,
}

impl PageRankState {
    fn new(num_nodes: usize) -> Self {
        Self {
            score: 1f64 / num_nodes as f64,
            weighted_out_degree: 0f64,
        }
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
/// - `personalization`: Optional node property key to use as personalization weight.
///     When provided, the random walk teleports proportionally to these node property values
///     instead of uniformly. Values are normalized to sum to 1.
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
    personalization: Option<&str>,
) -> TypedNodeState<'static, PageRankState, G> {
    let damp = damping_factor.unwrap_or(0.85);
    let personalization_id = personalization.and_then(|key| g.node_meta().get_prop_id(key, false));
    let n = g.count_nodes();
    
        let mut ctx: Context<G, ComputeStateVec> = g.into();
    
        let tol: f64 = tol.unwrap_or(0.000001f64);
        let iter_count = iter_count.unwrap_or(20);
    
        let max_diff = accumulators::sum::<f64>(2);
        let total_sink_contribution = accumulators::sum::<f64>(4);
        let personalization_total = accumulators::sum::<f64>(5);
    
        ctx.global_agg_reset(max_diff);
        ctx.global_agg_reset(total_sink_contribution);
        if personalization_id.is_some() {
            ctx.global_agg(personalization_total);
        }
    
        let uniform_teleport_prob = (1f64 - damp) / n as f64;
        let uniform_sink_factor = damp / n as f64;
    
        let weight_id = weight.and_then(|key| g.edge_meta().get_prop_id(key, false));
    
        let personalization_total_task = personalization_id.map(|personalization_id| {
            let task: ATask<G, ComputeStateVec, PageRankState, _> =
                ATask::new(move |s: &mut EvalNodeView<_, PageRankState>| {
                    let value = s
                        .properties()
                        .get_by_id(personalization_id)
                        .and_then(|p| p.as_f64())
                        .unwrap_or(0.0);
                    s.global_update(&personalization_total, value);
                    Step::Continue
                });
            task
        });
    
        let step1: ATask<G, ComputeStateVec, PageRankState, _> = ATask::new({
            move |s: &mut EvalNodeView<_, PageRankState>| {
                let weighted_out_degree = s.out_edges().iter().fold(0.0f64, |acc, edge| {
                    weight_id
                        .and_then(|id| edge.properties().get_by_id(id))
                        .and_then(|p| p.as_f64())
                        .unwrap_or(1.0)
                        + acc
                });
                let state: &mut PageRankState = s.get_mut();
                state.weighted_out_degree = weighted_out_degree;
                Step::Continue
            }
        });
    
        let step2: ATask<G, ComputeStateVec, PageRankState, _> =
            ATask::new(move |s: &mut EvalNodeView<_, PageRankState>| {
                let mut score = 0.0f64;
    
                for edge in s.in_edges() {
                    let w = weight_id
                        .and_then(|id| edge.properties().get_by_id(id))
                        .and_then(|p| p.as_f64())
                        .unwrap_or(1.0);
                    let nbr = edge.nbr();
                    let prev: &PageRankState = nbr.prev();
    
                    if prev.weighted_out_degree > 0.0 {
                        score += prev.score * w / prev.weighted_out_degree;
                    }
                }
    
                score *= damp;
                score += match personalization_id {
                    Some(personalization_id) => {
                        // NOTE: Obviously `unwrap`/`context` is tempting fate,
                        // but this does seem both unrecoverable and very unlikely.
                        let total = s
                            .read_global_state(&personalization_total)
                            .expect("Computing personalized pagerank, but total personalization missing");
                        if total.abs() > f64::EPSILON {
                            let value = s
                                .properties()
                                .get_by_id(personalization_id)
                                .and_then(|p| p.as_f64())
                                .unwrap_or(0.0);
                            (1.0 - damp) * value / total
                        } else {
                            uniform_teleport_prob
                        }
                    }
                    None => uniform_teleport_prob,
                };
    
                s.get_mut().score = score;
                Step::Continue
            });
    
        let step3: ATask<G, ComputeStateVec, PageRankState, _> =
            ATask::new(move |s: &mut EvalNodeView<_, PageRankState>| {
                let state: &PageRankState = s.get();
    
                if state.weighted_out_degree.abs() < f64::EPSILON {
                    let curr = s.prev().score;
                    let sink_contribution = match personalization_id {
                        Some(_) => {
                            let total = s
                                .read_global_state(&personalization_total)
                                .expect("Computing personalized pagerank, but total personalization missing");
                            if total.abs() > f64::EPSILON {
                                curr
                            } else {
                                uniform_sink_factor * curr
                            }
                        }
                        None => uniform_sink_factor * curr,
                    };
    
                    s.global_update(&total_sink_contribution, sink_contribution);
                }
                Step::Continue
            });

        let step4: ATask<G, ComputeStateVec, PageRankState, _> =
            ATask::new(move |s: &mut EvalNodeView<_, PageRankState>| {
                let total_sink_contribution = s
                    .read_global_state(&total_sink_contribution)
                    .unwrap_or_default();
                let sink_addition = match personalization_id {
                    Some(personalization_id) => {
                        let personalization_total = s
                            .read_global_state(&personalization_total)
                            .expect("Computing personalized pagerank, but total personalization missing");
                        if personalization_total.abs() > 0.0 {
                            let value = s
                                .properties()
                                .get_by_id(personalization_id)
                                .and_then(|p| p.as_f64())
                                .unwrap_or(0.0);
                            damp * total_sink_contribution * value / personalization_total
                        } else {
                            total_sink_contribution
                        }
                    }
                    None => total_sink_contribution,
                };
    
                let prev = s.prev().score;
                let state: &mut PageRankState = s.get_mut();
                state.score += sink_addition;
                let curr = state.score;
    
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
        let mut init_tasks = Vec::new();
        if let Some(personalization_total_task) = personalization_total_task {
            init_tasks.push(Job::new(personalization_total_task));
        }
        init_tasks.push(Job::new(step1));
    
        runner.run(
            init_tasks,
            vec![Job::new(step2), Job::new(step3), Job::new(step4), step5],
            Some(vec![PageRankState::new(num_nodes); num_nodes]),
            |_, _, _, local, index| {
                TypedNodeState::new(GenericNodeState::new_from_eval_with_index(
                    g.clone(),
                    local,
                    index,
                    None,
                ))
            },
            threads,
            iter_count,
            None,
            None,
        )
}