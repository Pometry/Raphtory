use crate::{
    core::state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    db::{
        api::{
            state::{GenericNodeState, TypedNodeState},
            view::{EdgeViewOps, NodeViewOps, StaticGraphViewOps, filter_ops::NodeSelect},
        }, graph::views::node_subgraph::NodeSubgraph, task::{
            context::Context,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        }
    },
    prelude::{GraphViewOps, PropertiesOps},
};
use num_traits::abs;
use raphtory_api::core::entities::properties::prop::PropUnwrap;
use serde::{Deserialize, Serialize};
use crate::prelude::NodeFilter;
use crate::db::graph::views::filter::model::degree_filter::DegreeFilterFactory;
use crate::db::graph::views::filter::model::property_filter::ops::PropertyFilterOps;
use std::collections::HashMap;
use raphtory_api::core::entities::VID;

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct PageRankState {
    #[serde(rename = "pagerank_score")]
    pub score: f64,
    #[serde(skip)]
    weighted_out_degree: f64,
    special_neighbor_weight: f64,
    special_node_score: f64,
    special_node_sink_contributor_count: usize,
}

impl PageRankState {
    fn new(num_nodes: usize) -> Self {
        Self {
            score: 1f64 / num_nodes as f64,
            special_node_score: 1f64 / num_nodes as f64,
            weighted_out_degree: 0f64,
            special_neighbor_weight: 0.0,
            special_node_sink_contributor_count: 0,
        }
    }

    fn reset(&mut self) {
        self.score = 0f64;
        self.special_node_score = 0f64;
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
) -> TypedNodeState<'static, PageRankState, NodeSubgraph<G>> {
    let n = g.count_nodes();
    let not_special_neighbors = g.nodes().select(NodeFilter.in_degree().eq(0)).unwrap();
    let f_g = g.subgraph(not_special_neighbors); 

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
    
    let weight_id = weight.and_then(|key| g.edge_meta().get_prop_id(key, false));

    let mut special_node_sink_contributor_count = 0;
    let mut special_node_out_degrees: HashMap<VID, f64> = HashMap::new();
    for node in g.nodes() {
        if node.in_degree() == 0 {
            let weighted_out_degree = node.out_edges().iter().fold(0.0f64, |acc, edge| {
                weight_id
                    .and_then(|id| edge.properties().get_by_id(id))
                    .and_then(|p| p.as_f64())
                    .unwrap_or(1.0)
                    + acc
            });
            if (weighted_out_degree.abs() < f64::EPSILON) {
                special_node_sink_contributor_count += 1;
            }
            special_node_out_degrees.insert(node.node, weighted_out_degree);
        }
    }


    let step1 = ATask::new({
        move |s| {
            let s_node = g.node(&s.node).unwrap();
            let weighted_out_degree = s_node.out_edges().iter().fold(0.0f64, |acc, edge| {
                weight_id
                    .and_then(|id| edge.properties().get_by_id(id))
                    .and_then(|p| p.as_f64())
                    .unwrap_or(1.0)
                    + acc
            });
            let state: &mut PageRankState = s.get_mut();
            state.weighted_out_degree = weighted_out_degree;
            let special_neighbor_weight = s_node.in_edges().iter().fold(0.0f64, |acc, edge| {
                let nbr = edge.nbr();
                if let Some(&weighted_out_degree) = special_node_out_degrees.get(&nbr.node) {
                    if weighted_out_degree > 0.0 {
                        let w = weight_id
                            .and_then(|id| edge.properties().get_by_id(id))
                            .and_then(|p| p.as_f64())
                            .unwrap_or(1.0);
                        acc + w / weighted_out_degree
                    } else {
                        acc
                    }
                } else {
                    acc
                }
            });
            state.special_neighbor_weight = special_neighbor_weight;
            Step::Continue
        }
    });

    let step2: ATask<G, ComputeStateVec, PageRankState, _> = ATask::new(move |s| {
        // reset score
        {
            let state: &mut PageRankState = s.get_mut();
            state.reset();
        }
        
        let special_node_score = s.prev().special_node_score; 

        for edge in s.in_edges() {
            let w = weight_id
                .and_then(|id| edge.properties().get_by_id(id))
                .and_then(|p| p.as_f64())
                .unwrap_or(1.0);
            let nbr = edge.nbr();
            let prev = nbr.prev();

            if prev.weighted_out_degree > 0.0 {
                s.get_mut().score += prev.score * w / prev.weighted_out_degree;
            }
        }
        s.get_mut().score += s.prev().special_neighbor_weight * special_node_score;  

        s.get_mut().score *= damp;

        s.get_mut().score += teleport_prob;

        s.get_mut().special_node_score *= damp;

        s.get_mut().special_node_score += teleport_prob;
        Step::Continue
    });

    let step3 = ATask::new(move |s| {
        let state: &mut PageRankState = s.get_mut();

        if state.weighted_out_degree.abs() < f64::EPSILON {
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
        let total_sink_contribution = total_sink_contribution + s.prev().special_node_sink_contributor_count as f64 * factor * s.prev().special_node_score; 
        let state: &mut PageRankState = s.get_mut();
        state.score += total_sink_contribution;
        state.special_node_score += total_sink_contribution;

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
            TypedNodeState::new(GenericNodeState::new_from_eval_with_index(
                f_g.clone(),
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
