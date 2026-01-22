use crate::{
    core::state::compute_state::ComputeStateVec,
    db::{
        api::{
            state::NodeState,
            view::{graph::GraphViewOps, NodeViewOps, StaticGraphViewOps},
        },
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
};
use rand::prelude::*;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
struct FastRPState {
    embedding_state: Vec<f64>,
}

/// Computes the embeddings of each vertex of a graph using the Fast RP algorithm
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `embedding_dim` - The size of the generated embeddings
/// - `normalization_strength` - The extent to which high-degree vertices should be discounted (range: 1-0)
/// - `iter_weights` - The scalar weights to apply to the results of each iteration
/// - `seed` - The seed for initialisation of random vectors
/// - `threads` - Number of threads to use
///
/// # Returns
///
/// An [AlgorithmResult] containing the mapping from the node to its embedding
///
pub fn fast_rp<G>(
    g: &G,
    embedding_dim: usize,
    normalization_strength: f64,
    iter_weights: Vec<f64>,
    seed: Option<u64>,
    threads: Option<usize>,
) -> NodeState<'static, Vec<f64>, G>
where
    G: StaticGraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = g.into();
    let m = g.count_nodes() as f64;
    let s = m.sqrt();
    let beta = normalization_strength - 1.0;
    let num_iters = iter_weights.len() - 1;
    let weights = Arc::new(iter_weights);
    let seed = seed.unwrap_or(rand::rng().random());

    // initialize each vertex with a random vector according to FastRP's construction rules
    let step1 = {
        let weights = Arc::clone(&weights);
        ATask::new(move |vv| {
            let l = ((vv.degree() as f64) / (m * 2.0)).powf(beta);
            let choices = [
                (l * s.sqrt(), 1.0 / (s * 2.0)),
                (0.0, 1.0 - (1.0 / s)),
                (-l * s.sqrt(), 1.0 / (s * 2.0)),
            ];
            let mut rng = SmallRng::seed_from_u64(vv.node.0 as u64 ^ seed);
            let state: &mut FastRPState = vv.get_mut();
            state.embedding_state = (0..embedding_dim)
                .map(|_| choices.choose_weighted(&mut rng, |item| item.1).unwrap().0 * weights[0])
                .collect();
            Step::Continue
        })
    };

    // sum each vector from neighbours and scale
    let step2 = ATask::new(move |vv: &mut EvalNodeView<_, FastRPState>| {
        // for neighbor, for i, add neighbors.prev[i] to current state
        // scale state by iteration weight
        let weights = Arc::clone(&weights);
        let denom: f64 =
            weights[vv.graph().ss] / ((vv.neighbours().iter().count() * (num_iters + 1)) as f64);
        for neighbour in vv.neighbours() {
            for i in 0..embedding_dim {
                // make sure contributions from neighbours are averaged and scaled correctly
                vv.get_mut().embedding_state[i] += neighbour.prev().embedding_state[i] * denom;
            }
        }

        Step::Continue
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(step1)],
        vec![Job::read_only(step2)],
        None,
        |_, _, _, local: Vec<FastRPState>, index| {
            NodeState::new_from_eval_mapped_with_index(g.clone(), local, index, |v| {
                v.embedding_state
            })
        },
        threads,
        num_iters,
        None,
        None,
    )
}
