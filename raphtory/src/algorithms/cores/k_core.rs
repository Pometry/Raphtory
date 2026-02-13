use crate::{
    core::{entities::VID, state::compute_state::ComputeStateVec},
    db::{
        api::view::{NodeViewOps, StaticGraphViewOps},
        graph::views::node_subgraph::NodeSubgraph,
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::GraphViewOps,
};
use std::collections::HashSet;

#[derive(Clone, Debug)]
struct KCoreState {
    alive: bool,
}

impl Default for KCoreState {
    fn default() -> Self {
        Self { alive: true }
    }
}

/// Determines which nodes are in the k-core for a given value of k
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `k` - Value of k such that the returned nodes have degree > k (recursively)
/// - `iter_count` - The number of iterations to run
/// - `threads` - number of threads to run on
///
/// # Returns
///
/// A hash set of nodes in the k core
///
pub fn k_core_set<G>(g: &G, k: usize, iter_count: usize, threads: Option<usize>) -> HashSet<VID>
where
    G: StaticGraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = g.into();

    let step1 = ATask::new(move |vv| {
        let deg = vv.degree();
        let state: &mut KCoreState = vv.get_mut();
        state.alive = deg >= k;
        Step::Continue
    });

    let step2 = ATask::new(move |vv: &mut EvalNodeView<_, KCoreState>| {
        let prev: bool = vv.prev().alive;
        if prev {
            let current = vv
                .neighbours()
                .into_iter()
                .filter(|n| n.prev().alive)
                .count()
                >= k;
            let state: &mut KCoreState = vv.get_mut();
            if current != prev {
                state.alive = current;
                Step::Continue
            } else {
                Step::Done
            }
        } else {
            Step::Done
        }
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(step1)],
        vec![Job::read_only(step2)],
        None,
        |_, _, _, local, index| {
            g.nodes()
                .iter()
                .filter(|node| local[index.index(&node.node).unwrap()].alive)
                .map(|node| node.node)
                .collect()
        },
        threads,
        iter_count,
        None,
        None,
    )
}

pub fn k_core<G>(g: &G, k: usize, iter_count: usize, threads: Option<usize>) -> NodeSubgraph<G>
where
    G: StaticGraphViewOps,
{
    let v_set = k_core_set(g, k, iter_count, threads);
    g.subgraph(v_set)
}
