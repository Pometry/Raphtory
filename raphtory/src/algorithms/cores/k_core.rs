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

    let step2 = ATask::new(move |vv: &mut EvalNodeView<G, KCoreState>| {
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
        |_, _, _, local| {
            g.nodes()
                .iter()
                .filter(|node| local[node.node.0].alive)
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

#[cfg(test)]
mod k_core_test {
    use crate::{algorithms::cores::k_core::k_core_set, prelude::*, test_storage};
    use std::collections::HashSet;

    #[test]
    fn k_core_2() {
        let graph = Graph::new();

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

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let result = k_core_set(graph, 2, usize::MAX, None);
            let subgraph = graph.subgraph(result.clone());
            let actual = vec!["1", "3", "4", "5", "6", "8", "9", "10", "11"]
                .into_iter()
                .map(|k| k.to_string())
                .collect::<HashSet<String>>();

            assert_eq!(actual, subgraph.nodes().name().collect::<HashSet<String>>());
        });
    }
}
