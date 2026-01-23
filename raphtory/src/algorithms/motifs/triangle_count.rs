use crate::{
    algorithms::cores::k_core::k_core_set,
    core::{
        entities::VID,
        state::{accumulator_id::accumulators, compute_state::ComputeStateVec},
    },
    db::{
        api::view::*,
        graph::views::node_subgraph::NodeSubgraph,
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
};
use rustc_hash::FxHashSet;

/// Computes the number of triangles in a graph using a fast algorithm
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `window` - A range indicating the temporal window to consider
///
/// Returns:
///
/// An optional integer containing the number of triangles in the graph. If the computation failed,
/// the function returns `None`.
///
/// # Example
/// ```rust
/// use std::{cmp::Reverse, iter::once};
/// use raphtory::algorithms::motifs::triangle_count::triangle_count;
/// use raphtory::prelude::*;
///
/// let graph = Graph::new();
///
/// let edges = vec![
///     // triangle 1
///     (1, 2, 1),
///     (2, 3, 1),
///     (3, 1, 1),
///     //triangle 2
///     (4, 5, 1),
///     (5, 6, 1),
///     (6, 4, 1),
///     // triangle 4 and 5
///     (7, 8, 2),
///     (8, 9, 3),
///     (9, 7, 4),
///     (8, 10, 5),
///     (10, 9, 6),
/// ];
///
/// for (src, dst, ts) in edges {
///     graph.add_edge(ts, src, dst, NO_PROPS, None);
/// }
///
/// let actual_tri_count = triangle_count(&graph, None);
/// ```
///
pub fn triangle_count<G: StaticGraphViewOps>(graph: &G, threads: Option<usize>) -> usize {
    let node_set = k_core_set(graph, 2, usize::MAX, None);
    let g = graph.subgraph(node_set);
    let mut ctx: Context<NodeSubgraph<G>, ComputeStateVec> = Context::from(&g);

    #[derive(Clone, Debug, Default)]
    struct NborState {
        nbors: FxHashSet<VID>,
    }

    let count = accumulators::sum::<usize>(1);
    ctx.global_agg(count);

    let step1 = ATask::new(move |s: &mut EvalNodeView<_, NborState>| {
        let mut nbors = FxHashSet::default();
        for t in s.neighbours() {
            if s.node < t.node {
                nbors.insert(t.node);
            }
        }
        let state = s.get_mut();
        state.nbors = nbors;
        Step::Continue
    });

    let step2 = ATask::new(move |s: &mut EvalNodeView<_, NborState>| {
        let mut intersection_count = 0;
        let nbors = &s.get().nbors;
        for t in s.neighbours() {
            if s.node < t.node {
                intersection_count += nbors.intersection(&t.prev().nbors).count();
            }
        }
        s.global_update(&count, intersection_count);
        Step::Continue
    });

    let init_tasks = vec![Job::new(step1)];
    let tasks = vec![Job::new(step2)];

    let mut runner: TaskRunner<NodeSubgraph<G>, _> = TaskRunner::new(ctx);

    runner.run(
        init_tasks,
        tasks,
        None,
        |egs, _, _, _| egs.finalize(&count),
        threads,
        1,
        None,
        None,
    )
}
