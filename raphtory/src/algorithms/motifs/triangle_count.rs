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
use hashbrown::HashSet;
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

    // let mut ctx: Context<G, ComputeStateVec> = graph.into();
    //let neighbours_set = accumulators::hash_set::<VID>(0);
    let count = accumulators::sum::<usize>(1);

    //ctx.agg(neighbours_set);
    ctx.global_agg(count);

    let step1 = ATask::new(move |s: &mut EvalNodeView<NodeSubgraph<G>, NborState>| {
        let mut nbors = FxHashSet::default();
        for t in s.neighbours() {
            if s.node < t.node {
                //t.update(&neighbours_set, s.node);
                nbors.insert(t.node);
            }
        }
        let state = s.get_mut();
        state.nbors = nbors;
        Step::Continue
    });

    let step2 = ATask::new(move |s: &mut EvalNodeView<NodeSubgraph<G>, NborState>| {
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

#[cfg(test)]
mod triangle_count_tests {
    use std::time::Instant;

    use super::*;
    use crate::{
        db::{
            api::mutation::AdditionOps,
            graph::{graph::Graph, path},
        },
        prelude::NO_PROPS,
        test_storage,
    };

    #[ignore]
    #[test]
    fn triangle_scale_test() {
        let graph = Graph::new();

        let n: u64 = 1500;
        for i in 1..n {
            for j in (i + 1)..n {
                graph.add_edge(i as i64, i, j, NO_PROPS, None).unwrap();
            }
        }

        let start = Instant::now();
        let tri_count = triangle_count(&graph.clone(), None);
        println!("Time elapsed: {:?}, {tri_count}", start.elapsed());
    }

    #[test]
    fn triangle_count_1() {
        let graph = Graph::new();

        let edges = vec![
            // triangle 1
            (1, 2, 1),
            (2, 3, 1),
            (3, 1, 1),
            //triangle 2
            (4, 5, 1),
            (5, 6, 1),
            (6, 4, 1),
            // triangle 4 and 5
            (7, 8, 2),
            (8, 9, 3),
            (9, 7, 4),
            (8, 10, 5),
            (10, 9, 6),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let actual_tri_count = triangle_count(graph, Some(2));

            assert_eq!(actual_tri_count, 4)
        });
    }

    #[test]
    fn triangle_count_3() {
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
            let actual_tri_count = triangle_count(graph, None);
            assert_eq!(actual_tri_count, 8)
        });
    }
}
