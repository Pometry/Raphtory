//! Computes the number of both open and closed triplets within a graph
//!
//! An open triplet, is one where a node has two neighbors, but no edge between them.
//! A closed triplet is one where a node has two neighbors, and an edge between them.
//!
//! # Arguments
//!
//! * `g` - A reference to the graph
//!
//! # Returns
//!
//! The total number of open and closed triplets in the graph
//!
//! # Example
//!
//! ```rust
//! use raphtory::prelude::*;
//! use raphtory::algorithms::motifs::triplet_count::triplet_count;
//! let graph = Graph::new();
//!  let edges = vec![
//!      (1, 2),
//!      (1, 3),
//!      (1, 4),
//!      (2, 1),
//!      (2, 6),
//!      (2, 7),
//!  ];
//!  for (src, dst) in edges {
//!      graph.add_edge(0, src, dst, NO_PROPS, None);
//!  }
//!  let results = triplet_count(&graph.at(1), None);
//!  println!("triplet count: {}", results);
//! ```
//!
use crate::{
    core::state::{accumulator_id::accumulators::sum, compute_state::ComputeStateVec},
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

/// Computes the number of both open and closed triplets within a graph
///
/// An open triplet, is one where a node has two neighbors, but no edge between them.
/// A closed triplet is one where a node has two neighbors, and an edge between them.
///
/// # Arguments
///
/// * `g` - A reference to the graph
///
/// Returns:
///
/// The total number of open and closed triplets in the graph
///
/// # Example
///
/// ```rust
/// use raphtory::algorithms::motifs::triplet_count::triplet_count;
/// use raphtory::prelude::*;
/// let graph = Graph::new();
///  let edges = vec![
///      (1, 2),
///      (1, 3),
///      (1, 4),
///      (2, 1),
///      (2, 6),
///      (2, 7),
///  ];
///  for (src, dst) in edges {
///      graph.add_edge(0, src, dst, NO_PROPS, None);
///  }
///
///  let results = triplet_count(&graph.at(1), None);
///  println!("triplet count: {}", results);
/// ```
///
pub fn triplet_count<G: GraphViewOps>(g: &G, threads: Option<usize>) -> usize {
    /// Source: https://stackoverflow.com/questions/65561566/number-of-combinations-permutations
    fn count_two_combinations(n: usize) -> usize {
        ((0.5 * n as f64) * (n - 1) as f64) as usize
    }

    let mut ctx: Context<G, ComputeStateVec> = g.into();

    let count = sum::<usize>(0);
    ctx.global_agg(count);

    let step1 = ATask::new(
        move |evv: &mut EvalVertexView<'_, G, ComputeStateVec, ()>| {
            let c1 = evv.neighbours().id().filter(|n| *n != evv.id()).count();
            let c2 = count_two_combinations(c1);
            evv.global_update(&count, c2);
            Step::Continue
        },
    );

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![],
        vec![Job::new(step1)],
        None,
        |egs, _, _, _| egs.finalize(&count),
        threads,
        1,
        None,
        None,
    )
}

#[cfg(test)]
mod triplet_test {
    use super::*;
    use crate::{
        db::{
            api::{mutation::AdditionOps, view::*},
            graph::graph::Graph,
        },
        prelude::NO_PROPS,
    };
    use pretty_assertions::assert_eq;

    /// Test the global clustering coefficient
    #[test]
    fn test_triplet_count() {
        let graph = Graph::new();

        // Graph has 2 triangles and 20 triplets
        let edges = vec![
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 1),
            (2, 6),
            (2, 7),
            (3, 1),
            (3, 4),
            (3, 7),
            (4, 1),
            (4, 3),
            (4, 5),
            (4, 6),
            (5, 4),
            (5, 6),
            (6, 4),
            (6, 5),
            (6, 2),
            (7, 2),
            (7, 3),
        ];

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }
        let exp_triplet_count = 20;
        let results = triplet_count(&graph, None);

        assert_eq!(results, exp_triplet_count);
    }
}
