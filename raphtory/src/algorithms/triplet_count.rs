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
//! use raphtory::db::graph::Graph;
//! use raphtory::algorithms::triplet_count::triplet_count;
//! use raphtory::db::view_api::*;
//! let graph = Graph::new(2);
//!  let edges = vec![
//!      (1, 2),
//!      (1, 3),
//!      (1, 4),
//!      (2, 1),
//!      (2, 6),
//!      (2, 7),
//!  ];
//!  for (src, dst) in edges {
//!      graph.add_edge(0, src, dst, &vec![], None);
//!  }
//!  let results = triplet_count(&graph.at(1), None);
//!  println!("triplet count: {}", results);
//! ```
//!
use crate::core::state::accumulator_id::accumulators::sum;
use crate::core::state::compute_state::ComputeStateVec;
use crate::db::task::context::Context;
use crate::db::task::task::{ATask, Job, Step};
use crate::db::task::task_runner::TaskRunner;
use crate::db::view_api::{GraphViewOps, VertexViewOps};

/// Computes the number of both open and closed triplets within a graph
///
/// An open triplet, is one where a node has two neighbors, but no edge between them.
/// A closed triplet is one where a node has two neighbors, and an edge between them.
///
/// # Arguments
///
/// * `g` - A reference to the graph
///
/// # Returns
///
/// The total number of open and closed triplets in the graph
///
/// # Example
///
/// ```rust
/// use raphtory::db::graph::Graph;
/// use raphtory::algorithms::triplet_count::triplet_count;
/// use raphtory::db::view_api::*;
/// let graph = Graph::new(2);
///  let edges = vec![
///      (1, 2),
///      (1, 3),
///      (1, 4),
///      (2, 1),
///      (2, 6),
///      (2, 7),
///  ];
///  for (src, dst) in edges {
///      graph.add_edge(0, src, dst, &vec![], None);
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

    let step1 = ATask::new(move |evv| {
        let c1 = evv.neighbours().id().filter(|n| *n != evv.id()).count();
        let c2 = count_two_combinations(c1);
        evv.global_update(&count, c2);
        Step::Continue
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![],
        vec![Job::new(step1)],
        (),
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
    use crate::db::graph::Graph;
    use crate::db::view_api::*;
    use pretty_assertions::assert_eq;

    /// Test the global clustering coefficient
    #[test]
    fn test_triplet_count() {
        let graph = Graph::new(1);

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
            graph.add_edge(0, src, dst, &vec![], None).unwrap();
        }
        let exp_triplet_count = 20;
        let results = triplet_count(&graph.at(1), None);

        assert_eq!(results, exp_triplet_count);
    }
}
