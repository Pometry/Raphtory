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
//! use docbrown::db::graph::Graph;
//! use docbrown::algorithms::triplet_count::triplet_count;
//! use docbrown::db::view_api::*;
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
//!  let results = triplet_count(&graph.at(1));
//!  println!("triplet count: {}", results);
//! ```
//!

use crate::core::state;
use crate::db::program::{GlobalEvalState, LocalState, Program};
use crate::db::view_api::GraphViewOps;

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
/// use docbrown::db::graph::Graph;
/// use docbrown::algorithms::triplet_count::triplet_count;
/// use docbrown::db::view_api::*;
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
///  let results = triplet_count(&graph.at(1));
///  println!("triplet count: {}", results);
/// ```
///
pub fn triplet_count<G: GraphViewOps>(g: &G) -> usize {
    let mut gs = GlobalEvalState::new(g.clone(), false);
    let tc = TripletCount {};
    tc.run_step(g, &mut gs);
    tc.produce_output(g, &gs)
}

/// Counts the number of open and closed triplets in a graph
pub struct TripletCount {}

/// Counts the number of open and closed triplets in a graph
impl Program for TripletCount {
    type Out = usize;

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let count = c.global_agg(state::def::sum::<usize>(111));

        /// Source: https://stackoverflow.com/questions/65561566/number-of-combinations-permutations
        fn count_two_combinations(n: u64) -> u64 {
            ((0.5 * n as f64) * (n - 1) as f64) as u64
        }

        // In the step function get the neighbours of v, remove it self, then count the result
        // and add it to the global count
        c.step(|v| {
            let c1 = v
                .neighbours()
                .map(|n| n.global_id())
                .filter(|n| *n != v.global_id())
                .count();
            let c2 = count_two_combinations(c1 as u64) as usize;
            v.global_update(&count, c2);
        })
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.global_agg(state::def::sum::<usize>(111));
        c.step(|_| false)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
        gs.read_global_state(&state::def::sum::<usize>(111))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod program_test {
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
            graph.add_edge(0, src, dst, &vec![], None);
        }
        let exp_triplet_count = 20;
        let results = triplet_count(&graph.at(1));
        assert_eq!(results, exp_triplet_count);
    }
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
            graph.add_edge(0, src, dst, &vec![], None);
        }
        let exp_triplet_count = 20;
        let results = triplet_count(&graph.at(1));
        assert_eq!(results, exp_triplet_count);
    }
}
