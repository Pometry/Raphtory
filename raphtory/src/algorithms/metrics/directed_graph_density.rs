//! Graph density - measures how dense or sparse a graph is.
//!
//! It is defined as the ratio of the number of edges in the graph to the total number of possible
//! edges. A dense graph has a high edge-to-node ratio, while a sparse graph has a low
//! edge-to-node ratio.
//!
//! For example in social network analysis, a dense graph may indicate a highly interconnected
//! community_detection, while a sparse graph may indicate more isolated individuals.
//!
//! # Examples
//!
//! ```rust
//! use raphtory::algorithms::metrics::directed_graph_density::directed_graph_density;
//! use raphtory::prelude::*;
//!
//! let g = Graph::new();
//! let windowed_graph = g.window(0, 7);
//! let vs = vec![
//!     (1, 1, 2),
//!     (2, 1, 3),
//!     (3, 2, 1),
//!     (4, 3, 2),
//!     (5, 1, 4),
//!     (6, 4, 5),
//! ];
//!
//! for (t, src, dst) in &vs {
//! g.add_edge(*t, *src, *dst, NO_PROPS, None);
//! }
//!
//! println!("graph density: {:?}", directed_graph_density(&windowed_graph));
//! ```
//!
use crate::db::api::view::*;

/// Graph density - measures how dense or sparse a graph is.
///
/// The ratio of the number of directed edges in the graph to the total number of possible directed
/// edges (given by N * (N-1) where N is the number of nodes).
///
/// # Arguments
/// - `g`: a directed Raphtory graph
///
/// # Returns
/// Directed graph density of G.
pub fn directed_graph_density<'graph, G: GraphViewOps<'graph>>(graph: &G) -> f64 {
    graph.count_edges() as f64 / (graph.count_nodes() as f64 * (graph.count_nodes() as f64 - 1.0))
}

#[cfg(test)]
mod directed_graph_density_tests {
    use super::*;
    use crate::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
        test_storage,
    };

    #[test]
    fn low_graph_density() {
        let graph = Graph::new();

        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (3, 2, 1),
            (4, 3, 2),
            (5, 1, 4),
            (6, 4, 5),
        ];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let windowed_graph = graph.window(0, 7);
            let actual = directed_graph_density(&windowed_graph);
            let expected = 0.3;

            assert_eq!(actual, expected);
        });
    }

    #[test]
    fn complete_graph_has_graph_density_of_one() {
        let graph = Graph::new();

        let vs = vec![(1, 1, 2), (2, 2, 1)];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let windowed_graph = graph.window(0, 3);
            let actual = directed_graph_density(&windowed_graph);
            let expected = 1.0;

            assert_eq!(actual, expected);
        });
    }
}
