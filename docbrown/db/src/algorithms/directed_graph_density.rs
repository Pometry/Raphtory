//! Graph density - measures how dense or sparse a graph is.
//!
//! It is defined as the ratio of the number of edges in the graph to the total number of possible
//! edges. A dense graph has a high edge-to-vertex ratio, while a sparse graph has a low
//! edge-to-vertex ratio.
//!
//! For example in social network analysis, a dense graph may indicate a highly interconnected
//! community, while a sparse graph may indicate more isolated individuals.
//!
//! # Examples
//!
//! ```rust
//! use docbrown_db::algorithms::directed_graph_density::directed_graph_density;
//! use docbrown_db::graph::Graph;
//!
//! let g = Graph::new(1);
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
//! g.add_edge(*t, *src, *dst, &vec![]);
//! }
//!
//! println!("graph density: {:?}", directed_graph_density(&windowed_graph));
//! ```
//!
use crate::view_api::*;
use docbrown_core::tgraph_shard::errors::GraphError;

/// Measures how dense or sparse a graph is
pub fn directed_graph_density<G: GraphViewOps>(graph: &G) -> Result<f32, GraphError> {
    Ok(graph.num_edges()? as f32
        / (graph.num_vertices()? as f32 * (graph.num_vertices()? as f32 - 1.0)))
}

#[cfg(test)]
mod directed_graph_density_tests {
    use crate::graph::Graph;

    use super::directed_graph_density;

    #[test]
    fn low_graph_density() {
        let g = Graph::new(1);
        let windowed_graph = g.window(0, 7);
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (3, 2, 1),
            (4, 3, 2),
            (5, 1, 4),
            (6, 4, 5),
        ];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let actual = directed_graph_density(&windowed_graph).unwrap();
        let expected = 0.3;

        assert_eq!(actual, expected);
    }

    #[test]
    fn complete_graph_has_graph_density_of_one() {
        let g = Graph::new(1);
        let windowed_graph = g.window(0, 3);
        let vs = vec![(1, 1, 2), (2, 2, 1)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let actual = directed_graph_density(&windowed_graph).unwrap();
        let expected = 1.0;

        assert_eq!(actual, expected);
    }
}
