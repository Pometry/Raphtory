//! Global triangle count:  Counts the number of triangles in a graph.
//!
//! A triangle in a graph is a set of three vertices that are mutually adjacent,
//! meaning each vertex is directly connected to the other two vertices.
//!
//! This can be computationally expensive for large graphs as every vertexes adjacenlist must be
//! calculated.
//!
//! In social network analysis, this can measure the level of connectedness or clustering
//! in a network. Un scientific simulations, this can measure the properties of complex systems.
//!
//! # Examples
//! ```rust
//! use docbrown_db::algorithms::global_triangle_count::global_triangle_count;
//! use docbrown_db::graph::Graph;
//! let g = Graph::new(1);
//! let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];
//!
//! for (t, src, dst) in &vs {
//! g.add_edge(*t, *src, *dst, &vec![]);
//! }
//!
//! let windowed_graph = g.window(0, 5);
//! let expected = 1;
//!
//! println!("global_triangle_count: {:?}", global_triangle_count(&windowed_graph));  
//! ```

use crate::view_api::*;
use docbrown_core::tgraph_shard::errors::GraphError;
use itertools::Itertools;
use rayon::prelude::*;

/// Counts the number of triangles in a graph.
pub fn global_triangle_count<G: GraphViewOps>(graph: &G) -> Result<usize, GraphError> {
    let r: Result<Vec<_>, _> = graph
        .vertices()
        .into_iter()
        .par_bridge()
        .map(|v| {
            let r: Result<Vec<_>, _> = v
                .neighbours()
                .id()
                .into_iter()
                .combinations(2)
                .filter_map(|nb| match graph.has_edge(nb[0], nb[1]) {
                    Ok(true) => Some(Ok(nb)),
                    Ok(false) => match graph.has_edge(nb[1], nb[0]) {
                        Ok(true) => Some(Ok(nb)),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    },
                    Err(e) => Some(Err(e)),
                })
                .collect();
            r.map(|t| t.len())
        })
        .collect();

    let count: usize = r?.into_iter().sum();
    Ok(count / 3)
}

#[cfg(test)]
mod triangle_count_tests {
    use super::global_triangle_count;
    use crate::graph::Graph;

    #[test]
    fn counts_triangles() {
        let g = Graph::new(1);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let windowed_graph = g.window(0, 5);
        let expected = 1;

        let actual = global_triangle_count(&windowed_graph).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn counts_triangles_again() {
        let g = Graph::new(1);

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

        for (src, dst, t) in &edges {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let windowed_graph = g.window(0, 95);
        let expected = 8;

        let actual = global_triangle_count(&windowed_graph).unwrap();

        assert_eq!(actual, expected);
    }
}
