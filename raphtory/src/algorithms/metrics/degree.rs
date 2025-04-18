//! Varying degree calculations for the entire graph.
//! The degree of a node is the number of edges connected to it.
//!
//! There are two types of degree: in-degree and out-degree.
//! In-degree is the number of edges that point to a node.
//! Out-degree is the number of edges that point away from a node.
//!
//! This library provides the following degree calculations:
//! - max_out_degree - The maximum out degree of any node in the graph.
//! - max_in_degree - The maximum in degree of any node in the graph.
//! - min_out_degree - The minimum out degree of any node in the graph.
//! - min_in_degree - The minimum in degree of any node in the graph.
//! - average_degree - The average degree of all nodes in the graph.
//!
//!
//! # Examples
//!
//! ```rust
//! use raphtory::algorithms::metrics::degree::*;
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
//!     g.add_edge(*t, *src, *dst, NO_PROPS, None);
//! }
//!
//! print!("Max out degree: {:?}", max_out_degree(&windowed_graph));
//! print!("Max in degree: {:?}", max_in_degree(&windowed_graph));
//! print!("Min out degree: {:?}", min_out_degree(&windowed_graph));
//! print!("Min in degree: {:?}", min_in_degree(&windowed_graph));
//! print!("Max degree: {:?}", min_degree(&windowed_graph));
//! print!("Min degree: {:?}", max_degree(&windowed_graph));
//! print!("Average degree: {:?}", average_degree(&windowed_graph));
//! ```
//!
use crate::{db::api::view::*, prelude::*};
use rayon::prelude::*;

/// The largest degree
pub fn max_degree<'graph, G: GraphViewOps<'graph>>(graph: &G) -> usize {
    graph.nodes().degree().max().unwrap_or(0)
}

/// The minimum degree of any node in the graph
pub fn min_degree<'graph, G: GraphViewOps<'graph>>(graph: &'graph G) -> usize {
    graph.nodes().degree().min().unwrap_or(0)
}

/// The maximum out degree of any node in the graph.
pub fn max_out_degree<'graph, G: GraphViewOps<'graph>>(graph: &'graph G) -> usize {
    graph.nodes().out_degree().max().unwrap_or(0)
}

/// The maximum in degree of any node in the graph.
pub fn max_in_degree<'graph, G: GraphViewOps<'graph>>(graph: &'graph G) -> usize {
    graph.nodes().in_degree().max().unwrap_or(0)
}

/// The minimum out degree of any node in the graph.
pub fn min_out_degree<'graph, G: GraphViewOps<'graph>>(graph: &'graph G) -> usize {
    graph.nodes().out_degree().min().unwrap_or(0)
}

/// The minimum in degree of any node in the graph.
pub fn min_in_degree<'graph, G: GraphViewOps<'graph>>(graph: &'graph G) -> usize {
    graph.nodes().in_degree().min().unwrap_or(0)
}

/// The average degree of all nodes in the graph.
pub fn average_degree<'graph, G: GraphViewOps<'graph>>(graph: &'graph G) -> f64 {
    let (deg_sum, count) = graph
        .nodes()
        .degree()
        .par_iter_values()
        .fold_with((0usize, 0usize), |(deg_sum, count), deg| {
            (deg_sum + deg, count + 1)
        })
        .reduce_with(|(deg_sum1, count1), (deg_sum2, count2)| {
            (deg_sum1 + deg_sum2, count1 + count2)
        })
        .unwrap_or((0, 1));

    deg_sum as f64 / count as f64
}

#[cfg(test)]
mod degree_test {
    use super::max_out_degree;
    use crate::{
        algorithms::metrics::degree::{
            average_degree, max_degree, max_in_degree, min_degree, min_in_degree, min_out_degree,
        },
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
        test_storage,
    };

    #[test]
    fn degree_test() {
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
            let expected_max_out_degree = 3;
            let actual_max_out_degree = max_out_degree(graph);

            let expected_max_in_degree = 2;
            let actual_max_in_degree = max_in_degree(graph);

            let expected_min_out_degree = 0;
            let actual_min_out_degree = min_out_degree(graph);

            let expected_min_in_degree = 1;
            let actual_min_in_degree = min_in_degree(graph);

            let expected_average_degree = 2.0;
            let actual_average_degree = average_degree(graph);

            let expected_max_degree = 3;
            let actual_max_degree = max_degree(graph);

            let expected_min_degree = 1;
            let actual_min_degree = min_degree(graph);

            assert_eq!(expected_max_out_degree, actual_max_out_degree);
            assert_eq!(expected_max_in_degree, actual_max_in_degree);
            assert_eq!(expected_min_out_degree, actual_min_out_degree);
            assert_eq!(expected_min_in_degree, actual_min_in_degree);
            assert_eq!(expected_average_degree, actual_average_degree);
            assert_eq!(expected_max_degree, actual_max_degree);
            assert_eq!(expected_min_degree, actual_min_degree);
        });
    }
}
