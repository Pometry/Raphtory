//! Reciprocity - measure of the symmetry of relationships in a graph.
//! This calculates the number of reciprocal connections (edges that go in both directions) in a
//! graph and normalizes it by the total number of edges.
//!
//! In a social network context, reciprocity measures the likelihood that if person A is linked
//! to person B, then person B is linked to person A. This algorithm can be used to determine the
//! level of symmetry or balance in a social network. It can also reveal the power dynamics in a
//! group or community. For example, if one person has many connections that are not reciprocated,
//! it could indicate that this person has more power or influence in the network than others.
//!
//! In a business context, reciprocity can be used to study customer behavior. For instance, in a
//! transactional network, if a customer tends to make a purchase from a seller and then the seller
//! makes a purchase from the same customer, it can indicate a strong reciprocal relationship
//! between them. On the other hand, if the seller does not make a purchase from the same customer,
//! it could imply a less reciprocal or more one-sided relationship.
//!
//! There are three algorithms in this module:
//!  - `local_reciprocity` - returns the reciprocity of a single vertex
//! - `all_local_reciprocity` - returns the reciprocity of every vertex in the graph as a tuple of
//! vector id and the reciprocity
//! - `global_reciprocity` - returns the global reciprocity of the entire graph
//!
//! # Examples
//!
//! ```rust
//! use docbrown_db::algorithms::reciprocity::{all_local_reciprocity, global_reciprocity, local_reciprocity};
//! use docbrown_db::graph::Graph;
//! use docbrown_db::view_api::GraphViewOps;
//! let g = Graph::new(1);
//! let vs = vec![
//!     (1, 1, 2),
//!     (1, 1, 4),
//!     (1, 2, 3),
//!     (1, 3, 2),
//!     (1, 3, 1),
//!     (1, 4, 3),
//!     (1, 4, 1),
//!     (1, 1, 5),
//! ];
//!
//! for (t, src, dst) in &vs {
//!     g.add_edge(*t, *src, *dst, &vec![]);
//! }
//!
//! let windowed_graph = g.window(0, 2);
//! println!("local_reciprocity: {:?}",  local_reciprocity(&windowed_graph, 5));
//!
//! println!("all_local_reciprocity: {:?}", all_local_reciprocity(&windowed_graph));
//!
//! println!("global_reciprocity: {:?}", global_reciprocity(&windowed_graph));
//! ```
use crate::vertex::VertexView;
use crate::view_api::*;
use docbrown_core::tgraph_shard::errors::GraphError;
use std::collections::HashSet;

fn get_reciprocal_edge_count<G: GraphViewOps>(v: &VertexView<G>) -> (u64, u64, u64) {
    let out_neighbours: HashSet<u64> = v.out_neighbours().id().filter(|x| *x != v.id()).collect();
    let in_neighbours: HashSet<u64> = v.in_neighbours().id().filter(|x| *x != v.id()).collect();
    (
        out_neighbours.len() as u64,
        in_neighbours.len() as u64,
        out_neighbours.intersection(&in_neighbours).count() as u64,
    )
}

/// Returns the global reciprocity of the entire graph
pub fn global_reciprocity<G: GraphViewOps>(graph: &G) -> f64 {
    let edges = graph.vertices().into_iter().fold((0, 0), |acc, v| {
        let r_e_c = get_reciprocal_edge_count(&v);
        (acc.0 + r_e_c.0, acc.1 + r_e_c.2)
    });
    edges.1 as f64 / edges.0 as f64
}

/// Returns the reciprocity of every vertex in the graph as a tuple of
/// vector id and the reciprocity
pub fn all_local_reciprocity<G: GraphViewOps>(graph: &G) -> Vec<(u64, f64)> {
    graph
        .vertices()
        .into_iter()
        .map(|v| (v.id(), local_reciprocity(graph, v.id())))
        .collect()
}

/// Returns the reciprocity value of a single vertex
pub fn local_reciprocity<G: GraphViewOps>(graph: &G, v: u64) -> f64 {
    match graph.vertex(v) {
        None => 0 as f64,
        Some(vertex) => {
            let intersection = get_reciprocal_edge_count(&vertex);
            2.0 * intersection.2 as f64 / (intersection.0 + intersection.1) as f64
        }
    }
}

#[cfg(test)]
mod reciprocity_test {
    use super::{all_local_reciprocity, global_reciprocity, local_reciprocity};
    use crate::graph::Graph;
    use crate::view_api::*;

    #[test]
    fn check_all_reciprocities() {
        let g = Graph::new(1);
        let vs = vec![
            (1, 1, 2),
            (1, 1, 4),
            (1, 2, 3),
            (1, 3, 2),
            (1, 3, 1),
            (1, 4, 3),
            (1, 4, 1),
            (1, 1, 5),
        ];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let windowed_graph = g.window(0, 2);
        let expected = 0.0;
        let actual = local_reciprocity(&windowed_graph, 5);
        assert_eq!(actual, expected);

        let expected: Vec<(u64, f64)> =
            vec![(1, 0.4), (2, 2.0 / 3.0), (3, 0.5), (4, 2.0 / 3.0), (5, 0.0)];

        let mut actual = all_local_reciprocity(&windowed_graph);
        actual.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(actual, expected);

        let actual = global_reciprocity(&windowed_graph);
        let expected = 0.5;
        assert_eq!(actual, expected);
    }
}
