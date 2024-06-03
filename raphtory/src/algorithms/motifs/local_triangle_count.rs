//! Local triangle count - calculates the number of triangles (a cycle of length 3) for a node.
//! It measures the local clustering of a graph.
//!
//! This is useful for understanding the level of connectivity and the likelihood of information
//! or influence spreading through a network.
//!
//! For example, in a social network, the local triangle count of a user's profile can reveal the
//! number of mutual friends they have and the level of interconnectivity between those friends.
//! A high local triangle count for a user indicates that they are part of a tightly-knit group
//! of people, which can be useful for targeted advertising or identifying key influencers
//! within a network.
//!
//! Local triangle count can also be used in other domains such as biology, where it can be used
//! to analyze protein interaction networks, or in transportation networks, where it can be used
//! to identify critical junctions or potential traffic bottlenecks.
//!
//! # Examples
//!
//! ```rust
//! use raphtory::algorithms::motifs::local_triangle_count::{local_triangle_count};
//! use raphtory::prelude::*;
//!
//! let g = Graph::new();
//! let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];
//!
//! for (t, src, dst) in &vs {
//!     g.add_edge(*t, *src, *dst, NO_PROPS, None);
//! }
//!
//! let windowed_graph = g.window(0, 5);
//! let expected = vec![(1), (1), (1)];
//!
//! let result = (1..=3)
//!     .map(|v| local_triangle_count(&windowed_graph, v))
//!     .collect::<Vec<_>>();
//!
//! println!("local_triangle_count: {:?}", result);
//! ```
//!
use crate::{core::entities::nodes::node_ref::AsNodeRef, db::api::view::*};
use itertools::Itertools;

/// calculates the number of triangles (a cycle of length 3) for a node.
pub fn local_triangle_count<G: StaticGraphViewOps, V: AsNodeRef>(graph: &G, v: V) -> Option<usize> {
    if let Some(node) = (&graph).node(v) {
        if node.degree() >= 2 {
            let len = node
                .neighbours()
                .iter()
                .combinations(2)
                .filter_map(|nb| match graph.has_edge(nb[0], nb[1]) {
                    true => Some(1),
                    false => match graph.has_edge(nb[1], nb[0]) {
                        true => Some(1),
                        false => None,
                    },
                })
                .count();
            Some(len)
        } else {
            Some(0)
        }
    } else {
        None
    }
}

#[cfg(test)]
mod triangle_count_tests {
    use super::local_triangle_count;
    use crate::{
        db::{
            api::{mutation::AdditionOps, view::*},
            graph::graph::Graph,
        },
        prelude::NO_PROPS,
    };
    use tempfile::TempDir;

    #[test]
    fn counts_triangles() {
        let graph = Graph::new();
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        let test_dir = TempDir::new().unwrap();
        #[cfg(feature = "storage")]
        let disk_graph = graph.persist_as_disk_graph(test_dir.path()).unwrap();

        fn test<G: StaticGraphViewOps>(graph: &G) {
            let windowed_graph = graph.window(0, 5);
            let expected = vec![1, 1, 1];

            let actual = (1..=3)
                .map(|v| local_triangle_count(&windowed_graph, v).unwrap())
                .collect::<Vec<_>>();

            assert_eq!(actual, expected);
        }
        test(&graph);
        #[cfg(feature = "storage")]
        test(&disk_graph);
    }
}
