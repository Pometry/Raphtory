//! Reciprocity - measure of the symmetry of relationships in a graph.
//! This calculates the number of reciprocal connections (edges that go in both directions) in a
//! graph and normalizes it by the total number of edges.
//!
//! In a social network context, reciprocity measures the likelihood that if person A is linked
//! to person B, then person B is linked to person A. This algorithm can be used to determine the
//! level of symmetry or balance in a social network. It can also reveal the power dynamics in a
//! group or community_detection. For example, if one person has many connections that are not reciprocated,
//! it could indicate that this person has more power or influence in the network than others.
//!
//! In a business context, reciprocity can be used to study customer behavior. For instance, in a
//! transactional network, if a customer tends to make a purchase from a seller and then the seller
//! makes a purchase from the same customer, it can indicate a strong reciprocal relationship
//! between them. On the other hand, if the seller does not make a purchase from the same customer,
//! it could imply a less reciprocal or more one-sided relationship.
//!
//! There are three algorithms in this module:
//! - `all_local_reciprocity` - returns the reciprocity of every node in the graph as a tuple of
//! vector id and the reciprocity
//! - `global_reciprocity` - returns the global reciprocity of the entire graph
//!
//! # Examples
//!
//! ```rust
//! use raphtory::algorithms::metrics::reciprocity::{all_local_reciprocity, global_reciprocity};
//! use raphtory::prelude::*;
//! let g = Graph::new();
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
//!     g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
//! }
//!
//! println!("all_local_reciprocity: {:?}", all_local_reciprocity(&g));
//! println!("global_reciprocity: {:?}", global_reciprocity(&g));
//! ```
use crate::{
    db::{
        api::{
            state::NodeState,
            view::{NodeViewOps, StaticGraphViewOps},
        },
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use rayon::prelude::*;
use std::collections::HashSet;

/// Gets the unique edge counts excluding cycles for a node. Returns a tuple of usize
/// (out neighbours, in neighbours, the intersection of the out and in neighbours)
fn get_reciprocal_edge_count<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>(
    v: &NodeView<G, GH>,
) -> (usize, usize, usize) {
    let id = v.node;
    let out_neighbours: HashSet<_> = v
        .out_neighbours()
        .iter()
        .filter_map(|x| (x.node != id).then_some(x.node))
        .collect();

    let in_neighbours: HashSet<_> = v
        .in_neighbours()
        .iter()
        .filter_map(|x| (x.node != id).then_some(x.node))
        .collect();

    let out_inter_in = out_neighbours.intersection(&in_neighbours).count();
    (out_neighbours.len(), in_neighbours.len(), out_inter_in)
}

/// Reciprocity - measure of the symmetry of relationships in a graph, the global reciprocity of
/// the entire graph.
/// This calculates the number of reciprocal connections (edges that go in both directions) in a
/// graph and normalizes it by the total number of directed edges.
///
/// # Arguments
/// - `g`: a directed Raphtory graph
///
/// # Returns
/// reciprocity of the graph between 0 and 1.
pub fn global_reciprocity<G: StaticGraphViewOps>(g: &G) -> f64 {
    let (edge_count, intersect_count) = g
        .nodes()
        .par_iter()
        .map(|n| {
            let (out, _, intersect) = get_reciprocal_edge_count(&n);
            (out, intersect)
        })
        .reduce(|| (0, 0), |l, r| (l.0 + r.0, l.1 + r.1));
    intersect_count as f64 / edge_count as f64
}

/// Local reciprocity - measure of the symmetry of relationships associated with a node
///
/// This measures the proportion of a node's outgoing edges which are reciprocated with an incoming edge.
///
/// # Arguments
/// - `g` : a directed Raphtory graph
///
/// # Returns
/// [AlgorithmResult] with string keys and float values mapping each node name to its reciprocity value.
///
pub fn all_local_reciprocity<G: StaticGraphViewOps>(g: &G) -> NodeState<f64, G> {
    let values: Vec<_> = g
        .nodes()
        .par_iter()
        .map(|n| {
            let (out_count, in_count, intersect_count) = get_reciprocal_edge_count(&n);
            2.0 * intersect_count as f64 / (out_count + in_count) as f64
        })
        .collect();
    NodeState::new_from_values(g.clone(), values)
}

#[cfg(test)]
mod reciprocity_test {
    use crate::{
        algorithms::metrics::reciprocity::{all_local_reciprocity, global_reciprocity},
        prelude::*,
        test_storage,
    };
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    #[test]
    fn test_global_recip() {
        let graph = Graph::new();

        let vs = vec![
            (1, 2),
            (1, 4),
            (2, 3),
            (3, 2),
            (3, 1),
            (4, 3),
            (4, 1),
            (1, 5),
        ];

        for (src, dst) in &vs {
            graph.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let actual = global_reciprocity(graph);
            assert_eq!(actual, 0.5);

            let mut hash_map_result: HashMap<String, f64> = HashMap::new();
            hash_map_result.insert("1".to_string(), 0.4);
            hash_map_result.insert("2".to_string(), 2.0 / 3.0);
            hash_map_result.insert("3".to_string(), 0.5);
            hash_map_result.insert("4".to_string(), 2.0 / 3.0);
            hash_map_result.insert("5".to_string(), 0.0);

            let res = all_local_reciprocity(graph);
            assert_eq!(res, hash_map_result);
        });
    }
}
