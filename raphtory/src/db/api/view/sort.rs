//! Sort keys for reordering node and edge collections.
//!
//! A sort is an ordered list of keys: members compare by the first key, ties
//! break to the next. Each key selects exactly one attribute (id / name /
//! type / time / property for nodes; src / dst / neighbour / time / property
//! for edges) and carries its own `reverse` flag. Incomparable values (e.g. a
//! property missing on one side) compare equal, so they keep their relative
//! order and fall through to the next key.

use crate::{
    db::graph::{edge::EdgeView, node::NodeView},
    prelude::*,
};
use std::cmp::Ordering;

/// Which time boundary of a member to sort by.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortByTime {
    /// Latest time
    Latest,
    /// Earliest time
    Earliest,
}

/// Which attribute a node sort key orders by. Exactly one, by construction —
/// an "all fields optional" struct would let a caller set none (a silent no-op
/// sort) or several (all but one silently ignored).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeSortKey {
    /// Unique id
    Id,
    /// Node name
    Name,
    /// Node type. Untyped nodes sort first (before any named type).
    Type,
    /// Earliest or latest event on the node
    Time(SortByTime),
    /// A property value
    Property(String),
}

/// One entry in a node sort-key list: what to order by, and which direction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeSortBy {
    /// Reverse order
    pub reverse: bool,
    /// The attribute to order by
    pub key: NodeSortKey,
}

/// Which attribute an edge sort key orders by. The endpoint keys carry a whole
/// `NodeSortBy`, so their own `reverse` controls direction and the outer one is
/// ignored for them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EdgeSortKey {
    /// Order by the source node
    Src(NodeSortBy),
    /// Order by the destination node
    Dst(NodeSortBy),
    /// Order by the neighbour node: the endpoint that is NOT the node these
    /// edges were traversed from (the destination for a graph-level edge
    /// collection).
    Neighbour(NodeSortBy),
    /// Earliest or latest event on the edge
    Time(SortByTime),
    /// A property value
    Property(String),
}

/// One entry in an edge sort-key list: what to order by, and which direction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeSortBy {
    /// Reverse order. Ignored by the endpoint keys, which carry their own.
    pub reverse: bool,
    /// The attribute to order by
    pub key: EdgeSortKey,
}

/// A missing property sorts before any value, as `Option` orders it.
fn compare_props(a: Option<Prop>, b: Option<Prop>) -> Option<Ordering> {
    match (a, b) {
        (Some(a), Some(b)) => a.total_cmp(&b),
        (a, b) => Some(a.is_some().cmp(&b.is_some())),
    }
}

/// Compare two nodes by a single `NodeSortBy` key, applying that key's
/// `reverse`. Returns `Ordering::Equal` when the key selects nothing or the
/// values are incomparable. Shared by node sorting and edge endpoint sorting.
pub fn compare_node<'graph, G: GraphViewOps<'graph>>(
    a: &NodeView<'graph, G>,
    b: &NodeView<'graph, G>,
    sort_by: &NodeSortBy,
) -> Ordering {
    let ordering = match &sort_by.key {
        NodeSortKey::Id => a.id().partial_cmp(&b.id()),
        NodeSortKey::Name => a.name().partial_cmp(&b.name()),
        NodeSortKey::Type => a.node_type().partial_cmp(&b.node_type()),
        NodeSortKey::Time(t) => {
            let (first, second) = match t {
                SortByTime::Latest => (a.latest_time(), b.latest_time()),
                SortByTime::Earliest => (a.earliest_time(), b.earliest_time()),
            };
            first.partial_cmp(&second)
        }
        NodeSortKey::Property(prop) => {
            compare_props(a.properties().get(prop), b.properties().get(prop))
        }
    };
    match ordering {
        Some(o) if sort_by.reverse => o.reverse(),
        Some(o) => o,
        None => Ordering::Equal,
    }
}

/// Compare two edges by a single `EdgeSortBy` key. Node keys resolve their
/// endpoint and delegate to [`compare_node`], which applies the nested
/// `NodeSortBy.reverse`; they return directly so the outer `reverse` never
/// double-negates.
pub fn compare_edge<'graph, G: GraphViewOps<'graph>>(
    a: &EdgeView<G>,
    b: &EdgeView<G>,
    sort_by: &EdgeSortBy,
) -> Ordering {
    // Endpoint keys delegate to `compare_node`, which applies the nested
    // `NodeSortBy.reverse`; they return directly so the outer `reverse` never
    // double-negates.
    let ordering = match &sort_by.key {
        EdgeSortKey::Src(key) => return compare_node(&a.src(), &b.src(), key),
        EdgeSortKey::Dst(key) => return compare_node(&a.dst(), &b.dst(), key),
        EdgeSortKey::Neighbour(key) => return compare_node(&a.nbr(), &b.nbr(), key),
        EdgeSortKey::Time(t) => {
            let (first, second) = match t {
                SortByTime::Latest => (a.latest_time(), b.latest_time()),
                SortByTime::Earliest => (a.earliest_time(), b.earliest_time()),
            };
            first.partial_cmp(&second)
        }
        EdgeSortKey::Property(prop) => {
            compare_props(a.properties().get(prop), b.properties().get(prop))
        }
    };
    match ordering {
        Some(o) if sort_by.reverse => o.reverse(),
        Some(o) => o,
        None => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorting_by_a_float_property_with_nan_is_a_total_order() {
        let g = Graph::new();
        let values = [1.0, f64::NAN, 3.0, 0.0, 2.0, f64::NAN];
        for (i, value) in values.iter().cycle().take(24).enumerate() {
            g.add_node(i as i64, i as u64, [("w", *value)], None, None)
                .unwrap();
            g.add_edge(i as i64, i as u64, i as u64 + 1, [("w", *value)], None)
                .unwrap();
        }
        let by_w = NodeSortBy {
            reverse: false,
            key: NodeSortKey::Property("w".to_string()),
        };
        let by_id = NodeSortBy {
            reverse: false,
            key: NodeSortKey::Id,
        };
        let sorted: Vec<f64> = g
            .nodes()
            .sorted(&[by_w, by_id])
            .iter()
            .filter_map(|node| node.properties().get("w").and_then(|w| w.as_f64()))
            .collect();
        assert_eq!(sorted.len(), 24);
        assert!(sorted.is_sorted_by(|a, b| a.total_cmp(b).is_le()));
        assert!(sorted[..16].iter().all(|w| !w.is_nan()));

        let by_w = EdgeSortBy {
            reverse: true,
            key: EdgeSortKey::Property("w".to_string()),
        };
        let by_time = EdgeSortBy {
            reverse: false,
            key: EdgeSortKey::Time(SortByTime::Earliest),
        };
        let sorted: Vec<f64> = g
            .edges()
            .sorted(&[by_w, by_time])
            .iter()
            .filter_map(|edge| edge.properties().get("w").and_then(|w| w.as_f64()))
            .collect();
        assert!(sorted.is_sorted_by(|a, b| a.total_cmp(b).is_ge()));
    }
}
