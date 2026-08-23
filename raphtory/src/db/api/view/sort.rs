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
        NodeSortKey::Property(prop) => a
            .properties()
            .get(prop)
            .partial_cmp(&b.properties().get(prop)),
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
        EdgeSortKey::Property(prop) => a
            .properties()
            .get(prop)
            .partial_cmp(&b.properties().get(prop)),
    };
    match ordering {
        Some(o) if sort_by.reverse => o.reverse(),
        Some(o) => o,
        None => Ordering::Equal,
    }
}
