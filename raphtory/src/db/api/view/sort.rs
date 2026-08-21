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

/// One entry in a node sort-key list. Exactly one of the key fields should be
/// set per entry.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeSortBy {
    /// Reverse order
    pub reverse: Option<bool>,
    /// Unique Id
    pub id: Option<bool>,
    /// Node name
    pub name: Option<bool>,
    /// Node type. Untyped nodes sort first (before any named type).
    pub type_: Option<bool>,
    /// Time
    pub time: Option<SortByTime>,
    /// Property
    pub property: Option<String>,
}

/// One entry in an edge sort-key list. Exactly one of the key fields should be
/// set per entry.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EdgeSortBy {
    /// Reverse order. Applies to the `time` / `property` keys; the node keys
    /// (`src` / `dst` / `neighbour`) carry their own `reverse` inside the
    /// nested `NodeSortBy` and ignore this flag.
    pub reverse: Option<bool>,
    /// Sort by the source node.
    pub src: Option<NodeSortBy>,
    /// Sort by the destination node.
    pub dst: Option<NodeSortBy>,
    /// Sort by the neighbour node: the endpoint that is NOT the node these
    /// edges were traversed from (the destination for a graph-level edge
    /// collection).
    pub neighbour: Option<NodeSortBy>,
    /// Time
    pub time: Option<SortByTime>,
    /// Property
    pub property: Option<String>,
}

/// Compare two nodes by a single `NodeSortBy` key, applying that key's
/// `reverse`. Returns `Ordering::Equal` when the key selects nothing or the
/// values are incomparable. Shared by node sorting and edge endpoint sorting.
pub fn compare_node<'graph, G: GraphViewOps<'graph>>(
    a: &NodeView<'graph, G>,
    b: &NodeView<'graph, G>,
    sort_by: &NodeSortBy,
) -> Ordering {
    let ordering = if sort_by.id == Some(true) {
        a.id().partial_cmp(&b.id())
    } else if sort_by.name == Some(true) {
        a.name().partial_cmp(&b.name())
    } else if sort_by.type_ == Some(true) {
        a.node_type().partial_cmp(&b.node_type())
    } else if let Some(sort_by_time) = sort_by.time.as_ref() {
        let (first, second) = match sort_by_time {
            SortByTime::Latest => (a.latest_time(), b.latest_time()),
            SortByTime::Earliest => (a.earliest_time(), b.earliest_time()),
        };
        first.partial_cmp(&second)
    } else if let Some(prop) = sort_by.property.as_ref() {
        a.properties()
            .get(prop)
            .partial_cmp(&b.properties().get(prop))
    } else {
        None
    };
    match ordering {
        Some(o) if sort_by.reverse == Some(true) => o.reverse(),
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
    if let Some(src_sort) = sort_by.src.as_ref() {
        return compare_node(&a.src(), &b.src(), src_sort);
    }
    if let Some(dst_sort) = sort_by.dst.as_ref() {
        return compare_node(&a.dst(), &b.dst(), dst_sort);
    }
    if let Some(neighbour_sort) = sort_by.neighbour.as_ref() {
        return compare_node(&a.nbr(), &b.nbr(), neighbour_sort);
    }
    let ordering = if let Some(sort_by_time) = sort_by.time.as_ref() {
        let (first, second) = match sort_by_time {
            SortByTime::Latest => (a.latest_time(), b.latest_time()),
            SortByTime::Earliest => (a.earliest_time(), b.earliest_time()),
        };
        first.partial_cmp(&second)
    } else if let Some(prop) = sort_by.property.as_ref() {
        a.properties()
            .get(prop)
            .partial_cmp(&b.properties().get(prop))
    } else {
        None
    };
    match ordering {
        Some(o) if sort_by.reverse == Some(true) => o.reverse(),
        Some(o) => o,
        None => Ordering::Equal,
    }
}
