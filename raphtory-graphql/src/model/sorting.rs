use dynamic_graphql::{Enum, InputObject};
use raphtory::{db::graph::node::NodeView, prelude::*};
use std::cmp::Ordering;

#[derive(InputObject, Clone, Debug, Eq, PartialEq)]
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

#[derive(InputObject, Clone, Debug, Eq, PartialEq)]
pub struct NodeSortBy {
    /// Reverse order
    pub reverse: Option<bool>,
    /// Unique Id
    pub id: Option<bool>,
    /// Node name
    pub name: Option<bool>,
    /// Node type. Untyped nodes sort first (before any named type).
    #[graphql(name = "type")]
    pub type_: Option<bool>,
    /// Time
    pub time: Option<SortByTime>,
    /// Property
    pub property: Option<String>,
}

#[derive(Enum, Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortByTime {
    /// Latest time
    Latest,
    /// Earliest time
    Earliest,
}

/// Compare two nodes by a single `NodeSortBy` key, applying that key's
/// `reverse`. Returns `Ordering::Equal` when the key selects nothing or the
/// values are incomparable. Shared by node sorting and edge neighbour sorting.
pub(crate) fn compare_node<'graph, G: GraphViewOps<'graph>>(
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
