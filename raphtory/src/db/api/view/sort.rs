//! Sort keys for reordering node and edge collections.
//!
//! A sort is an ordered list of keys: members compare by the first key, ties
//! break to the next. Each key selects exactly one attribute (id / name /
//! type / time / property / degree for nodes; src / dst / neighbour / time /
//! property for edges) and carries its own `reverse` flag. Incomparable values
//! (e.g. a property missing on one side) compare equal, so they keep their
//! relative order and fall through to the next key.

use crate::{
    db::graph::{edge::EdgeView, node::NodeView},
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::{
    entities::GID,
    storage::{arc_str::ArcStr, timeindex::EventTime},
    Direction,
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
    /// Number of distinct neighbours in the given direction
    Degree(Direction),
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

/// One evaluated sort key. Each list position holds the same variant for every
/// member, so the derived `PartialOrd` compares like with like.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum SortValue {
    Id(GID),
    Name(String),
    Type(Option<ArcStr>),
    Time(Option<EventTime>),
    Property(Option<Prop>),
    Degree(usize),
}

fn node_sort_value<'graph, G: GraphViewOps<'graph>>(
    node: &NodeView<'graph, G>,
    key: &NodeSortKey,
) -> SortValue {
    match key {
        NodeSortKey::Id => SortValue::Id(node.id()),
        NodeSortKey::Name => SortValue::Name(node.name()),
        NodeSortKey::Type => SortValue::Type(node.node_type()),
        NodeSortKey::Time(SortByTime::Latest) => SortValue::Time(node.latest_time()),
        NodeSortKey::Time(SortByTime::Earliest) => SortValue::Time(node.earliest_time()),
        NodeSortKey::Property(prop) => SortValue::Property(node.properties().get(prop)),
        NodeSortKey::Degree(Direction::OUT) => SortValue::Degree(node.out_degree()),
        NodeSortKey::Degree(Direction::IN) => SortValue::Degree(node.in_degree()),
        NodeSortKey::Degree(Direction::BOTH) => SortValue::Degree(node.degree()),
    }
}

fn edge_sort_value<'graph, G: GraphViewOps<'graph>>(
    edge: &EdgeView<G>,
    key: &EdgeSortKey,
) -> SortValue {
    match key {
        EdgeSortKey::Src(node_key) => node_sort_value(&edge.src(), &node_key.key),
        EdgeSortKey::Dst(node_key) => node_sort_value(&edge.dst(), &node_key.key),
        EdgeSortKey::Neighbour(node_key) => node_sort_value(&edge.nbr(), &node_key.key),
        EdgeSortKey::Time(SortByTime::Latest) => SortValue::Time(edge.latest_time()),
        EdgeSortKey::Time(SortByTime::Earliest) => SortValue::Time(edge.earliest_time()),
        EdgeSortKey::Property(prop) => SortValue::Property(edge.properties().get(prop)),
    }
}

/// Endpoint keys are reversed by their nested `NodeSortBy`; the outer flag is
/// ignored for them so it never double-negates.
fn edge_reverse(sort_by: &EdgeSortBy) -> bool {
    match &sort_by.key {
        EdgeSortKey::Src(k) | EdgeSortKey::Dst(k) | EdgeSortKey::Neighbour(k) => k.reverse,
        EdgeSortKey::Time(_) | EdgeSortKey::Property(_) => sort_by.reverse,
    }
}

/// Lexicographic comparison of evaluated keys: incomparable values (e.g. a
/// property missing on one side) compare equal and fall through to the next key.
fn compare_values(a: &[SortValue], b: &[SortValue], reverses: &[bool]) -> Ordering {
    a.iter()
        .zip(b)
        .zip(reverses)
        .fold(Ordering::Equal, |current, ((a, b), &reverse)| {
            current.then_with(|| match a.partial_cmp(b) {
                Some(o) if reverse => o.reverse(),
                Some(o) => o,
                None => Ordering::Equal,
            })
        })
}

/// Collect at most `max` items (erroring past that, before any key is computed),
/// evaluate every item's keys once, then stable-sort by them.
fn sort_bounded<T>(
    items: impl Iterator<Item = T>,
    max: Option<usize>,
    reverses: &[bool],
    keys: impl Fn(&T) -> Vec<SortValue>,
) -> Result<Vec<T>, GraphError> {
    let items: Vec<T> = match max {
        Some(max) => {
            let items: Vec<T> = items.take(max.saturating_add(1)).collect();
            if items.len() > max {
                return Err(GraphError::SortTooLarge { max });
            }
            items
        }
        None => items.collect(),
    };
    if reverses.is_empty() {
        return Ok(items);
    }
    let mut decorated: Vec<(Vec<SortValue>, T)> =
        items.into_iter().map(|item| (keys(&item), item)).collect();
    decorated.sort_by(|(a, _), (b, _)| compare_values(a, b, reverses));
    Ok(decorated.into_iter().map(|(_, item)| item).collect())
}

/// Sort nodes by `sort_bys`, refusing to sort more than `max` of them.
pub(crate) fn sort_nodes<'graph, G: GraphViewOps<'graph>>(
    nodes: impl Iterator<Item = NodeView<'graph, G>>,
    sort_bys: &[NodeSortBy],
    max: Option<usize>,
) -> Result<Vec<NodeView<'graph, G>>, GraphError> {
    let reverses: Vec<bool> = sort_bys.iter().map(|s| s.reverse).collect();
    sort_bounded(nodes, max, &reverses, |node| {
        sort_bys
            .iter()
            .map(|s| node_sort_value(node, &s.key))
            .collect()
    })
}

/// Sort edges by `sort_bys`, refusing to sort more than `max` of them.
pub(crate) fn sort_edges<'graph, G: GraphViewOps<'graph>>(
    edges: impl Iterator<Item = EdgeView<G>>,
    sort_bys: &[EdgeSortBy],
    max: Option<usize>,
) -> Result<Vec<EdgeView<G>>, GraphError> {
    let reverses: Vec<bool> = sort_bys.iter().map(edge_reverse).collect();
    sort_bounded(edges, max, &reverses, |edge| {
        sort_bys
            .iter()
            .map(|s| edge_sort_value(edge, &s.key))
            .collect()
    })
}
