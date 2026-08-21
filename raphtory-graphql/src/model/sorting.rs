use dynamic_graphql::{Enum, InputObject};
use raphtory::db::api::view::sort;

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

impl From<SortByTime> for sort::SortByTime {
    fn from(v: SortByTime) -> Self {
        match v {
            SortByTime::Latest => sort::SortByTime::Latest,
            SortByTime::Earliest => sort::SortByTime::Earliest,
        }
    }
}

impl From<NodeSortBy> for sort::NodeSortBy {
    fn from(v: NodeSortBy) -> Self {
        sort::NodeSortBy {
            reverse: v.reverse,
            id: v.id,
            name: v.name,
            type_: v.type_,
            time: v.time.map(Into::into),
            property: v.property,
        }
    }
}

impl From<EdgeSortBy> for sort::EdgeSortBy {
    fn from(v: EdgeSortBy) -> Self {
        sort::EdgeSortBy {
            reverse: v.reverse,
            src: v.src.map(Into::into),
            dst: v.dst.map(Into::into),
            neighbour: v.neighbour.map(Into::into),
            time: v.time.map(Into::into),
            property: v.property,
        }
    }
}

impl From<sort::SortByTime> for SortByTime {
    fn from(v: sort::SortByTime) -> Self {
        match v {
            sort::SortByTime::Latest => SortByTime::Latest,
            sort::SortByTime::Earliest => SortByTime::Earliest,
        }
    }
}

impl From<sort::NodeSortBy> for NodeSortBy {
    fn from(v: sort::NodeSortBy) -> Self {
        NodeSortBy {
            reverse: v.reverse,
            id: v.id,
            name: v.name,
            type_: v.type_,
            time: v.time.map(Into::into),
            property: v.property,
        }
    }
}

impl From<sort::EdgeSortBy> for EdgeSortBy {
    fn from(v: sort::EdgeSortBy) -> Self {
        EdgeSortBy {
            reverse: v.reverse,
            src: v.src.map(Into::into),
            dst: v.dst.map(Into::into),
            neighbour: v.neighbour.map(Into::into),
            time: v.time.map(Into::into),
            property: v.property,
        }
    }
}
