use dynamic_graphql::{Enum, InputObject};
use raphtory::{db::api::view::sort, errors::GraphError};

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

impl TryFrom<NodeSortBy> for sort::NodeSortBy {
    type Error = GraphError;

    /// The wire type has one optional field per key because GraphQL input
    /// objects cannot express "exactly one of"; this is where that constraint
    /// is checked, so the core type can carry a single key by construction.
    fn try_from(v: NodeSortBy) -> Result<Self, Self::Error> {
        let mut keys: Vec<sort::NodeSortKey> = Vec::new();
        if v.id == Some(true) {
            keys.push(sort::NodeSortKey::Id);
        }
        if v.name == Some(true) {
            keys.push(sort::NodeSortKey::Name);
        }
        if v.type_ == Some(true) {
            keys.push(sort::NodeSortKey::Type);
        }
        if let Some(t) = v.time {
            keys.push(sort::NodeSortKey::Time(t.into()));
        }
        if let Some(p) = v.property {
            keys.push(sort::NodeSortKey::Property(p));
        }
        match <[sort::NodeSortKey; 1]>::try_from(keys) {
            Ok([key]) => Ok(sort::NodeSortBy {
                reverse: v.reverse == Some(true),
                key,
            }),
            Err(keys) => Err(GraphError::InvalidGqlFilter(format!(
                "a node sort key must set exactly one of id/name/type/time/property, got {}",
                keys.len()
            ))),
        }
    }
}

impl TryFrom<EdgeSortBy> for sort::EdgeSortBy {
    type Error = GraphError;

    fn try_from(v: EdgeSortBy) -> Result<Self, Self::Error> {
        let mut keys: Vec<sort::EdgeSortKey> = Vec::new();
        if let Some(src) = v.src {
            keys.push(sort::EdgeSortKey::Src(src.try_into()?));
        }
        if let Some(dst) = v.dst {
            keys.push(sort::EdgeSortKey::Dst(dst.try_into()?));
        }
        if let Some(nbr) = v.neighbour {
            keys.push(sort::EdgeSortKey::Neighbour(nbr.try_into()?));
        }
        if let Some(t) = v.time {
            keys.push(sort::EdgeSortKey::Time(t.into()));
        }
        if let Some(p) = v.property {
            keys.push(sort::EdgeSortKey::Property(p));
        }
        match <[sort::EdgeSortKey; 1]>::try_from(keys) {
            Ok([key]) => Ok(sort::EdgeSortBy {
                reverse: v.reverse == Some(true),
                key,
            }),
            Err(keys) => Err(GraphError::InvalidGqlFilter(format!(
                "an edge sort key must set exactly one of src/dst/neighbour/time/property, got {}",
                keys.len()
            ))),
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
        let mut out = NodeSortBy {
            reverse: Some(v.reverse),
            id: None,
            name: None,
            type_: None,
            time: None,
            property: None,
        };
        match v.key {
            sort::NodeSortKey::Id => out.id = Some(true),
            sort::NodeSortKey::Name => out.name = Some(true),
            sort::NodeSortKey::Type => out.type_ = Some(true),
            sort::NodeSortKey::Time(t) => out.time = Some(t.into()),
            sort::NodeSortKey::Property(p) => out.property = Some(p),
        }
        out
    }
}

impl From<sort::EdgeSortBy> for EdgeSortBy {
    fn from(v: sort::EdgeSortBy) -> Self {
        let mut out = EdgeSortBy {
            reverse: Some(v.reverse),
            src: None,
            dst: None,
            neighbour: None,
            time: None,
            property: None,
        };
        match v.key {
            sort::EdgeSortKey::Src(k) => out.src = Some(k.into()),
            sort::EdgeSortKey::Dst(k) => out.dst = Some(k.into()),
            sort::EdgeSortKey::Neighbour(k) => out.neighbour = Some(k.into()),
            sort::EdgeSortKey::Time(t) => out.time = Some(t.into()),
            sort::EdgeSortKey::Property(p) => out.property = Some(p),
        }
        out
    }
}
