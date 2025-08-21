use dynamic_graphql::{Enum, InputObject};

#[derive(InputObject, Clone, Debug, Eq, PartialEq)]
pub struct EdgeSortBy {
    /// Reverse order
    pub reverse: Option<bool>,
    /// Source node
    pub src: Option<bool>,
    /// Destination
    pub dst: Option<bool>,
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
    /// Time
    pub time: Option<SortByTime>,
    /// Property
    pub property: Option<String>,
}

#[derive(Enum, Clone, Debug, Eq, PartialEq)]
pub enum SortByTime {
    /// Latest time
    Latest,
    /// Earliest time
    Earliest,
}
