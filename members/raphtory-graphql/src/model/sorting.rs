use dynamic_graphql::{Enum, InputObject};

#[derive(InputObject, Clone, Debug, Eq, PartialEq)]
pub struct EdgeSortBy {
    pub reverse: Option<bool>,
    pub src: Option<bool>,
    pub dst: Option<bool>,
    pub time: Option<SortByTime>,
    pub property: Option<String>,
}

#[derive(InputObject, Clone, Debug, Eq, PartialEq)]
pub struct NodeSortBy {
    pub reverse: Option<bool>,
    pub id: Option<bool>,
    pub time: Option<SortByTime>,
    pub property: Option<String>,
}

#[derive(Enum, Clone, Debug, Eq, PartialEq)]
pub enum SortByTime {
    Latest,
    Earliest,
}
