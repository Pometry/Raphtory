use crate::model::graph::property::GqlPropValue;
use dynamic_graphql::{Enum, InputObject};

#[derive(InputObject, Clone, Debug)]
pub struct FilterCollection {
    pub default_layer: Option<bool>,
    pub layers: Option<Vec<String>>,
    pub exclude_layers: Option<Vec<String>>,
    pub layer: Option<String>,
    pub exclude_layer: Option<String>,
    pub subgraph: Option<Vec<String>>,
    pub subgraph_id: Option<Vec<u64>>,
    pub subgraph_node_types: Option<Vec<String>>,
    pub exclude_nodes: Option<Vec<String>>,
    pub exclude_nodes_id: Option<Vec<u64>>,
    pub window: Option<FilterWindow>,
    pub at: Option<i64>,
    pub latest: Option<bool>,
    pub snapshot_latest: Option<bool>,
    pub before: Option<i64>,
    pub after: Option<i64>,
    pub shrink_window: Option<FilterWindow>,
    pub shrink_start: Option<i64>,
    pub shrink_end: Option<i64>,
    pub node_filter: Option<FilterProperty>,
    pub edge_filter: Option<FilterProperty>,
}

#[derive(InputObject, Clone, Debug)]
pub struct FilterWindow {
    pub start: i64,
    pub end: i64,
}

#[derive(InputObject, Clone, Debug)]
pub struct FilterProperty {
    pub property: String,
    pub condition: FilterCondition,
}
#[derive(InputObject, Clone, Debug)]
pub struct FilterCondition {
    pub operator: Operator,
    pub value: Option<GqlPropValue>,
}

#[derive(Enum, Copy, Clone, Debug)]
pub enum Operator {
    Equal,
    NotEqual,
    GreaterThanOrEqual,
    LessThanOrEqual,
    GreaterThan,
    LessThan,
    IsNone,
    IsSome,
    Any,
    NotAny,
}
