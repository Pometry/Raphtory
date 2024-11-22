use crate::model::graph::property::GqlPropValue;
use dynamic_graphql::{Enum, InputObject};

pub(crate) mod edge;
mod edges;
pub(crate) mod graph;
pub(crate) mod graphs;
pub(crate) mod mutable_graph;
pub(crate) mod node;
mod nodes;
mod path_from_node;
pub(crate) mod property;
pub(crate) mod vectorised_graph;

#[derive(InputObject, Debug)]
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
