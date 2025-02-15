use crate::model::graph::property::GqlPropValue;
use dynamic_graphql::{Enum, InputObject};

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
