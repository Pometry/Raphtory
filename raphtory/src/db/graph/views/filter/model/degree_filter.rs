use crate::db::graph::views::filter::model::{
    property_filter::{Op, PropertyFilter, PropertyFilterValue, PropertyRef},
    ComposableFilter, FilterOperator, NodeFilter,
};
use raphtory_api::core::Direction;
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DegreeFilter {
    pub direction: Direction,
    pub operator: FilterOperator,
    pub value: PropertyFilterValue,
    pub ops: Vec<Op>,
}

fn property_ref(direction: &Direction) -> PropertyRef {
    match direction {
        Direction::IN => PropertyRef::Property("in_degree".to_string()),
        Direction::OUT => PropertyRef::Property("out_degree".to_string()),
        Direction::BOTH => PropertyRef::Property("degree".to_string()),
    }
}

impl ComposableFilter for DegreeFilter {}

impl Display for DegreeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let property_filter = PropertyFilter {
            prop_ref: property_ref(&self.direction),
            prop_value: self.value.clone(),
            operator: self.operator,
            ops: self.ops.clone(),
            entity: NodeFilter,
        };
        property_filter.fmt(f)
    }
}
