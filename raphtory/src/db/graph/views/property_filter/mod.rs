use crate::core::{entities::properties::props::Meta, utils::errors::GraphError, Prop};
use std::fmt;

pub mod edge_property_filter;
pub mod exploded_edge_property_filter;
pub(crate) mod internal;
pub mod node_property_filter;

#[derive(Debug, Clone, Copy)]
pub enum ComparisonOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    NotIn,
    IsSome,
    IsNone,
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let operator = match self {
            ComparisonOperator::Eq => "==",
            ComparisonOperator::Ne => "!=",
            ComparisonOperator::Lt => "<",
            ComparisonOperator::Le => "<=",
            ComparisonOperator::Gt => ">",
            ComparisonOperator::Ge => ">=",
            ComparisonOperator::In => "IN",
            ComparisonOperator::NotIn => "NOT IN",
            ComparisonOperator::IsSome => "IS SOME",
            ComparisonOperator::IsNone => "IS NONE",
        };
        write!(f, "{}", operator)
    }
}

impl ComparisonOperator {
    pub fn is_binary_comparison(&self) -> bool {
        match self {
            ComparisonOperator::Eq
            | ComparisonOperator::Ne
            | ComparisonOperator::Lt
            | ComparisonOperator::Le
            | ComparisonOperator::Gt
            | ComparisonOperator::Ge => true,
            _ => false,
        }
    }

    pub fn compare(&self, left: Option<&Prop>, right: Option<&Prop>) -> bool {
        match self {
            ComparisonOperator::Eq => left.zip(right).map_or(false, |(l, r)| r == l),
            ComparisonOperator::Ne => left.zip(right).map_or(false, |(l, r)| r != l),
            ComparisonOperator::Lt => left.zip(right).map_or(false, |(l, r)| r < l),
            ComparisonOperator::Le => left.zip(right).map_or(false, |(l, r)| r <= l),
            ComparisonOperator::Gt => left.zip(right).map_or(false, |(l, r)| r > l),
            ComparisonOperator::Ge => left.zip(right).map_or(false, |(l, r)| r >= l),

            ComparisonOperator::In => right
                .and_then(|r| match r {
                    Prop::List(props) => Some(props),
                    _ => None,
                })
                .zip(left)
                .map_or(false, |(props, l)| props.contains(l)),
            ComparisonOperator::NotIn => right
                .and_then(|r| match r {
                    Prop::List(props) => Some(props),
                    _ => None,
                })
                .zip(left)
                .map_or(true, |(props, l)| !props.contains(l)),

            ComparisonOperator::IsSome => right.is_some(),
            ComparisonOperator::IsNone => right.is_none(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PropertyFilter {
    pub prop_name: String,
    pub prop_value: Option<Prop>,
    pub operator: ComparisonOperator,
}

impl fmt::Display for PropertyFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.prop_value {
            Some(value) => write!(f, "{} {} {}", self.prop_name, self.operator, value),
            None => write!(f, "{} {}", self.prop_name, self.operator),
        }
    }
}

impl PropertyFilter {
    pub fn eq(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: Some(prop_value.into()),
            operator: ComparisonOperator::Eq,
        }
    }

    pub fn ne(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: Some(prop_value.into()),
            operator: ComparisonOperator::Ne,
        }
    }

    pub fn le(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: Some(prop_value.into()),
            operator: ComparisonOperator::Le,
        }
    }

    pub fn ge(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: Some(prop_value.into()),
            operator: ComparisonOperator::Ge,
        }
    }

    pub fn lt(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: Some(prop_value.into()),
            operator: ComparisonOperator::Lt,
        }
    }

    pub fn gt(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: Some(prop_value.into()),
            operator: ComparisonOperator::Gt,
        }
    }

    pub fn any(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: Some(prop_value.into()),
            operator: ComparisonOperator::In,
        }
    }

    pub fn not_any(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: Some(prop_value.into()),
            operator: ComparisonOperator::NotIn,
        }
    }

    pub fn is_none(prop_name: impl Into<String>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: None,
            operator: ComparisonOperator::IsNone,
        }
    }

    pub fn is_some(prop_name: impl Into<String>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: None,
            operator: ComparisonOperator::IsSome,
        }
    }

    pub fn resolve_temporal_prop_ids(&self, meta: &Meta) -> Result<Option<usize>, GraphError> {
        if self.operator.is_binary_comparison() {
            if let Some(value) = &self.prop_value {
                Ok(meta
                    .temporal_prop_meta()
                    .get_and_validate(&self.prop_name, value.dtype())?)
            } else {
                Err(GraphError::InvalidFilter(self.operator))
            }
        } else {
            Ok(meta.temporal_prop_meta().get_id(&self.prop_name))
        }
    }

    pub fn resolve_constant_prop_ids(&self, meta: &Meta) -> Result<Option<usize>, GraphError> {
        if self.operator.is_binary_comparison() {
            if let Some(value) = &self.prop_value {
                Ok(meta
                    .const_prop_meta()
                    .get_and_validate(&self.prop_name, value.dtype())?)
            } else {
                Err(GraphError::InvalidFilter(self.operator))
            }
        } else {
            Ok(meta.const_prop_meta().get_id(&self.prop_name))
        }
    }

    pub fn matches(&self, other: Option<&Prop>) -> bool {
        let value = self.prop_value.as_ref();
        self.operator.compare(value, other)
    }
}

#[derive(Debug, Clone)]
pub enum CompositeFilter {
    Single(PropertyFilter),
    And(Vec<CompositeFilter>),
    Or(Vec<CompositeFilter>),
}

impl fmt::Display for CompositeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeFilter::Single(filter) => write!(f, "{}", filter),
            CompositeFilter::And(filters) => {
                let formatted = filters
                    .iter()
                    .map(|filter| format!("({})", filter))
                    .collect::<Vec<String>>()
                    .join(" AND ");
                write!(f, "{}", formatted)
            }
            CompositeFilter::Or(filters) => {
                let formatted = filters
                    .iter()
                    .map(|filter| format!("({})", filter))
                    .collect::<Vec<String>>()
                    .join(" OR ");
                write!(f, "{}", formatted)
            }
        }
    }
}

impl CompositeFilter {
    pub fn single(filter: PropertyFilter) -> Self {
        Self::Single(filter)
    }

    pub fn and(filters: impl IntoIterator<Item =CompositeFilter>) -> Self {
        Self::And(filters.into_iter().collect())
    }

    pub fn or(filters: impl IntoIterator<Item =CompositeFilter>) -> Self {
        Self::Or(filters.into_iter().collect())
    }
}

#[cfg(test)]
mod test_filters {
    use crate::{
        db::graph::views::property_filter::CompositeFilter, prelude::PropertyFilter,
    };

    #[test]
    fn test_composite_filter() {
        assert_eq!(
            "p2 == 2",
            CompositeFilter::single(PropertyFilter::eq("p2", 2u64)).to_string()
        );

        assert_eq!(
            "((p2 == 2) AND (p1 == 1) AND ((p3 <= 5) OR (p4 <= 1))) OR (p5 == 9)",
            CompositeFilter::Or(vec![
                CompositeFilter::And(vec![
                    CompositeFilter::single(PropertyFilter::eq("p2", 2u64)),
                    CompositeFilter::single(PropertyFilter::eq("p1", 1u64)),
                    CompositeFilter::Or(vec![
                        CompositeFilter::single(PropertyFilter::le("p3", 5u64)),
                        CompositeFilter::single(PropertyFilter::le("p4", 1u64))
                    ]),
                ]),
                CompositeFilter::single(PropertyFilter::eq("p5", 9u64)),
            ])
            .to_string()
        );
    }
}
