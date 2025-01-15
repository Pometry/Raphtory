use crate::core::{entities::properties::props::Meta, utils::errors::GraphError, Prop};
use std::{collections::HashSet, fmt};
use std::sync::Arc;

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
    pub fn compare(&self, left: &PropertyFilterValue, right: Option<&Prop>) -> bool {
        match left {
            PropertyFilterValue::None => match self {
                ComparisonOperator::IsSome => right.is_some(),
                ComparisonOperator::IsNone => right.is_none(),
                _ => unreachable!(),
            },
            PropertyFilterValue::Single(l) => match self {
                ComparisonOperator::Eq => right.map_or(false, |r| r == l),
                ComparisonOperator::Ne => right.map_or(false, |r| r != l),
                ComparisonOperator::Lt => right.map_or(false, |r| r < l),
                ComparisonOperator::Le => right.map_or(false, |r| r <= l),
                ComparisonOperator::Gt => right.map_or(false, |r| r > l),
                ComparisonOperator::Ge => right.map_or(false, |r| r >= l),
                _ => unreachable!(),
            },
            PropertyFilterValue::Set(l) => match self {
                ComparisonOperator::In => right.map_or(false, |r| l.contains(r)),
                ComparisonOperator::NotIn => right.map_or(false, |r| !l.contains(r)),
                _ => unreachable!(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum PropertyFilterValue {
    None,
    Single(Prop),
    Set(Arc<HashSet<Prop>>),
}

#[derive(Debug, Clone)]
pub struct PropertyFilter {
    pub prop_name: String,
    pub prop_value: PropertyFilterValue,
    pub operator: ComparisonOperator,
}

impl fmt::Display for PropertyFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.prop_value {
            PropertyFilterValue::None => {
                write!(f, "{} {}", self.prop_name, self.operator)
            }
            PropertyFilterValue::Single(value) => {
                write!(f, "{} {} {}", self.prop_name, self.operator, value)
            }
            PropertyFilterValue::Set(values) => {
                let values_str = values.iter().map(|v| format!("{}", v)).collect::<Vec<_>>().join(", ");
                write!(f, "{} {} [{}]", self.prop_name, self.operator, values_str)
            }
        }
    }
}

impl PropertyFilter {
    pub fn eq(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: ComparisonOperator::Eq,
        }
    }

    pub fn ne(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: ComparisonOperator::Ne,
        }
    }

    pub fn le(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: ComparisonOperator::Le,
        }
    }

    pub fn ge(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: ComparisonOperator::Ge,
        }
    }

    pub fn lt(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: ComparisonOperator::Lt,
        }
    }

    pub fn gt(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: ComparisonOperator::Gt,
        }
    }

    pub fn any(prop_name: impl Into<String>, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: ComparisonOperator::In,
        }
    }

    pub fn not_any(
        prop_name: impl Into<String>,
        prop_values: impl IntoIterator<Item = Prop>,
    ) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: ComparisonOperator::NotIn,
        }
    }

    pub fn is_none(prop_name: impl Into<String>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::None,
            operator: ComparisonOperator::IsNone,
        }
    }

    pub fn is_some(prop_name: impl Into<String>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::None,
            operator: ComparisonOperator::IsSome,
        }
    }

    pub fn resolve_temporal_prop_ids(&self, meta: &Meta) -> Result<Option<usize>, GraphError> {
        if let PropertyFilterValue::Single(value) = &self.prop_value {
            Ok(meta
                .temporal_prop_meta()
                .get_and_validate(&self.prop_name, value.dtype())?)
        } else {
            Ok(meta.temporal_prop_meta().get_id(&self.prop_name))
        }
    }

    pub fn resolve_constant_prop_ids(&self, meta: &Meta) -> Result<Option<usize>, GraphError> {
        if let PropertyFilterValue::Single(value) = &self.prop_value {
            Ok(meta
                .const_prop_meta()
                .get_and_validate(&self.prop_name, value.dtype())?)
        } else {
            Ok(meta.const_prop_meta().get_id(&self.prop_name))
        }
    }

    pub fn matches(&self, other: Option<&Prop>) -> bool {
        let value = &self.prop_value;
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

#[cfg(test)]
mod test_filters {
    use crate::{db::graph::views::property_filter::CompositeFilter, prelude::PropertyFilter};

    #[test]
    fn test_composite_filter() {
        assert_eq!(
            "p2 == 2",
            CompositeFilter::Single(PropertyFilter::eq("p2", 2u64)).to_string()
        );

        assert_eq!(
            "((p2 == 2) AND (p1 == 1) AND ((p3 <= 5) OR (p4 <= 1))) OR (p5 == 9)",
            CompositeFilter::Or(vec![
                CompositeFilter::And(vec![
                    CompositeFilter::Single(PropertyFilter::eq("p2", 2u64)),
                    CompositeFilter::Single(PropertyFilter::eq("p1", 1u64)),
                    CompositeFilter::Or(vec![
                        CompositeFilter::Single(PropertyFilter::le("p3", 5u64)),
                        CompositeFilter::Single(PropertyFilter::le("p4", 1u64))
                    ]),
                ]),
                CompositeFilter::Single(PropertyFilter::eq("p5", 9u64)),
            ])
            .to_string()
        );
    }
}
