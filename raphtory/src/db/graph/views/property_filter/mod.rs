use crate::core::{entities::properties::props::Meta, utils::errors::GraphError, Prop};
use std::{collections::HashSet, fmt, sync::Arc};

pub mod edge_property_filter;
pub mod exploded_edge_property_filter;
pub(crate) mod internal;
pub mod node_property_filter;

#[derive(Debug, Clone, Copy)]
pub enum FilterOperator {
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

impl fmt::Display for FilterOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let operator = match self {
            FilterOperator::Eq => "==",
            FilterOperator::Ne => "!=",
            FilterOperator::Lt => "<",
            FilterOperator::Le => "<=",
            FilterOperator::Gt => ">",
            FilterOperator::Ge => ">=",
            FilterOperator::In => "IN",
            FilterOperator::NotIn => "NOT IN",
            FilterOperator::IsSome => "IS SOME",
            FilterOperator::IsNone => "IS NONE",
        };
        write!(f, "{}", operator)
    }
}

impl FilterOperator {
    fn operation<T>(&self) -> impl Fn(&T, &T) -> bool
    where
        T: ?Sized + PartialEq + PartialOrd,
    {
        match self {
            FilterOperator::Eq => T::eq,
            FilterOperator::Ne => T::ne,
            FilterOperator::Lt => T::lt,
            FilterOperator::Le => T::le,
            FilterOperator::Gt => T::gt,
            FilterOperator::Ge => T::ge,
            _ => panic!("Operation not supported for this operator"),
        }
    }

    fn collection_operation<T>(&self) -> impl Fn(&HashSet<T>, &T) -> bool
    where
        T: Eq + std::hash::Hash,
    {
        match self {
            FilterOperator::In => |set: &HashSet<T>, value: &T| set.contains(value),
            FilterOperator::NotIn => |set: &HashSet<T>, value: &T| !set.contains(value),
            _ => panic!("Collection operation not supported for this operator"),
        }
    }

    pub fn apply_to_property(&self, left: &PropertyFilterValue, right: Option<&Prop>) -> bool {
        match left {
            PropertyFilterValue::None => match self {
                FilterOperator::IsSome => right.is_some(),
                FilterOperator::IsNone => right.is_none(),
                _ => unreachable!(),
            },
            PropertyFilterValue::Single(l) => match self {
                FilterOperator::Eq
                | FilterOperator::Ne
                | FilterOperator::Lt
                | FilterOperator::Le
                | FilterOperator::Gt
                | FilterOperator::Ge => right.map_or(false, |r| self.operation()(r, l)),
                _ => unreachable!(),
            },
            PropertyFilterValue::Set(l) => match self {
                FilterOperator::In | FilterOperator::NotIn => {
                    right.map_or(false, |r| self.collection_operation()(l, r))
                }
                _ => unreachable!(),
            },
        }
    }

    pub fn apply_to_node(&self, left: &NodeFilterValue, right: Option<&str>) -> bool {
        match left {
            NodeFilterValue::Single(l) => match self {
                FilterOperator::Eq | FilterOperator::Ne => {
                    right.map_or(false, |r| self.operation()(r, l))
                }
                _ => unreachable!(),
            },
            NodeFilterValue::Set(l) => match self {
                FilterOperator::In | FilterOperator::NotIn => {
                    right.map_or(false, |r| self.collection_operation()(l, &r.to_string()))
                }
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
    pub operator: FilterOperator,
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
                let values_str = values
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join(", ");
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
            operator: FilterOperator::Eq,
        }
    }

    pub fn ne(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ne,
        }
    }

    pub fn le(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Le,
        }
    }

    pub fn ge(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ge,
        }
    }

    pub fn lt(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Lt,
        }
    }

    pub fn gt(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Gt,
        }
    }

    pub fn any(prop_name: impl Into<String>, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::In,
        }
    }

    pub fn not_any(
        prop_name: impl Into<String>,
        prop_values: impl IntoIterator<Item = Prop>,
    ) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::NotIn,
        }
    }

    pub fn is_none(prop_name: impl Into<String>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsNone,
        }
    }

    pub fn is_some(prop_name: impl Into<String>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsSome,
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
        self.operator.apply_to_property(value, other)
    }
}

#[derive(Debug, Clone)]
pub enum NodeFilterValue {
    Single(String),
    Set(Arc<HashSet<String>>),
}

#[derive(Debug, Clone)]
pub struct NodeFilter {
    pub field_name: String,
    pub field_value: NodeFilterValue,
    pub operator: FilterOperator,
}

impl fmt::Display for NodeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.field_value {
            NodeFilterValue::Single(value) => {
                write!(f, "{} {} {}", self.field_name, self.operator, value)
            }
            NodeFilterValue::Set(values) => {
                let values_str = values
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} {} [{}]", self.field_name, self.operator, values_str)
            }
        }
    }
}

impl NodeFilter {
    pub fn eq(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: NodeFilterValue::Single(field_value.into()),
            operator: FilterOperator::Eq,
        }
    }

    pub fn ne(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: NodeFilterValue::Single(field_value.into()),
            operator: FilterOperator::Ne,
        }
    }

    pub fn any(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: NodeFilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::In,
        }
    }

    pub fn not_any(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: NodeFilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::NotIn,
        }
    }

    pub fn matches(&self, node_value: Option<&str>) -> bool {
        self.operator.apply_to_node(&self.field_value, node_value)
    }
}

#[derive(Debug, Clone)]
pub enum CompositeNodeFilter {
    Node(NodeFilter),
    Property(PropertyFilter),
    And(Vec<CompositeNodeFilter>),
    Or(Vec<CompositeNodeFilter>),
}

impl fmt::Display for CompositeNodeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeNodeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Node(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::And(filters) => {
                let formatted = filters
                    .iter()
                    .map(|filter| format!("({})", filter))
                    .collect::<Vec<String>>()
                    .join(" AND ");
                write!(f, "{}", formatted)
            }
            CompositeNodeFilter::Or(filters) => {
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
    use crate::{
        db::graph::views::property_filter::{CompositeNodeFilter, NodeFilter},
        prelude::PropertyFilter,
    };

    #[test]
    fn test_composite_filter() {
        assert_eq!(
            "p2 == 2",
            CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64)).to_string()
        );

        assert_eq!(
            "((node_type NOT IN [fire_nation, water_tribe]) AND (p2 == 2) AND (p1 == 1) AND ((p3 <= 5) OR (p4 <= 1))) OR (node_name == pometry) OR (p5 == 9)",
            CompositeNodeFilter::Or(vec![
                CompositeNodeFilter::And(vec![
                    CompositeNodeFilter::Node(NodeFilter::not_any(
                        "node_type",
                        vec!["fire_nation".into(), "water_tribe".into()]
                    )),
                    CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                    CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64)),
                    CompositeNodeFilter::Or(vec![
                        CompositeNodeFilter::Property(PropertyFilter::le("p3", 5u64)),
                        CompositeNodeFilter::Property(PropertyFilter::le("p4", 1u64))
                    ]),
                ]),
                CompositeNodeFilter::Node(NodeFilter::eq("node_name", "pometry")),
                CompositeNodeFilter::Property(PropertyFilter::eq("p5", 9u64)),
            ])
            .to_string()
        );
    }
}
