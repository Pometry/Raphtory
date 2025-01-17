use crate::core::{entities::properties::props::Meta, sort_comparable_props, utils::errors::GraphError, Prop};
use itertools::Itertools;
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

    pub fn apply_to_edge(&self, left: &EdgeFilterValue, right: Option<&str>) -> bool {
        match left {
            EdgeFilterValue::Single(l) => match self {
                FilterOperator::Eq | FilterOperator::Ne => {
                    right.map_or(false, |r| self.operation()(r, l))
                }
                _ => unreachable!(),
            },
            EdgeFilterValue::Set(l) => match self {
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
                let sorted_values = sort_comparable_props(values.iter().collect_vec());
                let values_str = sorted_values
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
                let mut sorted_values: Vec<_> = values.iter().collect();
                sorted_values.sort();
                let values_str = sorted_values
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

#[derive(Debug, Clone)]
pub enum EdgeFilterValue {
    Single(String),
    Set(Arc<HashSet<String>>),
}

#[derive(Debug, Clone)]
pub struct EdgeFilter {
    pub field_name: String,
    pub field_value: EdgeFilterValue,
    pub operator: FilterOperator,
}

impl fmt::Display for EdgeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.field_value {
            EdgeFilterValue::Single(value) => {
                write!(f, "{} {} {}", self.field_name, self.operator, value)
            }
            EdgeFilterValue::Set(values) => {
                let mut sorted_values: Vec<_> = values.iter().collect();
                sorted_values.sort();
                let values_str = sorted_values
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} {} [{}]", self.field_name, self.operator, values_str)
            }
        }
    }
}

impl EdgeFilter {
    pub fn eq(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: EdgeFilterValue::Single(field_value.into()),
            operator: FilterOperator::Eq,
        }
    }

    pub fn ne(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: EdgeFilterValue::Single(field_value.into()),
            operator: FilterOperator::Ne,
        }
    }

    pub fn any(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: EdgeFilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::In,
        }
    }

    pub fn not_any(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: EdgeFilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::NotIn,
        }
    }

    pub fn matches(&self, node_value: Option<&str>) -> bool {
        self.operator.apply_to_edge(&self.field_value, node_value)
    }
}

#[derive(Debug, Clone)]
pub enum CompositeEdgeFilter {
    Edge(NodeFilter),
    Property(PropertyFilter),
    And(Vec<CompositeEdgeFilter>),
    Or(Vec<CompositeEdgeFilter>),
}

impl fmt::Display for CompositeEdgeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeEdgeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::Edge(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::And(filters) => {
                let formatted = filters
                    .iter()
                    .map(|filter| format!("({})", filter))
                    .collect::<Vec<String>>()
                    .join(" AND ");
                write!(f, "{}", formatted)
            }
            CompositeEdgeFilter::Or(filters) => {
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
mod test_composite_filters {
    use crate::{
        core::Prop,
        db::graph::views::property_filter::{CompositeEdgeFilter, CompositeNodeFilter, NodeFilter},
        prelude::PropertyFilter,
    };

    #[test]
    fn test_composite_node_filter() {
        assert_eq!(
            "p2 == 2",
            CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64)).to_string()
        );

        assert_eq!(
            "((node_type NOT IN [fire_nation, water_tribe]) AND (p2 == 2) AND (p1 == 1) AND ((p3 <= 5) OR (p4 IN [2, 10]))) OR (node_name == pometry) OR (p5 == 9)",
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
                        CompositeNodeFilter::Property(PropertyFilter::any("p4", vec![Prop::U64(10), Prop::U64(2)]))
                    ]),
                ]),
                CompositeNodeFilter::Node(NodeFilter::eq("node_name", "pometry")),
                CompositeNodeFilter::Property(PropertyFilter::eq("p5", 9u64)),
            ])
            .to_string()
        );
    }

    #[test]
    fn test_composite_edge_filter() {
        assert_eq!(
            "p2 == 2",
            CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 2u64)).to_string()
        );

        assert_eq!(
            "((edge_type NOT IN [fire_nation, water_tribe]) AND (p2 == 2) AND (p1 == 1) AND ((p3 <= 5) OR (p4 IN [2, 10]))) OR (from == pometry) OR (p5 == 9)",
            CompositeEdgeFilter::Or(vec![
                CompositeEdgeFilter::And(vec![
                    CompositeEdgeFilter::Edge(NodeFilter::not_any(
                        "edge_type",
                        vec!["fire_nation".into(), "water_tribe".into()]
                    )),
                    CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                    CompositeEdgeFilter::Property(PropertyFilter::eq("p1", 1u64)),
                    CompositeEdgeFilter::Or(vec![
                        CompositeEdgeFilter::Property(PropertyFilter::le("p3", 5u64)),
                        CompositeEdgeFilter::Property(PropertyFilter::any("p4", vec![Prop::U64(10), Prop::U64(2)]))
                    ]),
                ]),
                CompositeEdgeFilter::Edge(NodeFilter::eq("from", "pometry")),
                CompositeEdgeFilter::Property(PropertyFilter::eq("p5", 9u64)),
            ])
                .to_string()
        );
    }
}
