use crate::model::graph::property::Value;
use dynamic_graphql::{Enum, InputObject};
use raphtory::{
    core::{utils::errors::GraphError, Prop},
    db::graph::views::property_filter::{
        Filter, FilterExpr, FilterOperator, FilterValue, PropertyFilterValue, PropertyRef, Temporal,
    },
    prelude::PropertyFilter,
};
use std::{
    fmt,
    fmt::{Display, Formatter},
};

#[derive(InputObject, Clone, Debug)]
pub struct GraphViewCollection {
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
    pub window: Option<Window>,
    pub at: Option<i64>,
    pub latest: Option<bool>,
    pub snapshot_at: Option<i64>,
    pub snapshot_latest: Option<bool>,
    pub before: Option<i64>,
    pub after: Option<i64>,
    pub shrink_window: Option<Window>,
    pub shrink_start: Option<i64>,
    pub shrink_end: Option<i64>,
    pub node_filter: Option<FilterProperty>,
    pub edge_filter: Option<FilterProperty>,
}

#[derive(InputObject, Clone, Debug)]
pub struct NodesViewCollection {
    pub default_layer: Option<bool>,
    pub layers: Option<Vec<String>>,
    pub exclude_layers: Option<Vec<String>>,
    pub layer: Option<String>,
    pub exclude_layer: Option<String>,
    pub window: Option<Window>,
    pub at: Option<i64>,
    pub latest: Option<bool>,
    pub snapshot_at: Option<i64>,
    pub snapshot_latest: Option<bool>,
    pub before: Option<i64>,
    pub after: Option<i64>,
    pub shrink_window: Option<Window>,
    pub shrink_start: Option<i64>,
    pub shrink_end: Option<i64>,
    pub type_filter: Option<Vec<String>>,
    pub node_filter: Option<FilterProperty>,
}

#[derive(InputObject, Clone, Debug)]
pub struct NodeViewCollection {
    pub default_layer: Option<bool>,
    pub layers: Option<Vec<String>>,
    pub exclude_layers: Option<Vec<String>>,
    pub layer: Option<String>,
    pub exclude_layer: Option<String>,
    pub window: Option<Window>,
    pub at: Option<i64>,
    pub latest: Option<bool>,
    pub snapshot_at: Option<i64>,
    pub snapshot_latest: Option<bool>,
    pub before: Option<i64>,
    pub after: Option<i64>,
    pub shrink_window: Option<Window>,
    pub shrink_start: Option<i64>,
    pub shrink_end: Option<i64>,
}

#[derive(InputObject, Clone, Debug)]
pub struct EdgesViewCollection {
    pub default_layer: Option<bool>,
    pub layers: Option<Vec<String>>,
    pub exclude_layers: Option<Vec<String>>,
    pub layer: Option<String>,
    pub exclude_layer: Option<String>,
    pub window: Option<Window>,
    pub at: Option<i64>,
    pub latest: Option<bool>,
    pub snapshot_at: Option<i64>,
    pub snapshot_latest: Option<bool>,
    pub before: Option<i64>,
    pub after: Option<i64>,
    pub shrink_window: Option<Window>,
    pub shrink_start: Option<i64>,
    pub shrink_end: Option<i64>,
}

#[derive(InputObject, Clone, Debug)]
pub struct EdgeViewCollection {
    pub default_layer: Option<bool>,
    pub layers: Option<Vec<String>>,
    pub exclude_layers: Option<Vec<String>>,
    pub layer: Option<String>,
    pub exclude_layer: Option<String>,
    pub window: Option<Window>,
    pub at: Option<i64>,
    pub latest: Option<bool>,
    pub snapshot_at: Option<i64>,
    pub snapshot_latest: Option<bool>,
    pub before: Option<i64>,
    pub after: Option<i64>,
    pub shrink_window: Option<Window>,
    pub shrink_start: Option<i64>,
    pub shrink_end: Option<i64>,
}

#[derive(InputObject, Clone, Debug)]
pub struct Window {
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
    pub value: Option<Value>,
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

#[derive(InputObject, Clone, Debug)]
pub struct NodeFilter {
    pub node: Option<NodeFieldFilter>,
    pub property: Option<PropertyFilterExpr>,
    pub constant_property: Option<ConstantPropertyFilterExpr>,
    pub temporal_property: Option<TemporalPropertyFilterExpr>,
    pub and: Option<Vec<NodeFilter>>,
    pub or: Option<Vec<NodeFilter>>,
}

#[derive(InputObject, Clone, Debug)]
pub struct NodeFieldFilter {
    pub field: NodeField,
    pub operator: Operator,
    pub value: String,
}

#[derive(Enum, Copy, Clone, Debug)]
pub enum NodeField {
    NodeName,
    NodeType,
}

#[derive(InputObject, Clone, Debug)]
pub struct EdgeFilter {
    pub src: Option<NodeFieldFilter>,
    pub dst: Option<NodeFieldFilter>,
    pub property: Option<PropertyFilterExpr>,
    pub constant_property: Option<ConstantPropertyFilterExpr>,
    pub temporal_property: Option<TemporalPropertyFilterExpr>,
    pub and: Option<Vec<EdgeFilter>>,
    pub or: Option<Vec<EdgeFilter>>,
}

#[derive(InputObject, Clone, Debug)]
pub struct PropertyFilterExpr {
    pub name: String,
    pub operator: Operator,
    pub value: Option<Value>,
}

#[derive(InputObject, Clone, Debug)]
pub struct ConstantPropertyFilterExpr {
    pub name: String,
    pub operator: Operator,
    pub value: Option<Value>,
}

#[derive(InputObject, Clone, Debug)]
pub struct TemporalPropertyFilterExpr {
    pub name: String,
    pub temporal: TemporalType,
    pub operator: Operator,
    pub value: Option<Value>,
}

#[derive(Enum, Copy, Clone, Debug)]
pub enum TemporalType {
    Any,
    Latest,
}

impl TryFrom<NodeFilter> for FilterExpr {
    type Error = GraphError;
    fn try_from(node_filter: NodeFilter) -> Result<Self, Self::Error> {
        let NodeFilter {
            node,
            property,
            constant_property,
            temporal_property,
            and,
            or,
        } = node_filter;

        let mut exprs = Vec::new();

        if let Some(node) = node {
            exprs.push(FilterExpr::Node(Filter {
                field_name: node.field.to_string(),
                field_value: FilterValue::Single(node.value),
                operator: node.operator.into(),
            }));
        }

        if let Some(property) = property {
            exprs.push(FilterExpr::Property(property.try_into()?));
        }

        if let Some(constant_property) = constant_property {
            exprs.push(FilterExpr::Property(constant_property.try_into()?));
        }

        if let Some(temporal_property) = temporal_property {
            exprs.push(FilterExpr::Property(temporal_property.try_into()?));
        }

        if let Some(and) = and {
            exprs.push(FilterExpr::And(
                and.into_iter()
                    .map(FilterExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ));
        }

        if let Some(or) = or {
            exprs.push(FilterExpr::Or(
                or.into_iter()
                    .map(FilterExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ));
        }

        Ok(match exprs.len() {
            1 => exprs.into_iter().next().unwrap(),
            _ => FilterExpr::And(exprs),
        })
    }
}

impl TryFrom<EdgeFilter> for FilterExpr {
    type Error = GraphError;
    fn try_from(edge_filter: EdgeFilter) -> Result<Self, Self::Error> {
        let EdgeFilter {
            src,
            dst,
            property,
            constant_property,
            temporal_property,
            and,
            or,
        } = edge_filter;

        let mut exprs = Vec::new();

        if let Some(src) = src {
            exprs.push(FilterExpr::Edge(Filter {
                field_name: "from".to_string(),
                field_value: FilterValue::Single(src.value),
                operator: src.operator.into(),
            }));
        }

        if let Some(dst) = dst {
            exprs.push(FilterExpr::Edge(Filter {
                field_name: "to".to_string(),
                field_value: FilterValue::Single(dst.value),
                operator: dst.operator.into(),
            }));
        }

        if let Some(property) = property {
            exprs.push(FilterExpr::Property(property.try_into()?));
        }

        if let Some(constant_property) = constant_property {
            exprs.push(FilterExpr::Property(constant_property.try_into()?));
        }

        if let Some(temporal_property) = temporal_property {
            exprs.push(FilterExpr::Property(temporal_property.try_into()?));
        }

        if let Some(and) = and {
            exprs.push(FilterExpr::And(
                and.into_iter()
                    .map(FilterExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ));
        }

        if let Some(or) = or {
            exprs.push(FilterExpr::Or(
                or.into_iter()
                    .map(FilterExpr::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ));
        }

        Ok(match exprs.len() {
            1 => exprs.into_iter().next().unwrap(),
            _ => FilterExpr::And(exprs),
        })
    }
}

impl TryFrom<PropertyFilterExpr> for PropertyFilter {
    type Error = GraphError;

    fn try_from(expr: PropertyFilterExpr) -> Result<Self, Self::Error> {
        let prop = expr.value.map(Prop::try_from).transpose()?;
        Ok(PropertyFilter {
            prop_ref: PropertyRef::Property(expr.name),
            prop_value: prop.into(),
            operator: expr.operator.into(),
        })
    }
}

impl TryFrom<ConstantPropertyFilterExpr> for PropertyFilter {
    type Error = GraphError;

    fn try_from(expr: ConstantPropertyFilterExpr) -> Result<Self, Self::Error> {
        let prop = expr.value.map(Prop::try_from).transpose()?;
        Ok(PropertyFilter {
            prop_ref: PropertyRef::ConstantProperty(expr.name),
            prop_value: prop.into(),
            operator: expr.operator.into(),
        })
    }
}

impl TryFrom<TemporalPropertyFilterExpr> for PropertyFilter {
    type Error = GraphError;
    fn try_from(expr: TemporalPropertyFilterExpr) -> Result<Self, Self::Error> {
        let prop = expr.value.map(Prop::try_from).transpose()?;
        Ok(PropertyFilter {
            prop_ref: PropertyRef::TemporalProperty(expr.name, expr.temporal.into()),
            prop_value: prop.into(),
            operator: expr.operator.into(),
        })
    }
}

impl Display for NodeField {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let field_name = match self {
            NodeField::NodeName => "node_name",
            NodeField::NodeType => "node_type",
        };
        write!(f, "{}", field_name)
    }
}

impl From<Operator> for FilterOperator {
    fn from(op: Operator) -> Self {
        match op {
            Operator::Equal => FilterOperator::Eq,
            Operator::NotEqual => FilterOperator::Ne,
            Operator::GreaterThanOrEqual => FilterOperator::Ge,
            Operator::LessThanOrEqual => FilterOperator::Le,
            Operator::GreaterThan => FilterOperator::Gt,
            Operator::LessThan => FilterOperator::Lt,
            Operator::Any => FilterOperator::In,
            Operator::NotAny => FilterOperator::NotIn,
            Operator::IsSome => FilterOperator::IsSome,
            Operator::IsNone => FilterOperator::IsNone,
        }
    }
}

impl From<TemporalType> for Temporal {
    fn from(temporal: TemporalType) -> Self {
        match temporal {
            TemporalType::Any => Temporal::Any,
            TemporalType::Latest => Temporal::Latest,
        }
    }
}
