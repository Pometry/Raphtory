use crate::model::graph::property::Value;
use async_graphql::dynamic::ValueAccessor;
use dynamic_graphql::{
    internal::{
        FromValue, GetInputTypeRef, InputTypeName, InputValueResult, Register, Registry, TypeName,
    },
    Enum, InputObject,
};
use raphtory::{
    db::graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter,
        filter_operator::FilterOperator,
        node_filter::CompositeNodeFilter,
        property_filter::{PropertyFilter, PropertyFilterValue, PropertyRef, Temporal},
        Filter, FilterValue,
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::{
    borrow::Cow,
    fmt,
    fmt::{Display, Formatter},
    ops::Deref,
    sync::Arc,
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
    pub node_filter: Option<NodeFilter>,
    pub edge_filter: Option<EdgeFilter>,
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
    pub node_filter: Option<NodeFilter>,
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
    IsIn,
    IsNotIn,
    Contains,
    NotContains,
}

#[derive(InputObject, Clone, Debug)]
pub struct NodeFilter {
    pub node: Option<NodeFieldFilter>,
    pub property: Option<PropertyFilterExpr>,
    pub constant_property: Option<ConstantPropertyFilterExpr>,
    pub temporal_property: Option<TemporalPropertyFilterExpr>,
    pub and: Option<Vec<NodeFilter>>,
    pub or: Option<Vec<NodeFilter>>,
    pub not: Option<Wrapped<NodeFilter>>,
}

#[derive(Clone, Debug)]
pub struct Wrapped<T>(Box<T>);

impl<T> Deref for Wrapped<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<T: Register + 'static> Register for Wrapped<T> {
    fn register(registry: Registry) -> Registry {
        registry.register::<T>()
    }
}

impl<T: FromValue + GetInputTypeRef + InputTypeName + 'static> FromValue for Wrapped<T> {
    fn from_value(value: async_graphql::Result<ValueAccessor>) -> InputValueResult<Self> {
        match T::from_value(value) {
            Ok(value) => Ok(Wrapped(Box::new(value))),
            Err(err) => Err(err.propagate()),
        }
    }
}

impl<T: TypeName + 'static> TypeName for Wrapped<T> {
    fn get_type_name() -> Cow<'static, str> {
        T::get_type_name()
    }
}

impl<T: InputTypeName + 'static> InputTypeName for Wrapped<T> {}

impl NodeFilter {
    pub fn validate(&self) -> Result<(), GraphError> {
        let fields_set = [
            self.node.is_some(),
            self.property.is_some(),
            self.constant_property.is_some(),
            self.temporal_property.is_some(),
            self.and.is_some(),
            self.or.is_some(),
            self.not.is_some(),
        ];

        let count = fields_set.iter().filter(|x| **x).count();

        match count {
            0 => Err(GraphError::InvalidGqlFilter("At least one field in NodeFilter must be provided.".to_string())),
            1 => Ok(()),
            _ => Err(GraphError::InvalidGqlFilter("Only one of node, property, constant_property, temporal_property, and/or must be provided.".to_string())),
        }
    }
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
    pub not: Option<Wrapped<EdgeFilter>>,
}

impl EdgeFilter {
    pub fn validate(&self) -> Result<(), GraphError> {
        let fields_set = [
            self.src.is_some(),
            self.dst.is_some(),
            self.property.is_some(),
            self.constant_property.is_some(),
            self.temporal_property.is_some(),
            self.and.is_some(),
            self.or.is_some(),
            self.not.is_some(),
        ];

        let count = fields_set.iter().filter(|x| **x).count();

        match count {
            0 => Err(GraphError::InvalidGqlFilter("At least one field in EdgeFilter must be provided.".to_string())),
            1 => Ok(()),
            _ => Err(GraphError::InvalidGqlFilter("Only one of src, dst, property, constant_property, temporal_property, and/or must be provided.".to_string())),
        }
    }
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

impl TryFrom<NodeFilter> for CompositeNodeFilter {
    type Error = GraphError;

    fn try_from(filter: NodeFilter) -> Result<Self, Self::Error> {
        let mut exprs = Vec::new();

        if let Some(node) = filter.node {
            exprs.push(CompositeNodeFilter::Node(Filter {
                field_name: node.field.to_string(),
                field_value: FilterValue::Single(node.value),
                operator: node.operator.into(),
            }));
        }

        if let Some(prop) = filter.property {
            exprs.push(CompositeNodeFilter::Property(prop.try_into()?));
        }

        if let Some(constant_prop) = filter.constant_property {
            exprs.push(CompositeNodeFilter::Property(constant_prop.try_into()?));
        }

        if let Some(temporal_prop) = filter.temporal_property {
            exprs.push(CompositeNodeFilter::Property(temporal_prop.try_into()?));
        }

        if let Some(and_filters) = filter.and {
            let mut iter = and_filters
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter();
            if let Some(first) = iter.next() {
                let and_chain = iter.fold(first, |acc, next| {
                    CompositeNodeFilter::And(Box::new(acc), Box::new(next))
                });
                exprs.push(and_chain);
            }
        }

        if let Some(or_filters) = filter.or {
            let mut iter = or_filters
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter();
            if let Some(first) = iter.next() {
                let or_chain = iter.fold(first, |acc, next| {
                    CompositeNodeFilter::Or(Box::new(acc), Box::new(next))
                });
                exprs.push(or_chain);
            }
        }

        if let Some(not_filters) = filter.not {
            let inner = CompositeNodeFilter::try_from(not_filters.deref().clone())?;
            exprs.push(CompositeNodeFilter::Not(Box::new(inner)));
        }

        let result = match exprs.len() {
            0 => Err(GraphError::ParsingError),
            1 => Ok(exprs.remove(0)),
            _ => {
                let mut iter = exprs.into_iter();
                let first = iter.next().unwrap();
                let and_chain = iter.fold(first, |acc, next| {
                    CompositeNodeFilter::And(Box::new(acc), Box::new(next))
                });
                Ok(and_chain)
            }
        };

        result
    }
}

impl TryFrom<EdgeFilter> for CompositeEdgeFilter {
    type Error = GraphError;

    fn try_from(filter: EdgeFilter) -> Result<Self, Self::Error> {
        let mut exprs = Vec::new();

        if let Some(src) = filter.src {
            exprs.push(CompositeEdgeFilter::Edge(Filter {
                field_name: "src".to_string(),
                field_value: FilterValue::Single(src.value),
                operator: src.operator.into(),
            }));
        }

        if let Some(dst) = filter.dst {
            exprs.push(CompositeEdgeFilter::Edge(Filter {
                field_name: "dst".to_string(),
                field_value: FilterValue::Single(dst.value),
                operator: dst.operator.into(),
            }));
        }

        if let Some(prop) = filter.property {
            exprs.push(CompositeEdgeFilter::Property(prop.try_into()?));
        }

        if let Some(prop) = filter.constant_property {
            exprs.push(CompositeEdgeFilter::Property(prop.try_into()?));
        }

        if let Some(prop) = filter.temporal_property {
            exprs.push(CompositeEdgeFilter::Property(prop.try_into()?));
        }

        if let Some(and_filters) = filter.and {
            let mut iter = and_filters
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter();

            if let Some(first) = iter.next() {
                let and_chain = iter.fold(first, |acc, next| {
                    CompositeEdgeFilter::And(Box::new(acc), Box::new(next))
                });
                exprs.push(and_chain);
            }
        }

        if let Some(or_filters) = filter.or {
            let mut iter = or_filters
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter();

            if let Some(first) = iter.next() {
                let or_chain = iter.fold(first, |acc, next| {
                    CompositeEdgeFilter::Or(Box::new(acc), Box::new(next))
                });
                exprs.push(or_chain);
            }
        }

        if let Some(not_filters) = filter.not {
            let inner = CompositeEdgeFilter::try_from(not_filters.deref().clone())?;
            exprs.push(CompositeEdgeFilter::Not(Box::new(inner)));
        }

        match exprs.len() {
            0 => Err(GraphError::ParsingError),
            1 => Ok(exprs.remove(0)),
            _ => {
                let mut iter = exprs.into_iter();
                let first = iter.next().unwrap();
                let and_chain = iter.fold(first, |acc, next| {
                    CompositeEdgeFilter::And(Box::new(acc), Box::new(next))
                });
                Ok(and_chain)
            }
        }
    }
}

fn build_property_filter(
    prop_ref: PropertyRef,
    operator: Operator,
    value: Option<Value>,
) -> Result<PropertyFilter, GraphError> {
    let prop = value.map(Prop::try_from).transpose()?;

    // Validates required value
    if matches!(
        operator,
        Operator::Equal
            | Operator::NotEqual
            | Operator::GreaterThan
            | Operator::LessThan
            | Operator::GreaterThanOrEqual
            | Operator::LessThanOrEqual
            | Operator::IsIn
            | Operator::IsNotIn
            | Operator::Contains
            | Operator::NotContains
    ) && prop.is_none()
    {
        return Err(GraphError::ExpectedValueForOperator(
            "value".into(),
            format!("{:?}", operator),
        ));
    }

    let prop_value = match (&prop, operator) {
        (Some(Prop::List(list)), Operator::IsIn | Operator::IsNotIn) => {
            PropertyFilterValue::Set(Arc::new(list.iter().cloned().collect()))
        }
        (Some(p), _) => PropertyFilterValue::Single(p.clone()),
        (None, _) => PropertyFilterValue::None,
    };

    Ok(PropertyFilter {
        prop_ref,
        prop_value,
        operator: operator.into(),
    })
}

impl TryFrom<PropertyFilterExpr> for PropertyFilter {
    type Error = GraphError;

    fn try_from(expr: PropertyFilterExpr) -> Result<Self, Self::Error> {
        build_property_filter(PropertyRef::Property(expr.name), expr.operator, expr.value)
    }
}

impl TryFrom<ConstantPropertyFilterExpr> for PropertyFilter {
    type Error = GraphError;

    fn try_from(expr: ConstantPropertyFilterExpr) -> Result<Self, Self::Error> {
        build_property_filter(
            PropertyRef::ConstantProperty(expr.name),
            expr.operator,
            expr.value,
        )
    }
}

impl TryFrom<TemporalPropertyFilterExpr> for PropertyFilter {
    type Error = GraphError;

    fn try_from(expr: TemporalPropertyFilterExpr) -> Result<Self, Self::Error> {
        build_property_filter(
            PropertyRef::TemporalProperty(expr.name, expr.temporal.into()),
            expr.operator,
            expr.value,
        )
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
            Operator::IsIn => FilterOperator::In,
            Operator::IsNotIn => FilterOperator::NotIn,
            Operator::IsSome => FilterOperator::IsSome,
            Operator::IsNone => FilterOperator::IsNone,
            Operator::Contains => FilterOperator::Contains,
            Operator::NotContains => FilterOperator::NotContains,
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
