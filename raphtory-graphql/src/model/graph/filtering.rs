use crate::model::graph::property::Value;
use async_graphql::dynamic::ValueAccessor;
use dynamic_graphql::{
    internal::{
        FromValue, GetInputTypeRef, InputTypeName, InputValueResult, Register, Registry, TypeName,
    },
    Enum, InputObject, OneOfInput,
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
    pub subgraph_node_types: Option<Vec<String>>,
    pub exclude_nodes: Option<Vec<String>>,
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

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let op_str = match self {
            Operator::Equal => "EQUAL",
            Operator::NotEqual => "NOT_EQUAL",
            Operator::GreaterThanOrEqual => "GREATER_THAN_OR_EQUAL",
            Operator::LessThanOrEqual => "LESS_THAN_OR_EQUAL",
            Operator::GreaterThan => "GREATER_THAN",
            Operator::LessThan => "LESS_THAN",
            Operator::IsNone => "IS_NONE",
            Operator::IsSome => "IS_SOME",
            Operator::IsIn => "IS_IN",
            Operator::IsNotIn => "IS_NOT_IN",
            Operator::Contains => "CONTAINS",
            Operator::NotContains => "NOT_CONTAINS",
        };
        write!(f, "{op_str}")
    }
}

#[derive(OneOfInput, Clone, Debug)]
pub enum NodeFilter {
    Node(NodeFieldFilter),
    Property(PropertyFilterExpr),
    ConstantProperty(ConstantPropertyFilterExpr),
    TemporalProperty(TemporalPropertyFilterExpr),
    And(Vec<NodeFilter>),
    Or(Vec<NodeFilter>),
    Not(Wrapped<NodeFilter>),
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

#[derive(InputObject, Clone, Debug)]
pub struct NodeFieldFilter {
    pub field: NodeField,
    pub operator: Operator,
    pub value: Value,
}

impl NodeFieldFilter {
    pub fn validate(&self) -> Result<(), GraphError> {
        validate_operator_value_pair(self.operator, &Some(self.value.clone()))
    }
}

#[derive(Enum, Copy, Clone, Debug)]
pub enum NodeField {
    NodeName,
    NodeType,
}

#[derive(OneOfInput, Clone, Debug)]
pub enum EdgeFilter {
    Src(NodeFieldFilter),
    Dst(NodeFieldFilter),
    Property(PropertyFilterExpr),
    ConstantProperty(ConstantPropertyFilterExpr),
    TemporalProperty(TemporalPropertyFilterExpr),
    And(Vec<EdgeFilter>),
    Or(Vec<EdgeFilter>),
    Not(Wrapped<EdgeFilter>),
}

#[derive(InputObject, Clone, Debug)]
pub struct PropertyFilterExpr {
    pub name: String,
    pub operator: Operator,
    pub value: Option<Value>,
}

impl PropertyFilterExpr {
    pub fn validate(&self) -> Result<(), GraphError> {
        validate_operator_value_pair(self.operator, &self.value)
    }
}

#[derive(InputObject, Clone, Debug)]
pub struct ConstantPropertyFilterExpr {
    pub name: String,
    pub operator: Operator,
    pub value: Option<Value>,
}

impl ConstantPropertyFilterExpr {
    pub fn validate(&self) -> Result<(), GraphError> {
        validate_operator_value_pair(self.operator, &self.value)
    }
}

#[derive(InputObject, Clone, Debug)]
pub struct TemporalPropertyFilterExpr {
    pub name: String,
    pub temporal: TemporalType,
    pub operator: Operator,
    pub value: Option<Value>,
}

impl TemporalPropertyFilterExpr {
    pub fn validate(&self) -> Result<(), GraphError> {
        validate_operator_value_pair(self.operator, &self.value)
    }
}

#[derive(Enum, Copy, Clone, Debug)]
pub enum TemporalType {
    Any,
    Latest,
}

fn field_value(value: Value, operator: Operator) -> Result<FilterValue, GraphError> {
    let prop = Prop::try_from(value.clone())?;
    match (prop, operator) {
        (Prop::List(list), Operator::IsIn | Operator::IsNotIn) => {
            let strings: Vec<String> = list
                .iter()
                .map(|p| match p {
                    Prop::Str(s) => Ok(s.to_string()),
                    _ => Err(GraphError::InvalidGqlFilter(format!(
                        "Invalid field value {:?} or operator {}",
                        value, operator
                    ))),
                })
                .collect::<Result<_, _>>()?;

            Ok(FilterValue::Set(Arc::new(
                strings.iter().cloned().collect(),
            )))
        }
        (Prop::Str(p), _) => Ok(FilterValue::Single(p.to_string())),
        _ => Err(GraphError::InvalidGqlFilter(format!(
            "Invalid field value {:?} or operator {}",
            value, operator
        ))),
    }
}

impl TryFrom<NodeFilter> for CompositeNodeFilter {
    type Error = GraphError;

    fn try_from(filter: NodeFilter) -> Result<Self, Self::Error> {
        let mut exprs = Vec::new();

        if let NodeFilter::Node(node) = filter.clone() {
            node.validate()?;
            exprs.push(CompositeNodeFilter::Node(Filter {
                field_name: node.field.to_string(),
                field_value: field_value(node.value, node.operator)?,
                operator: node.operator.into(),
            }));
        }

        if let NodeFilter::Property(prop) = filter.clone() {
            prop.validate()?;
            exprs.push(CompositeNodeFilter::Property(prop.try_into()?));
        }

        if let NodeFilter::ConstantProperty(prop) = filter.clone() {
            prop.validate()?;
            exprs.push(CompositeNodeFilter::Property(prop.try_into()?));
        }

        if let NodeFilter::TemporalProperty(prop) = filter.clone() {
            prop.validate()?;
            exprs.push(CompositeNodeFilter::Property(prop.try_into()?));
        }

        if let NodeFilter::And(and_filters) = filter.clone() {
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

        if let NodeFilter::Or(or_filters) = filter.clone() {
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

        if let NodeFilter::Not(not_filters) = filter.clone() {
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

        if let EdgeFilter::Src(src) = filter.clone() {
            src.validate()?;
            exprs.push(CompositeEdgeFilter::Edge(Filter {
                field_name: "src".to_string(),
                field_value: field_value(src.value, src.operator)?,
                operator: src.operator.into(),
            }));
        }

        if let EdgeFilter::Dst(dst) = filter.clone() {
            dst.validate()?;
            exprs.push(CompositeEdgeFilter::Edge(Filter {
                field_name: "dst".to_string(),
                field_value: field_value(dst.value, dst.operator)?,
                operator: dst.operator.into(),
            }));
        }

        if let EdgeFilter::Property(prop) = filter.clone() {
            prop.validate()?;
            exprs.push(CompositeEdgeFilter::Property(prop.try_into()?));
        }

        if let EdgeFilter::ConstantProperty(prop) = filter.clone() {
            prop.validate()?;
            exprs.push(CompositeEdgeFilter::Property(prop.try_into()?));
        }

        if let EdgeFilter::TemporalProperty(prop) = filter.clone() {
            prop.validate()?;
            exprs.push(CompositeEdgeFilter::Property(prop.try_into()?));
        }

        if let EdgeFilter::And(and_filters) = filter.clone() {
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

        if let EdgeFilter::Or(or_filters) = filter.clone() {
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

        if let EdgeFilter::Not(not_filters) = filter.clone() {
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
    let prop = value.clone().map(Prop::try_from).transpose()?;

    validate_operator_value_pair(operator, &value)?;

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

fn validate_operator_value_pair(
    operator: Operator,
    value: &Option<Value>,
) -> Result<(), GraphError> {
    use Operator::*;

    match operator {
        IsSome | IsNone => {
            if value.is_some() {
                Err(GraphError::InvalidGqlFilter(format!(
                    "Operator {operator} does not accept a value"
                )))
            } else {
                Ok(())
            }
        }

        IsIn | IsNotIn => match value {
            Some(Value::List(_)) => Ok(()),
            Some(v) => Err(GraphError::InvalidGqlFilter(format!(
                "Operator {operator} requires a list value, got {v}"
            ))),
            None => Err(GraphError::InvalidGqlFilter(format!(
                "Operator {operator} requires a non-empty list"
            ))),
        },

        Contains | NotContains => match value {
            Some(Value::Str(_)) => Ok(()),
            Some(v) => Err(GraphError::InvalidGqlFilter(format!(
                "Operator {operator} requires a string value, got {v}"
            ))),
            None => Err(GraphError::InvalidGqlFilter(format!(
                "Operator {operator} requires a string value"
            ))),
        },

        Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => {
            if value.is_none() {
                return Err(GraphError::InvalidGqlFilter(format!(
                    "Operator {operator} requires a value"
                )));
            }

            Ok(())
        }
    }
}
