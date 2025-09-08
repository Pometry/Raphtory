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

#[derive(OneOfInput, Clone, Debug)]
pub enum GraphViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single included layer.
    Layer(String),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Subgraph nodes.
    Subgraph(Vec<String>),
    /// Subgraph node types.
    SubgraphNodeTypes(Vec<String>),
    /// List of excluded nodes.
    ExcludeNodes(Vec<String>),
    /// Valid state.
    Valid(bool),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(i64),
    /// View at the latest time.
    Latest(bool),
    /// Snapshot at specified time.
    SnapshotAt(i64),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// View before a specified time (end exclusive).
    Before(i64),
    /// View after a specified time (start exclusive).
    After(i64),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(i64),
    /// Set the window end to a specified time.
    ShrinkEnd(i64),
    /// Node filter.
    NodeFilter(NodeFilter),
    /// Edge filter.
    EdgeFilter(EdgeFilter),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum NodesViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// View at the latest time.
    Latest(bool),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single included layer.
    Layer(String),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(i64),
    /// Snapshot at specified time.
    SnapshotAt(i64),
    /// View before a specified time (end exclusive).
    Before(i64),
    /// View after a specified time (start exclusive).
    After(i64),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(i64),
    /// Set the window end to a specified time.
    ShrinkEnd(i64),
    /// Node filter.
    NodeFilter(NodeFilter),
    /// List of types.
    TypeFilter(Vec<String>),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum NodeViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// View at the latest time.
    Latest(bool),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// Snapshot at specified time.
    SnapshotAt(i64),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single included layer.
    Layer(String),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(i64),
    /// View before a specified time (end exclusive).
    Before(i64),
    /// View after a specified time (start exclusive).
    After(i64),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(i64),
    /// Set the window end to a specified time.
    ShrinkEnd(i64),
    /// Node filter.
    NodeFilter(NodeFilter),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum EdgesViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// Latest time.
    Latest(bool),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// Snapshot at specified time.
    SnapshotAt(i64),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single included layer.
    Layer(String),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(i64),
    /// View before a specified time (end exclusive).
    Before(i64),
    /// View after a specified time (start exclusive).
    After(i64),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(i64),
    /// Set the window end to a specified time.
    ShrinkEnd(i64),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum EdgeViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// Latest time.
    Latest(bool),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// Snapshot at specified time.
    SnapshotAt(i64),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single included layer.
    Layer(String),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(i64),
    /// View before a specified time (end exclusive).
    Before(i64),
    /// View after a specified time (start exclusive).
    After(i64),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(i64),
    /// Set the window end to a specified time.
    ShrinkEnd(i64),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum PathFromNodeViewCollection {
    /// Latest time.
    Latest(bool),
    /// Latest snapshot.
    SnapshotLatest(bool),
    /// Time.
    SnapshotAt(i64),
    /// List of layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single layer.
    Layer(String),
    /// Single layer to exclude.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(i64),
    /// View before a specified time (end exclusive).
    Before(i64),
    /// View after a specified time (start exclusive).
    After(i64),
    /// Shrink a Window to a specified start and end time.
    ShrinkWindow(Window),
    /// Set the window start to a specified time.
    ShrinkStart(i64),
    /// Set the window end to a specified time.
    ShrinkEnd(i64),
}

#[derive(InputObject, Clone, Debug)]
pub struct Window {
    /// Window start time.
    pub start: i64,
    /// Window end time.
    pub end: i64,
}

#[derive(Enum, Copy, Clone, Debug)]
pub enum Operator {
    /// Equality operator.
    Equal,
    /// Inequality operator.
    NotEqual,
    /// Greater Than Or Equal operator.
    GreaterThanOrEqual,
    /// Less Than Or Equal operator.
    LessThanOrEqual,
    /// Greater Than operator.
    GreaterThan,
    /// Less Than operator.
    LessThan,
    /// Is None operator.
    IsNone,
    /// Is Some operator.
    IsSome,
    /// Is In operator.
    IsIn,
    /// Is Not In operator.
    IsNotIn,
    /// Contains operator.
    Contains,
    /// Not Contains operator.
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
    /// Node filter.
    Node(NodeFieldFilter),
    /// Property filter.
    Property(PropertyFilterExpr),
    /// Metadata filter.
    Metadata(MetadataFilterExpr),
    /// Temporal property filter.
    TemporalProperty(TemporalPropertyFilterExpr),
    /// AND operator.
    And(Vec<NodeFilter>),
    /// OR operator.
    Or(Vec<NodeFilter>),
    /// NOT operator.
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
    /// Node component to compare against.
    pub field: NodeField,
    /// Operator filter.
    pub operator: Operator,
    /// Value filter.
    pub value: Value,
}

impl NodeFieldFilter {
    pub fn validate(&self) -> Result<(), GraphError> {
        validate_operator_value_pair(self.operator, &Some(self.value.clone()))
    }
}

#[derive(Enum, Copy, Clone, Debug)]
pub enum NodeField {
    /// Node name.
    NodeName,
    /// Node type.
    NodeType,
}

#[derive(OneOfInput, Clone, Debug)]
pub enum EdgeFilter {
    /// Source node.
    Src(NodeFieldFilter),
    /// Destination node.
    Dst(NodeFieldFilter),
    /// Property.
    Property(PropertyFilterExpr),
    /// Metadata.
    Metadata(MetadataFilterExpr),
    /// Temporal property.
    TemporalProperty(TemporalPropertyFilterExpr),
    /// AND operator.
    And(Vec<EdgeFilter>),
    /// OR operator.
    Or(Vec<EdgeFilter>),
    /// NOT operator.
    Not(Wrapped<EdgeFilter>),
}

#[derive(InputObject, Clone, Debug)]
pub struct PropertyFilterExpr {
    /// Node property to compare against.
    pub name: String,
    /// Operator.
    pub operator: Operator,
    /// Value.
    pub value: Option<Value>,
}

impl PropertyFilterExpr {
    pub fn validate(&self) -> Result<(), GraphError> {
        validate_operator_value_pair(self.operator, &self.value)
    }
}

#[derive(InputObject, Clone, Debug)]
pub struct MetadataFilterExpr {
    /// Node metadata to compare against.
    pub name: String,
    /// Operator.
    pub operator: Operator,
    /// Value.
    pub value: Option<Value>,
}

impl MetadataFilterExpr {
    pub fn validate(&self) -> Result<(), GraphError> {
        validate_operator_value_pair(self.operator, &self.value)
    }
}

#[derive(InputObject, Clone, Debug)]
pub struct TemporalPropertyFilterExpr {
    /// Name.
    pub name: String,
    /// Type of temporal property. Choose from: any, latest.
    pub temporal: TemporalType,
    /// Operator.
    pub operator: Operator,
    /// Value.
    pub value: Option<Value>,
}

impl TemporalPropertyFilterExpr {
    pub fn validate(&self) -> Result<(), GraphError> {
        validate_operator_value_pair(self.operator, &self.value)
    }
}

#[derive(Enum, Copy, Clone, Debug)]
pub enum TemporalType {
    /// Any.
    Any,
    /// Latest.
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
        match filter {
            NodeFilter::Node(node) => {
                node.validate()?;
                Ok(CompositeNodeFilter::Node(Filter {
                    field_name: node.field.to_string(),
                    field_value: field_value(node.value, node.operator)?,
                    operator: node.operator.into(),
                }))
            }
            NodeFilter::Property(prop) => {
                prop.validate()?;
                Ok(CompositeNodeFilter::Property(prop.try_into()?))
            }
            NodeFilter::Metadata(prop) => {
                prop.validate()?;
                Ok(CompositeNodeFilter::Property(prop.try_into()?))
            }
            NodeFilter::TemporalProperty(prop) => {
                prop.validate()?;
                Ok(CompositeNodeFilter::Property(prop.try_into()?))
            }
            NodeFilter::And(and_filters) => {
                let mut iter = and_filters
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter();
                if let Some(first) = iter.next() {
                    let and_chain = iter.fold(first, |acc, next| {
                        CompositeNodeFilter::And(Box::new(acc), Box::new(next))
                    });
                    Ok(and_chain)
                } else {
                    Err(GraphError::InvalidGqlFilter(
                        "Filter 'and' requires non-empty list".to_string(),
                    ))
                }
            }
            NodeFilter::Or(or_filters) => {
                let mut iter = or_filters
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter();
                if let Some(first) = iter.next() {
                    let or_chain = iter.fold(first, |acc, next| {
                        CompositeNodeFilter::Or(Box::new(acc), Box::new(next))
                    });
                    Ok(or_chain)
                } else {
                    Err(GraphError::InvalidGqlFilter(
                        "Filter 'or' requires non-empty list".to_string(),
                    ))
                }
            }
            NodeFilter::Not(not_filters) => {
                let inner = CompositeNodeFilter::try_from(not_filters.deref().clone())?;
                Ok(CompositeNodeFilter::Not(Box::new(inner)))
            }
        }
    }
}

impl TryFrom<EdgeFilter> for CompositeEdgeFilter {
    type Error = GraphError;

    fn try_from(filter: EdgeFilter) -> Result<Self, Self::Error> {
        match filter {
            EdgeFilter::Src(src) => {
                src.validate()?;
                Ok(CompositeEdgeFilter::Edge(Filter {
                    field_name: "src".to_string(),
                    field_value: field_value(src.value, src.operator)?,
                    operator: src.operator.into(),
                }))
            }
            EdgeFilter::Dst(dst) => {
                dst.validate()?;
                Ok(CompositeEdgeFilter::Edge(Filter {
                    field_name: "dst".to_string(),
                    field_value: field_value(dst.value, dst.operator)?,
                    operator: dst.operator.into(),
                }))
            }
            EdgeFilter::Property(prop) => {
                prop.validate()?;
                Ok(CompositeEdgeFilter::Property(prop.try_into()?))
            }
            EdgeFilter::Metadata(prop) => {
                prop.validate()?;
                Ok(CompositeEdgeFilter::Property(prop.try_into()?))
            }
            EdgeFilter::TemporalProperty(prop) => {
                prop.validate()?;
                Ok(CompositeEdgeFilter::Property(prop.try_into()?))
            }
            EdgeFilter::And(and_filters) => {
                let mut iter = and_filters
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter();

                if let Some(first) = iter.next() {
                    let and_chain = iter.fold(first, |acc, next| {
                        CompositeEdgeFilter::And(Box::new(acc), Box::new(next))
                    });
                    Ok(and_chain)
                } else {
                    Err(GraphError::InvalidGqlFilter(
                        "Filter 'and' requires non-empty list".to_string(),
                    ))
                }
            }
            EdgeFilter::Or(or_filters) => {
                let mut iter = or_filters
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter();

                if let Some(first) = iter.next() {
                    let or_chain = iter.fold(first, |acc, next| {
                        CompositeEdgeFilter::Or(Box::new(acc), Box::new(next))
                    });
                    Ok(or_chain)
                } else {
                    Err(GraphError::InvalidGqlFilter(
                        "Filter 'or' requires non-empty list".to_string(),
                    ))
                }
            }
            EdgeFilter::Not(not_filters) => {
                let inner = CompositeEdgeFilter::try_from(not_filters.deref().clone())?;
                Ok(CompositeEdgeFilter::Not(Box::new(inner)))
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

impl TryFrom<MetadataFilterExpr> for PropertyFilter {
    type Error = GraphError;

    fn try_from(expr: MetadataFilterExpr) -> Result<Self, Self::Error> {
        build_property_filter(PropertyRef::Metadata(expr.name), expr.operator, expr.value)
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
                "Operator {operator} requires a list"
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
