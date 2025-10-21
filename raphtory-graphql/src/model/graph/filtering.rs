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
        property_filter::{Op, PropertyFilter, PropertyFilterValue, PropertyRef},
        Filter, FilterValue,
    },
    errors::GraphError,
};
use raphtory_api::core::entities::{properties::prop::Prop, GID};
use std::{
    borrow::Cow,
    fmt,
    fmt::{Display, Formatter},
    marker::PhantomData,
    ops::Deref,
    sync::Arc,
};

#[derive(InputObject, Clone, Debug)]
pub struct Window {
    /// Window start time.
    pub start: i64,
    /// Window end time.
    pub end: i64,
}

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

#[derive(Enum, Copy, Clone, Debug)]
pub enum NodeField {
    /// Node id.
    NodeId,
    /// Node name.
    NodeName,
    /// Node type.
    NodeType,
}

impl Display for NodeField {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                NodeField::NodeId => "node_id",
                NodeField::NodeName => "node_name",
                NodeField::NodeType => "node_type",
            }
        )
    }
}

#[derive(InputObject, Clone, Debug)]
pub struct PropertyFilterNew {
    pub name: String,
    #[graphql(name = "where")]
    pub where_: PropCondition,
}

#[derive(OneOfInput, Clone, Debug)]
pub enum PropCondition {
    Eq(Value),
    Ne(Value),
    Gt(Value),
    Ge(Value),
    Lt(Value),
    Le(Value),

    StartsWith(Value),
    EndsWith(Value),
    Contains(Value),
    NotContains(Value),

    IsIn(Value),
    IsNotIn(Value),

    IsSome(bool),
    IsNone(bool),

    And(Vec<PropCondition>),
    Or(Vec<PropCondition>),
    Not(Wrapped<PropCondition>),

    First(Wrapped<PropCondition>),
    Last(Wrapped<PropCondition>),
    Any(Wrapped<PropCondition>),
    All(Wrapped<PropCondition>),
    Sum(Wrapped<PropCondition>),
    Avg(Wrapped<PropCondition>),
    Min(Wrapped<PropCondition>),
    Max(Wrapped<PropCondition>),
    Len(Wrapped<PropCondition>),
}

impl PropCondition {
    pub fn op_name(&self) -> Option<&'static str> {
        use PropCondition::*;
        Some(match self {
            Eq(_) => "eq",
            Ne(_) => "ne",
            Gt(_) => "gt",
            Ge(_) => "ge",
            Lt(_) => "lt",
            Le(_) => "le",

            StartsWith(_) => "startsWith",
            EndsWith(_) => "endsWith",
            Contains(_) => "contains",
            NotContains(_) => "notContains",

            IsIn(_) => "isIn",
            IsNotIn(_) => "isNotIn",

            IsSome(_) => "isSome",
            IsNone(_) => "isNone",

            And(_) | Or(_) | Not(_) | First(_) | Last(_) | Any(_) | All(_) | Sum(_) | Avg(_)
            | Min(_) | Max(_) | Len(_) => return None,
        })
    }
}

#[derive(OneOfInput, Clone, Debug)]
pub enum NodeFieldCondition {
    Eq(Value),
    Ne(Value),
    Gt(Value),
    Ge(Value),
    Lt(Value),
    Le(Value),

    StartsWith(Value),
    EndsWith(Value),
    Contains(Value),
    NotContains(Value),

    IsIn(Value),
    IsNotIn(Value),
}

impl NodeFieldCondition {
    pub fn op_name(&self) -> Option<&'static str> {
        use NodeFieldCondition::*;
        Some(match self {
            Eq(_) => "eq",
            Ne(_) => "ne",
            Gt(_) => "gt",
            Ge(_) => "ge",
            Lt(_) => "lt",
            Le(_) => "le",
            StartsWith(_) => "startsWith",
            EndsWith(_) => "endsWith",
            Contains(_) => "contains",
            NotContains(_) => "notContains",
            IsIn(_) => "isIn",
            IsNotIn(_) => "isNotIn",
        })
    }
}

#[derive(InputObject, Clone, Debug)]
pub struct NodeFieldFilterNew {
    pub field: NodeField,
    #[graphql(name = "where")]
    pub where_: NodeFieldCondition,
}

#[derive(OneOfInput, Clone, Debug)]
pub enum NodeFilter {
    /// Node filter.
    Node(NodeFieldFilterNew),
    /// Property filter.
    Property(PropertyFilterNew),
    /// Metadata filter.
    Metadata(PropertyFilterNew),
    /// Temporal property filter.
    TemporalProperty(PropertyFilterNew),
    /// AND operator.
    And(Vec<NodeFilter>),
    /// OR operator.
    Or(Vec<NodeFilter>),
    /// NOT operator.
    Not(Wrapped<NodeFilter>),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum EdgeFilter {
    /// Source node filter.
    Src(NodeFieldFilterNew),
    /// Destination node filter.
    Dst(NodeFieldFilterNew),
    /// Property filter.
    Property(PropertyFilterNew),
    /// Metadata filter.
    Metadata(PropertyFilterNew),
    /// Temporal property filter.
    TemporalProperty(PropertyFilterNew),
    /// AND operator.
    And(Vec<EdgeFilter>),
    /// OR operator.
    Or(Vec<EdgeFilter>),
    /// NOT operator.
    Not(Wrapped<EdgeFilter>),
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
        T::from_value(value)
            .map(|v| Wrapped(Box::new(v)))
            .map_err(|e| e.propagate())
    }
}

impl<T: TypeName + 'static> TypeName for Wrapped<T> {
    fn get_type_name() -> Cow<'static, str> {
        T::get_type_name()
    }
}
impl<T: InputTypeName + 'static> InputTypeName for Wrapped<T> {}

fn peel_prop_wrappers_and_collect_ops<'a>(
    cond: &'a PropCondition,
    ops: &mut Vec<Op>,
) -> Option<&'a PropCondition> {
    use PropCondition::*;

    match cond {
        First(inner) => {
            ops.push(Op::First);
            Some(inner.deref())
        }
        Last(inner) => {
            ops.push(Op::Last);
            Some(inner.deref())
        }
        Any(inner) => {
            ops.push(Op::Any);
            Some(inner.deref())
        }
        All(inner) => {
            ops.push(Op::All);
            Some(inner.deref())
        }
        Sum(inner) => {
            ops.push(Op::Sum);
            Some(inner.deref())
        }
        Avg(inner) => {
            ops.push(Op::Avg);
            Some(inner.deref())
        }
        Min(inner) => {
            ops.push(Op::Min);
            Some(inner.deref())
        }
        Max(inner) => {
            ops.push(Op::Max);
            Some(inner.deref())
        }
        Len(inner) => {
            ops.push(Op::Len);
            Some(inner.deref())
        }

        _ => None,
    }
}

fn require_string_value(op: Option<&str>, v: &Value) -> Result<String, GraphError> {
    if let Value::Str(s) = v {
        Ok(s.clone())
    } else {
        let name = op.unwrap_or("UNKNOWN");
        Err(GraphError::InvalidGqlFilter(format!(
            "{name} requires a string value, got {v}"
        )))
    }
}

fn require_prop_list_value(op: Option<&str>, v: &Value) -> Result<PropertyFilterValue, GraphError> {
    if let Value::List(vs) = v {
        let props = vs
            .iter()
            .cloned()
            .map(Prop::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PropertyFilterValue::Set(Arc::new(
            props.into_iter().collect(),
        )))
    } else {
        let name = op.unwrap_or("UNKNOWN");
        Err(GraphError::InvalidGqlFilter(format!(
            "{name} requires a list value, got {v}"
        )))
    }
}

fn require_u64_value(op: Option<&str>, v: &Value) -> Result<u64, GraphError> {
    if let Value::U64(i) = v {
        Ok(*i)
    } else {
        let name = op.unwrap_or("UNKNOWN");
        Err(GraphError::InvalidGqlFilter(format!(
            "{name} requires a u64 value, got {v}"
        )))
    }
}

fn parse_node_id_scalar(op: Option<&str>, v: &Value) -> Result<FilterValue, GraphError> {
    match v {
        Value::U64(i) => Ok(FilterValue::ID(GID::U64(*i))),
        Value::Str(s) => Ok(FilterValue::ID(GID::Str(s.clone()))),
        other => {
            let name = op.unwrap_or("UNKNOWN");
            Err(GraphError::InvalidGqlFilter(format!(
                "{name} requires int or str, got {other}"
            )))
        }
    }
}

fn parse_node_id_list(op: Option<&str>, v: &Value) -> Result<FilterValue, GraphError> {
    let name = op.unwrap_or("UNKNOWN");
    let Value::List(vs) = v else {
        return Err(GraphError::InvalidGqlFilter(format!(
            "{name} requires a list value, got {v}"
        )));
    };

    let all_u64 = vs.iter().all(|v| matches!(v, Value::U64(_)));
    let all_str = vs.iter().all(|v| matches!(v, Value::Str(_)));
    if !(all_u64 || all_str) {
        return Err(GraphError::InvalidGqlFilter(format!(
            "{name} requires a homogeneous list of ints or strings"
        )));
    }

    let mut set = std::collections::HashSet::with_capacity(vs.len());
    if all_u64 {
        for v in vs {
            if let Value::U64(i) = v {
                set.insert(GID::U64(*i));
            }
        }
    } else {
        for v in vs {
            if let Value::Str(s) = v {
                set.insert(GID::Str(s.clone()));
            }
        }
    }
    Ok(FilterValue::IDSet(Arc::new(set)))
}

fn parse_string_list(op: Option<&str>, v: &Value) -> Result<FilterValue, GraphError> {
    let name = op.unwrap_or("UNKNOWN");
    let Value::List(vs) = v else {
        return Err(GraphError::InvalidGqlFilter(format!(
            "{name} requires a list value, got {v}"
        )));
    };

    let strings = vs
        .iter()
        .map(|v| {
            if let Value::Str(s) = v {
                Ok(s.clone())
            } else {
                Err(GraphError::InvalidGqlFilter(format!(
                    "Expected list of strings for {name}, got {v}"
                )))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(FilterValue::Set(Arc::new(strings.into_iter().collect())))
}

fn translate_node_field_where(
    field: NodeField,
    cond: &NodeFieldCondition,
) -> Result<(String, FilterValue, FilterOperator), GraphError> {
    use FilterOperator as FO;
    use NodeField::*;
    use NodeFieldCondition::*;

    let field_name = field.to_string();
    let op = cond.op_name();

    Ok(match (field, cond) {
        (NodeId, Eq(v)) => (field_name, parse_node_id_scalar(op, v)?, FO::Eq),
        (NodeId, Ne(v)) => (field_name, parse_node_id_scalar(op, v)?, FO::Ne),
        (NodeId, Gt(v)) => (
            field_name,
            FilterValue::ID(GID::U64(require_u64_value(op, v)?)),
            FO::Gt,
        ),
        (NodeId, Ge(v)) => (
            field_name,
            FilterValue::ID(GID::U64(require_u64_value(op, v)?)),
            FO::Ge,
        ),
        (NodeId, Lt(v)) => (
            field_name,
            FilterValue::ID(GID::U64(require_u64_value(op, v)?)),
            FO::Lt,
        ),
        (NodeId, Le(v)) => (
            field_name,
            FilterValue::ID(GID::U64(require_u64_value(op, v)?)),
            FO::Le,
        ),

        (NodeId, StartsWith(v)) => (
            field_name,
            FilterValue::ID(GID::Str(require_string_value(op, v)?)),
            FO::StartsWith,
        ),
        (NodeId, EndsWith(v)) => (
            field_name,
            FilterValue::ID(GID::Str(require_string_value(op, v)?)),
            FO::EndsWith,
        ),
        (NodeId, Contains(v)) => (
            field_name,
            FilterValue::ID(GID::Str(require_string_value(op, v)?)),
            FO::Contains,
        ),
        (NodeId, NotContains(v)) => (
            field_name,
            FilterValue::ID(GID::Str(require_string_value(op, v)?)),
            FO::NotContains,
        ),

        (NodeId, IsIn(v)) => (field_name, parse_node_id_list(op, v)?, FO::IsIn),
        (NodeId, IsNotIn(v)) => (field_name, parse_node_id_list(op, v)?, FO::IsNotIn),

        (NodeName, Eq(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Eq,
        ),
        (NodeName, Ne(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Ne,
        ),
        (NodeName, Gt(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Gt,
        ),
        (NodeName, Ge(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Ge,
        ),
        (NodeName, Lt(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Lt,
        ),
        (NodeName, Le(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Le,
        ),

        (NodeName, StartsWith(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::StartsWith,
        ),
        (NodeName, EndsWith(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::EndsWith,
        ),
        (NodeName, Contains(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Contains,
        ),
        (NodeName, NotContains(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::NotContains,
        ),

        (NodeName, IsIn(v)) => (field_name, parse_string_list(op, v)?, FO::IsIn),
        (NodeName, IsNotIn(v)) => (field_name, parse_string_list(op, v)?, FO::IsNotIn),

        (NodeType, Eq(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Eq,
        ),
        (NodeType, Ne(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Ne,
        ),
        (NodeType, Gt(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Gt,
        ),
        (NodeType, Ge(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Ge,
        ),
        (NodeType, Lt(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Lt,
        ),
        (NodeType, Le(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Le,
        ),

        (NodeType, StartsWith(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::StartsWith,
        ),
        (NodeType, EndsWith(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::EndsWith,
        ),
        (NodeType, Contains(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::Contains,
        ),
        (NodeType, NotContains(v)) => (
            field_name,
            FilterValue::Single(require_string_value(op, v)?),
            FO::NotContains,
        ),

        (NodeType, IsIn(v)) => (field_name, parse_string_list(op, v)?, FO::IsIn),
        (NodeType, IsNotIn(v)) => (field_name, parse_string_list(op, v)?, FO::IsNotIn),
    })
}

fn translate_prop_leaf_to_filter(
    name_for_errors: &str,
    cmp: &PropCondition,
) -> Result<(FilterOperator, PropertyFilterValue), GraphError> {
    use FilterOperator as FO;
    use PropCondition::*;

    let single = |v: &Value| -> Result<PropertyFilterValue, GraphError> {
        Ok(PropertyFilterValue::Single(Prop::try_from(v.clone())?))
    };

    Ok(match cmp {
        Eq(v) => (FO::Eq, single(v)?),
        Ne(v) => (FO::Ne, single(v)?),
        Gt(v) => (FO::Gt, single(v)?),
        Ge(v) => (FO::Ge, single(v)?),
        Lt(v) => (FO::Lt, single(v)?),
        Le(v) => (FO::Le, single(v)?),

        StartsWith(v) => (
            FO::StartsWith,
            PropertyFilterValue::Single(Prop::Str(require_string_value(cmp.op_name(), v)?.into())),
        ),
        EndsWith(v) => (
            FO::EndsWith,
            PropertyFilterValue::Single(Prop::Str(require_string_value(cmp.op_name(), v)?.into())),
        ),

        Contains(v) => (FO::Contains, single(v)?),
        NotContains(v) => (FO::NotContains, single(v)?),

        IsIn(v) => (FO::IsIn, require_prop_list_value(cmp.op_name(), v)?),
        IsNotIn(v) => (FO::IsNotIn, require_prop_list_value(cmp.op_name(), v)?),

        IsSome(true) => (FO::IsSome, PropertyFilterValue::None),
        IsNone(true) => (FO::IsNone, PropertyFilterValue::None),

        And(_) | Or(_) | Not(_) | First(_) | Last(_) | Any(_) | All(_) | Sum(_) | Avg(_)
        | Min(_) | Max(_) | Len(_) | IsSome(false) | IsNone(false) => {
            return Err(GraphError::InvalidGqlFilter(format!(
                "Expected comparison at leaf for {}",
                name_for_errors
            )));
        }
    })
}

fn build_property_filter_from_condition<M: Clone + Send + Sync + 'static>(
    prop_ref: PropertyRef,
    cond: &PropCondition,
) -> Result<PropertyFilter<M>, GraphError> {
    let mut ops: Vec<Op> = Vec::new();
    let mut cursor = cond;
    while let Some(inner) = peel_prop_wrappers_and_collect_ops(cursor, &mut ops) {
        cursor = inner;
    }
    let (operator, prop_value) = translate_prop_leaf_to_filter(prop_ref.name(), cursor)?;
    Ok(PropertyFilter {
        prop_ref,
        prop_value,
        operator,
        ops,
        _phantom: PhantomData,
    })
}

fn build_node_filter_from_prop_condition(
    prop_ref: PropertyRef,
    cond: &PropCondition,
) -> Result<CompositeNodeFilter, GraphError> {
    use PropCondition::*;

    match cond {
        And(list) => {
            let mut it = list.iter();
            let first = it
                .next()
                .ok_or_else(|| GraphError::InvalidGqlFilter("and expects non-empty list".into()))?;
            let mut acc = build_node_filter_from_prop_condition(prop_ref.clone(), first)?;
            for c in it {
                let next = build_node_filter_from_prop_condition(prop_ref.clone(), c)?;
                acc = CompositeNodeFilter::And(Box::new(acc), Box::new(next));
            }
            Ok(acc)
        }
        Or(list) => {
            let mut it = list.iter();
            let first = it
                .next()
                .ok_or_else(|| GraphError::InvalidGqlFilter("or expects non-empty list".into()))?;
            let mut acc = build_node_filter_from_prop_condition(prop_ref.clone(), first)?;
            for c in it {
                let next = build_node_filter_from_prop_condition(prop_ref.clone(), c)?;
                acc = CompositeNodeFilter::Or(Box::new(acc), Box::new(next));
            }
            Ok(acc)
        }
        Not(inner) => {
            let nf = build_node_filter_from_prop_condition(prop_ref, inner)?;
            Ok(CompositeNodeFilter::Not(Box::new(nf)))
        }
        _ => {
            let pf = build_property_filter_from_condition::<
                raphtory::db::graph::views::filter::model::node_filter::NodeFilter,
            >(prop_ref, cond)?;
            Ok(CompositeNodeFilter::Property(pf))
        }
    }
}

impl TryFrom<NodeFilter> for CompositeNodeFilter {
    type Error = GraphError;
    fn try_from(filter: NodeFilter) -> Result<Self, Self::Error> {
        match filter {
            NodeFilter::Node(node) => {
                let (field_name, field_value, operator) =
                    translate_node_field_where(node.field, &node.where_)?;
                Ok(CompositeNodeFilter::Node(Filter {
                    field_name,
                    field_value,
                    operator,
                }))
            }
            NodeFilter::Property(prop) => {
                let prop_ref = PropertyRef::Property(prop.name);
                build_node_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            NodeFilter::Metadata(prop) => {
                let prop_ref = PropertyRef::Metadata(prop.name);
                build_node_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            NodeFilter::TemporalProperty(prop) => {
                let prop_ref = PropertyRef::TemporalProperty(prop.name);
                build_node_filter_from_prop_condition(prop_ref, &prop.where_)
            }
            NodeFilter::And(and_filters) => {
                let mut iter = and_filters.into_iter().map(TryInto::try_into);
                let first = iter.next().ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'and' requires non-empty list".into())
                })??;
                Ok(iter.try_fold(first, |acc, next| {
                    let n = next?;
                    Ok::<_, GraphError>(CompositeNodeFilter::And(Box::new(acc), Box::new(n)))
                })?)
            }
            NodeFilter::Or(or_filters) => {
                let mut iter = or_filters.into_iter().map(TryInto::try_into);
                let first = iter.next().ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'or' requires non-empty list".into())
                })??;
                Ok(iter.try_fold(first, |acc, next| {
                    let n = next?;
                    Ok::<_, GraphError>(CompositeNodeFilter::Or(Box::new(acc), Box::new(n)))
                })?)
            }
            NodeFilter::Not(not_filters) => {
                let inner = CompositeNodeFilter::try_from(not_filters.deref().clone())?;
                Ok(CompositeNodeFilter::Not(Box::new(inner)))
            }
        }
    }
}

fn build_edge_filter_from_prop_condition(
    prop_ref: PropertyRef,
    cond: &PropCondition,
) -> Result<CompositeEdgeFilter, GraphError> {
    use PropCondition::*;

    match cond {
        And(list) => {
            let mut it = list.iter();
            let first = it
                .next()
                .ok_or_else(|| GraphError::InvalidGqlFilter("and expects non-empty list".into()))?;
            let mut acc = build_edge_filter_from_prop_condition(prop_ref.clone(), first)?;
            for c in it {
                let next = build_edge_filter_from_prop_condition(prop_ref.clone(), c)?;
                acc = CompositeEdgeFilter::And(Box::new(acc), Box::new(next));
            }
            Ok(acc)
        }
        Or(list) => {
            let mut it = list.iter();
            let first = it
                .next()
                .ok_or_else(|| GraphError::InvalidGqlFilter("or expects non-empty list".into()))?;
            let mut acc = build_edge_filter_from_prop_condition(prop_ref.clone(), first)?;
            for c in it {
                let next = build_edge_filter_from_prop_condition(prop_ref.clone(), c)?;
                acc = CompositeEdgeFilter::Or(Box::new(acc), Box::new(next));
            }
            Ok(acc)
        }
        Not(inner) => {
            let ef = build_edge_filter_from_prop_condition(prop_ref, inner)?;
            Ok(CompositeEdgeFilter::Not(Box::new(ef)))
        }
        _ => {
            let pf = build_property_filter_from_condition::<
                raphtory::db::graph::views::filter::model::edge_filter::EdgeFilter,
            >(prop_ref, cond)?;
            Ok(CompositeEdgeFilter::Property(pf))
        }
    }
}

impl TryFrom<EdgeFilter> for CompositeEdgeFilter {
    type Error = GraphError;
    fn try_from(filter: EdgeFilter) -> Result<Self, Self::Error> {
        match filter {
            EdgeFilter::Src(src) => {
                if matches!(src.field, NodeField::NodeType) {
                    return Err(GraphError::InvalidGqlFilter(
                        "Src filter does not support NODE_TYPE".into(),
                    ));
                }
                let (_, field_value, operator) =
                    translate_node_field_where(src.field, &src.where_)?;
                Ok(CompositeEdgeFilter::Edge(Filter {
                    field_name: "src".to_string(),
                    field_value,
                    operator,
                }))
            }
            EdgeFilter::Dst(dst) => {
                if matches!(dst.field, NodeField::NodeType) {
                    return Err(GraphError::InvalidGqlFilter(
                        "Dst filter does not support NODE_TYPE".into(),
                    ));
                }
                let (_, field_value, operator) =
                    translate_node_field_where(dst.field, &dst.where_)?;
                Ok(CompositeEdgeFilter::Edge(Filter {
                    field_name: "dst".to_string(),
                    field_value,
                    operator,
                }))
            }
            EdgeFilter::Property(p) => {
                let prop_ref = PropertyRef::Property(p.name);
                build_edge_filter_from_prop_condition(prop_ref, &p.where_)
            }
            EdgeFilter::Metadata(p) => {
                let prop_ref = PropertyRef::Metadata(p.name);
                build_edge_filter_from_prop_condition(prop_ref, &p.where_)
            }
            EdgeFilter::TemporalProperty(p) => {
                let prop_ref = PropertyRef::TemporalProperty(p.name);
                build_edge_filter_from_prop_condition(prop_ref, &p.where_)
            }
            EdgeFilter::And(and_filters) => {
                let mut iter = and_filters.into_iter().map(TryInto::try_into);
                let first = iter.next().ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'and' requires non-empty list".into())
                })??;
                Ok(iter.try_fold(first, |acc, next| {
                    let n = next?;
                    Ok::<_, GraphError>(CompositeEdgeFilter::And(Box::new(acc), Box::new(n)))
                })?)
            }
            EdgeFilter::Or(or_filters) => {
                let mut iter = or_filters.into_iter().map(TryInto::try_into);
                let first = iter.next().ok_or_else(|| {
                    GraphError::InvalidGqlFilter("Filter 'or' requires non-empty list".into())
                })??;
                Ok(iter.try_fold(first, |acc, next| {
                    let n = next?;
                    Ok::<_, GraphError>(CompositeEdgeFilter::Or(Box::new(acc), Box::new(n)))
                })?)
            }
            EdgeFilter::Not(not_filters) => {
                let inner = CompositeEdgeFilter::try_from(not_filters.deref().clone())?;
                Ok(CompositeEdgeFilter::Not(Box::new(inner)))
            }
        }
    }
}
