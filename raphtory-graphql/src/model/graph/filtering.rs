use crate::model::graph::{
    filter_expr_input::GqlFilter, node_id::GqlNodeId, property::Value, timeindex::GqlTimeInput,
};
use async_graphql::dynamic::ValueAccessor;
use dynamic_graphql::{
    internal::{
        FromValue, GetInputTypeRef, InputTypeName, InputValueResult, Register, Registry, TypeName,
    },
    InputObject, OneOfInput,
};
use raphtory::{
    db::graph::views::filter::model::{
        filter_operator::FilterOperator,
        property_filter::{Op, PropertyFilter, PropertyFilterValue, PropertyRef},
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::Prop;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, ops::Deref, sync::Arc};

#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    /// Window start time.
    pub start: GqlTimeInput,
    /// Window end time.
    pub end: GqlTimeInput,
}

#[derive(OneOfInput, Clone, Debug)]
pub enum GraphViewCollection {
    /// Contains only the default layer.
    DefaultLayer(bool),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Subgraph nodes.
    Subgraph(Vec<GqlNodeId>),
    /// Subgraph node types.
    SubgraphNodeTypes(Vec<String>),
    /// List of excluded nodes.
    ExcludeNodes(Vec<GqlNodeId>),
    /// Valid state.
    Valid(bool),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// View at the latest time.
    Latest(bool),
    /// Snapshot at specified time.
    SnapshotAt(GqlTimeInput),
    /// Snapshot at latest time.
    SnapshotLatest(bool),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
    /// A filter tree; the entity it tests is written in the tree.
    Filter(GqlFilter),
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
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// Snapshot at specified time.
    SnapshotAt(GqlTimeInput),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
    /// A filter tree; the entity it tests is written in the tree.
    Filter(GqlFilter),
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
    SnapshotAt(GqlTimeInput),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
    /// A filter tree; the entity it tests is written in the tree.
    Filter(GqlFilter),
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
    SnapshotAt(GqlTimeInput),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
    /// A filter tree; the entity it tests is written in the tree.
    Filter(GqlFilter),
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
    SnapshotAt(GqlTimeInput),
    /// List of included layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single excluded layer.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
    /// A filter tree; the entity it tests is written in the tree.
    Filter(GqlFilter),
}

#[derive(OneOfInput, Clone, Debug)]
pub enum PathFromNodeViewCollection {
    /// Latest time.
    Latest(bool),
    /// Latest snapshot.
    SnapshotLatest(bool),
    /// Time.
    SnapshotAt(GqlTimeInput),
    /// List of layers.
    Layers(Vec<String>),
    /// List of excluded layers.
    ExcludeLayers(Vec<String>),
    /// Single layer to exclude.
    ExcludeLayer(String),
    /// Window between a start and end time.
    Window(Window),
    /// View at a specified time.
    At(GqlTimeInput),
    /// View before a specified time (end exclusive).
    Before(GqlTimeInput),
    /// View after a specified time (start exclusive).
    After(GqlTimeInput),
    /// Set the window start to a specified time.
    ShrinkStart(GqlTimeInput),
    /// Set the window end to a specified time.
    ShrinkEnd(GqlTimeInput),
}

/// Boolean expression over a property value.
///
/// `PropCondition` is the `where` of a namespace metagraph filter: how one
/// graph-level metadata value, or a graph field, should be matched.
///
/// It supports:
/// - comparisons (`Eq`, `Gt`, `Le`, …),
/// - string predicates (`Contains`, `StartsWith`, …),
/// - set membership (`IsIn`, `IsNotIn`),
/// - presence checks (`IsSome`, `IsNone`),
/// - boolean composition (`And`, `Or`, `Not`),
/// - and list/aggregate qualifiers (`First`, `Sum`, `Len`, …).
///
/// Notes:
/// - `Value` is interpreted according to the property’s type.
/// - Aggregators/qualifiers like `Sum` and `Len` apply when the underlying
///   property is list-like or aggregatable (depending on your engine rules).
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PropCondition {
    /// Equality: property value equals the given value.
    Eq(Value),
    /// Inequality: property value does not equal the given value.
    Ne(Value),
    /// Greater-than: property value is greater than the given value.
    Gt(Value),
    /// Greater-than-or-equal: property value is >= the given value.
    Ge(Value),
    /// Less-than: property value is less than the given value.
    Lt(Value),
    /// Less-than-or-equal: property value is <= the given value.
    Le(Value),

    /// String prefix match against the property's string representation.
    StartsWith(Value),
    /// String suffix match against the property's string representation.
    EndsWith(Value),
    /// Substring match against the property's string representation.
    Contains(Value),
    /// Negated substring match against the property's string representation.
    NotContains(Value),

    /// Fuzzy string match (Levenshtein distance, optional prefix matching).
    FuzzySearch(FuzzySearchExpr),

    /// Set membership: property value is contained in the given list of values.
    IsIn(Value),
    /// Negated set membership: property value is not contained in the given list of values.
    IsNotIn(Value),

    /// Presence check: property value is present (not null/missing).
    ///
    /// When set to `true`, requires the property to exist.
    IsSome(bool),
    /// Absence check: property value is missing / null.
    ///
    /// When set to `true`, requires the property to be missing.
    IsNone(bool),

    /// Logical AND over nested conditions.
    And(Vec<PropCondition>),
    /// Logical OR over nested conditions.
    Or(Vec<PropCondition>),
    /// Logical NOT over a nested condition.
    Not(Wrapped<PropCondition>),

    /// Applies the nested condition to the **first** element of a list-like property.
    First(Wrapped<PropCondition>),
    /// Applies the nested condition to the **last** element of a list-like property.
    Last(Wrapped<PropCondition>),
    /// Requires that **any** element of a list-like property matches the nested condition.
    Any(Wrapped<PropCondition>),
    /// Requires that **all** elements of a list-like property match the nested condition.
    All(Wrapped<PropCondition>),

    /// Applies the nested condition to the **sum** of a numeric list-like property.
    Sum(Wrapped<PropCondition>),
    /// Applies the nested condition to the **average** of a numeric list-like property.
    Avg(Wrapped<PropCondition>),
    /// Applies the nested condition to the **minimum** element of a list-like property.
    Min(Wrapped<PropCondition>),
    /// Applies the nested condition to the **maximum** element of a list-like property.
    Max(Wrapped<PropCondition>),
    /// Applies the nested condition to the **length** of a list-like property.
    Len(Wrapped<PropCondition>),
}

impl PropCondition {
    pub fn op_name(&self) -> &'static str {
        use PropCondition::*;
        match self {
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
            FuzzySearch(_) => "fuzzySearch",

            IsIn(_) => "isIn",
            IsNotIn(_) => "isNotIn",

            IsSome(_) => "isSome",
            IsNone(_) => "isNone",

            And(_) => "and",
            Or(_) => "or",
            Not(_) => "not",

            First(_) => "first",
            Last(_) => "last",
            Any(_) => "any",
            All(_) => "all",

            Sum(_) => "sum",
            Avg(_) => "avg",
            Min(_) => "min",
            Max(_) => "max",
            Len(_) => "len",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Wrapped<T>(Box<T>);

impl<T> From<T> for Wrapped<T> {
    fn from(inner: T) -> Self {
        Wrapped(Box::new(inner))
    }
}

impl<T> Deref for Wrapped<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

/// Fuzzy string match: passes when the candidate is within `levenshteinDistance`
/// edits of `value` (optionally also matching by prefix). Mirrors the local
/// `fuzzy_search(value, levenshtein_distance, prefix_match)` builder.
#[derive(InputObject, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FuzzySearchExpr {
    /// The string to match against.
    pub value: String,
    /// Maximum Levenshtein edit distance for a match.
    pub levenshtein_distance: usize,
    /// Whether a prefix match within the distance also passes.
    pub prefix_match: bool,
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

fn require_string_value(op: &str, v: &Value) -> Result<String, GraphError> {
    if let Value::Str(s) = v {
        Ok(s.clone())
    } else {
        Err(GraphError::InvalidGqlFilter(format!(
            "{op} requires a string value, got {v}"
        )))
    }
}

fn require_prop_list_value(op: &str, v: &Value) -> Result<PropertyFilterValue, GraphError> {
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
        Err(GraphError::InvalidGqlFilter(format!(
            "{op} requires a list value, got {v}"
        )))
    }
}

pub(crate) fn translate_prop_leaf_to_filter(
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
        // `isSome: false` is exactly `isNone: true` (and vice versa) — lower
        // to the dual operator instead of rejecting.
        IsSome(false) => (FO::IsNone, PropertyFilterValue::None),
        IsNone(false) => (FO::IsSome, PropertyFilterValue::None),

        FuzzySearch(f) => (
            FO::FuzzySearch {
                levenshtein_distance: f.levenshtein_distance,
                prefix_match: f.prefix_match,
            },
            PropertyFilterValue::Single(Prop::Str(f.value.clone().into())),
        ),

        And(_) | Or(_) | Not(_) | First(_) | Last(_) | Any(_) | All(_) | Sum(_) | Avg(_)
        | Min(_) | Max(_) | Len(_) => {
            let op = cmp.op_name();
            return Err(GraphError::InvalidGqlFilter(format!(
                "Expected comparison at leaf for {name_for_errors}; got '{op}'"
            )));
        }
    })
}

pub(crate) fn build_property_filter_from_condition_with_entity<M: Clone + Send + Sync + 'static>(
    prop_ref: PropertyRef,
    cond: &PropCondition,
    entity: M,
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
        entity,
    })
}

/// Property/metadata keys to hide per entity type.
#[derive(InputObject, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HiddenKeys {
    /// Keys to strip from node property/metadata responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<Vec<String>>,
    /// Keys to strip from edge property/metadata responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge: Option<Vec<String>>,
    /// Keys to strip from graph-own property/metadata responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<Vec<String>>,
}

/// Top-level access filter accepted by `grantGraphFilteredReadOnly`.
/// Separates row-level visibility (which entities are returned) from column-level
/// visibility (which property keys appear on returned entities).
#[derive(InputObject, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GraphAccessFilter {
    /// Row-level filter: which nodes/edges/graph-view are visible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<GqlFilter>,
    /// Temporal property keys to hide per entity type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hidden_properties: Option<HiddenKeys>,
    /// Metadata keys to hide per entity type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hidden_metadata: Option<HiddenKeys>,
}
