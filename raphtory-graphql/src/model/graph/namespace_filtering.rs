//! Filtering and sorting for namespace listings.
//!
//! Namespace collections (`graphs`, `children`, `items`) are browsed a page at a
//! time, so any narrowing or ordering a client wants has to happen server-side —
//! filtering a single page client-side would filter the wrong set, and sorting it
//! would only sort within that page.
//!
//! Graphs are described entirely by their metadata, and which keys matter is up
//! to whoever wrote the graph. So rather than a fixed set of filterable and
//! sortable columns, both filter and sort address metadata by key: filters reuse
//! the same `PropCondition` grammar as graph property filters, and sorts either
//! name a built-in field or a metadata key, with an optional explicit value order
//! for keys holding a small vocabulary (`"critical"`, `"high"`, ...) whose
//! natural ordering is not alphabetical.

use crate::{
    data::Data,
    model::graph::{
        filtering::{
            build_property_filter_from_condition_with_entity, FuzzySearchExpr, PropCondition,
            Wrapped,
        },
        meta_graph::MetaGraph,
        namespace::Namespace,
        namespaced_item::NamespacedItem,
        property::{ObjectEntry, Value},
    },
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{Enum, InputObject, OneOfInput, Result};
use raphtory::{
    db::graph::views::filter::model::{node_filter::NodeFilter, property_filter::PropertyRef},
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::cmp::Ordering;
// ─── Addressable attributes ──────────────────────────────────────────────────

/// A graph's built-in attributes. Both filtering and sorting address them
/// through this enum, so a client that knows how a column maps onto a field can
/// drive either from one declaration.
#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq)]
pub enum MetaGraphField {
    /// The graph's name.
    Name,
    /// The graph's full path.
    Path,
    /// Creation timestamp.
    Created,
    /// Last-updated timestamp.
    LastUpdated,
    /// Number of nodes.
    NodeCount,
    /// Number of edges.
    EdgeCount,
}

// ─── Filter inputs ───────────────────────────────────────────────────────────

/// Narrows a namespace's graph listing.
///
/// Composes the same way as the graph/node/edge filters: leaves test one
/// attribute or metadata key, and `and` / `or` / `not` combine them.
#[derive(OneOfInput, Clone, Debug)]
pub enum MetaGraphFilter {
    /// Condition on a built-in attribute or a metadata key.
    Condition(MetaGraphCondition),
    /// Logical AND over nested filters.
    And(Vec<MetaGraphFilter>),
    /// Logical OR over nested filters.
    Or(Vec<MetaGraphFilter>),
    /// Logical NOT over a nested filter.
    Not(Wrapped<MetaGraphFilter>),
}

/// One condition on a graph, testing either a built-in attribute or a metadata
/// key. Set exactly one of `field` / `metadataKey`, as for `MetaGraphSort`.
#[derive(InputObject, Clone, Debug)]
pub struct MetaGraphCondition {
    /// Built-in attribute to test.
    pub field: Option<MetaGraphField>,
    /// Metadata key to test.
    pub metadata_key: Option<String>,
    /// Condition applied to the value, using the same grammar as property
    /// filters elsewhere in the schema. Names, paths and metadata strings are
    /// tested as strings; counts and timestamps as integers.
    pub where_: PropCondition,
    /// Overrides the result when the graph has no value for the target.
    ///
    /// Set this when absence should read as a default rather than a non-match —
    /// e.g. treating a graph with no `archived` key as not archived, so
    /// `archived == false` still selects it. Applies to `field` too, since a
    /// graph may have no name.
    ///
    /// Left unset, the condition itself decides, which is what you want for
    /// conditions already about absence (`isNone`, `ne`).
    pub matches_if_absent: Option<bool>,
    /// Compare strings case-sensitively (defaults to false, matching
    /// `NamespaceFilter`). Only affects string comparisons: numbers, booleans
    /// and the string-encoded temporal/decimal values are unaffected.
    pub case_sensitive: Option<bool>,
}

/// Narrows a namespace's sub-namespace listing. Sub-namespaces carry no metadata
/// of their own, so only their path can be matched.
#[derive(InputObject, Clone, Debug)]
pub struct NamespaceFilter {
    /// Substring match against the namespace's path.
    pub path_contains: Option<String>,
    /// Match `pathContains` case-sensitively (defaults to false).
    pub case_sensitive: Option<bool>,
}

/// Narrows a namespace's heterogeneous `items` listing. Each half applies to the
/// matching kind of item; an item whose kind has no filter is kept. To list only
/// one kind, query `graphs` or `children` instead.
#[derive(InputObject, Clone, Debug)]
pub struct NamespacedItemFilter {
    /// Applied to the graphs in the collection.
    pub graphs: Option<MetaGraphFilter>,
    /// Applied to the sub-namespaces in the collection.
    pub namespaces: Option<NamespaceFilter>,
}

// ─── Sort inputs ─────────────────────────────────────────────────────────────

/// One sort key for a graph listing. Set exactly one of `field` or
/// `metadataKey`. Keys are applied in order, each breaking ties left by the
/// previous one; graphs missing the sort value sort last.
#[derive(InputObject, Clone, Debug)]
pub struct MetaGraphSort {
    /// Sort on a built-in attribute.
    pub field: Option<MetaGraphField>,
    /// Sort on the value of this metadata key.
    pub metadata_key: Option<String>,
    /// Explicit ordering for the values of `metadataKey`, lowest first. Values
    /// not listed sort after every listed one, among themselves by their natural
    /// order. Use for keys holding a small vocabulary whose meaningful order
    /// isn't alphabetical.
    pub value_order: Option<Vec<Value>>,
    /// Reverse this key's direction (default ascending).
    pub reverse: Option<bool>,
}

/// One sort key for a sub-namespace listing.
#[derive(InputObject, Clone, Debug)]
pub struct NamespaceSort {
    /// Reverse the path ordering (default ascending).
    pub reverse: Option<bool>,
}

// ─── Filter evaluation ───────────────────────────────────────────────────────

fn contains_with_case(haystack: &str, needle: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        haystack.contains(needle)
    } else {
        haystack.to_lowercase().contains(&needle.to_lowercase())
    }
}

/// Lowercases free-text values so a comparison can ignore case.
///
/// Only `Str` is folded. The numeric and boolean variants have no case, and
/// `DTime` / `NDTime` / `Decimal` are string-encoded but not free text, so
/// folding them would be meaningless. `List` and `Object` recurse — `IsIn`
/// carries its candidates in a list, and object *keys* are identifiers so only
/// entry values are folded.
///
/// Both matches are exhaustive on purpose: a new variant upstream should fail to
/// compile here rather than silently pass through unfolded.
fn lower_case_value(value: &Value) -> Value {
    match value {
        Value::Str(text) => Value::Str(text.to_lowercase()),
        Value::List(values) => Value::List(values.iter().map(lower_case_value).collect()),
        Value::Object(entries) => Value::Object(
            entries
                .iter()
                .map(|entry| ObjectEntry {
                    key: entry.key.clone(),
                    value: lower_case_value(&entry.value),
                })
                .collect(),
        ),
        Value::U8(v) => Value::U8(*v),
        Value::U16(v) => Value::U16(*v),
        Value::U32(v) => Value::U32(*v),
        Value::U64(v) => Value::U64(*v),
        Value::I32(v) => Value::I32(*v),
        Value::I64(v) => Value::I64(*v),
        Value::F32(v) => Value::F32(*v),
        Value::F64(v) => Value::F64(*v),
        Value::F32Special(v) => Value::F32Special(*v),
        Value::F64Special(v) => Value::F64Special(*v),
        Value::Bool(v) => Value::Bool(*v),
        Value::DTime(v) => Value::DTime(v.clone()),
        Value::NDTime(v) => Value::NDTime(v.clone()),
        Value::Decimal(v) => Value::Decimal(v.clone()),
    }
}

fn lower_case_condition(cond: &PropCondition) -> PropCondition {
    use PropCondition::*;
    let fold = lower_case_value;
    let nested = |inner: &Wrapped<PropCondition>| lower_case_condition(inner).into();
    let list = |items: &Vec<PropCondition>| items.iter().map(lower_case_condition).collect();
    match cond {
        Eq(v) => Eq(fold(v)),
        Ne(v) => Ne(fold(v)),
        Gt(v) => Gt(fold(v)),
        Ge(v) => Ge(fold(v)),
        Lt(v) => Lt(fold(v)),
        Le(v) => Le(fold(v)),
        StartsWith(v) => StartsWith(fold(v)),
        EndsWith(v) => EndsWith(fold(v)),
        Contains(v) => Contains(fold(v)),
        NotContains(v) => NotContains(fold(v)),
        IsIn(v) => IsIn(fold(v)),
        IsNotIn(v) => IsNotIn(fold(v)),
        IsSome(v) => IsSome(*v),
        IsNone(v) => IsNone(*v),
        And(items) => And(list(items)),
        Or(items) => Or(list(items)),
        Not(inner) => Not(nested(inner)),
        First(inner) => First(nested(inner)),
        Last(inner) => Last(nested(inner)),
        Any(inner) => Any(nested(inner)),
        All(inner) => All(nested(inner)),
        Sum(inner) => Sum(nested(inner)),
        Avg(inner) => Avg(nested(inner)),
        Min(inner) => Min(nested(inner)),
        Max(inner) => Max(nested(inner)),
        Len(inner) => Len(nested(inner)),
        FuzzySearch(expr) => FuzzySearch(FuzzySearchExpr {
            value: expr.value.to_lowercase(),
            ..expr.clone()
        }),
    }
}

/// Lowercases the value side, mirroring `lower_case_value`.
///
/// Only scalar strings are folded. List-valued properties are left alone: they
/// can only be reached through the aggregate wrappers (`any` / `all` / `first`
/// / `last`), and every attribute this applies to is a scalar. `IsIn` is still
/// handled, because there the list is the *operand* — folded by
/// `fold_value_case` — and the value it tests is a scalar.
fn lower_case_prop(prop: &Prop) -> Prop {
    match prop {
        Prop::Str(text) => Prop::Str(text.to_lowercase().into()),
        other => other.clone(),
    }
}

/// As `condition_matches`, folding case on both sides when `case_sensitive` is
/// false. Folding the operand and the value together is what keeps the
/// comparison symmetric — folding only one side would silently never match.
fn condition_matches_with_case(
    key: &str,
    cond: &PropCondition,
    value: Option<&Prop>,
    case_sensitive: bool,
) -> Result<bool> {
    if !case_sensitive {
        let folded_value = value.map(lower_case_prop);
        return condition_matches(key, &lower_case_condition(cond), folded_value.as_ref());
    }
    condition_matches(key, cond, value)
}

/// Evaluates a `PropCondition` against a single metadata value.
///
/// `And`/`Or`/`Not` are handled here because they compose whole conditions,
/// while everything below them compiles to one `PropertyFilter` whose `matches`
/// already understands absent values and list-aggregating wrappers.
fn condition_matches(key: &str, cond: &PropCondition, value: Option<&Prop>) -> Result<bool> {
    match cond {
        PropCondition::And(list) => {
            for c in list {
                if !condition_matches(key, c, value)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        PropCondition::Or(list) => {
            for c in list {
                if condition_matches(key, c, value)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        PropCondition::Not(inner) => Ok(!condition_matches(key, inner, value)?),
        leaf => {
            let filter = build_property_filter_from_condition_with_entity::<NodeFilter>(
                PropertyRef::Metadata(key.to_string()),
                leaf,
                NodeFilter,
            )?;
            Ok(filter.matches(value))
        }
    }
}

/// A built-in attribute's value as a `Prop`, so field conditions run through the
/// same `PropCondition` evaluation as metadata ones. Names and paths become
/// strings; counts and timestamps become integers, which is what makes numeric
/// comparisons (`gt`, `between`, ...) behave numerically rather than
/// lexically.
async fn field_value(
    graph: &MetaGraph,
    field: MetaGraphField,
    ctx: &Context<'_>,
    data: &Data,
) -> Result<Option<Prop>> {
    Ok(match field {
        MetaGraphField::Name => graph.name_value().map(|name| Prop::Str(name.into())),
        MetaGraphField::Path => Some(Prop::Str(graph.local_path().into())),
        MetaGraphField::Created => Some(Prop::I64(graph.created_value().await?)),
        MetaGraphField::LastUpdated => Some(Prop::I64(graph.last_updated_value().await?)),
        // I64 for every count and timestamp, so one client-side number maps onto
        // one variant here regardless of which field it targets.
        MetaGraphField::NodeCount => graph
            .node_count_value(ctx, data)
            .await?
            .map(|count| Prop::I64(count as i64)),
        MetaGraphField::EdgeCount => graph
            .edge_count_value(ctx, data)
            .await?
            .map(|count| Prop::I64(count as i64)),
    })
}

fn field_name(field: MetaGraphField) -> &'static str {
    match field {
        MetaGraphField::Name => "name",
        MetaGraphField::Path => "path",
        MetaGraphField::Created => "created",
        MetaGraphField::LastUpdated => "lastUpdated",
        MetaGraphField::NodeCount => "nodeCount",
        MetaGraphField::EdgeCount => "edgeCount",
    }
}

impl MetaGraphCondition {
    /// Mirrors `MetaGraphSort::validate` — the cost of the `Option` pair is that
    /// "exactly one target" has to be checked rather than made unrepresentable.
    fn validate(&self) -> Result<()> {
        match (self.field.is_some(), self.metadata_key.is_some()) {
            (true, true) => Err(GraphError::InvalidGqlFilter(
                "a condition must set either `field` or `metadataKey`, not both".into(),
            )
            .into()),
            (false, false) => Err(GraphError::InvalidGqlFilter(
                "a condition must set one of `field` or `metadataKey`".into(),
            )
            .into()),
            _ => Ok(()),
        }
    }
}

impl MetaGraphFilter {
    /// Validates every condition in the tree.
    ///
    /// Done up front rather than during evaluation so a malformed condition is
    /// reported whether or not short-circuiting would have reached it.
    fn validate(&self) -> Result<()> {
        match self {
            MetaGraphFilter::Condition(condition) => condition.validate(),
            MetaGraphFilter::And(list) | MetaGraphFilter::Or(list) => {
                require_non_empty(
                    if matches!(self, MetaGraphFilter::And(_)) {
                        "and"
                    } else {
                        "or"
                    },
                    list,
                )?;
                for nested in list {
                    nested.validate()?;
                }
                Ok(())
            }
            MetaGraphFilter::Not(inner) => inner.validate(),
        }
    }

    /// Built-in attributes referenced anywhere in the tree.
    fn referenced_fields(&self, out: &mut Vec<MetaGraphField>) {
        match self {
            MetaGraphFilter::Condition(condition) => {
                if let Some(field) = condition.field {
                    if !out.contains(&field) {
                        out.push(field);
                    }
                }
            }
            MetaGraphFilter::And(list) | MetaGraphFilter::Or(list) => {
                for nested in list {
                    nested.referenced_fields(out);
                }
            }
            MetaGraphFilter::Not(inner) => inner.referenced_fields(out),
        }
    }

    fn uses_metadata(&self) -> bool {
        match self {
            MetaGraphFilter::Condition(condition) => condition.metadata_key.is_some(),
            MetaGraphFilter::And(list) | MetaGraphFilter::Or(list) => {
                list.iter().any(|nested| nested.uses_metadata())
            }
            MetaGraphFilter::Not(inner) => inner.uses_metadata(),
        }
    }

    /// Resolves everything the tree needs — each referenced attribute once, and
    /// the metadata pairs once — then evaluates the tree against those values.
    /// Reading up front keeps evaluation synchronous, so nesting costs nothing
    /// beyond the reads the leaves actually ask for.
    pub(crate) async fn matches(
        &self,
        graph: &MetaGraph,
        ctx: &Context<'_>,
        data: &Data,
    ) -> Result<bool> {
        self.validate()?;
        let mut fields = Vec::new();
        self.referenced_fields(&mut fields);
        let mut resolved = Vec::with_capacity(fields.len());
        for field in fields {
            resolved.push((field, field_value(graph, field, ctx, data).await?));
        }
        // `None` from `metadata_pairs` means the caller may not read this graph's
        // metadata, which reads here as "every key absent" — so a filter can no
        // more observe a value than the `metadata` field can return it.
        let pairs = if self.uses_metadata() {
            graph.metadata_pairs(ctx, data).await?.unwrap_or_default()
        } else {
            Vec::new()
        };
        self.evaluate(&resolved, &pairs)
    }

    fn evaluate(
        &self,
        fields: &[(MetaGraphField, Option<Prop>)],
        pairs: &[(String, Prop)],
    ) -> Result<bool> {
        match self {
            MetaGraphFilter::Condition(condition) => {
                let (label, value) = match (&condition.field, &condition.metadata_key) {
                    (Some(field), _) => (
                        field_name(*field),
                        fields
                            .iter()
                            .find_map(|(f, v)| (f == field).then_some(v))
                            .and_then(|v| v.as_ref()),
                    ),
                    (None, Some(key)) => (
                        key.as_str(),
                        pairs.iter().find_map(|(k, p)| (k == key).then_some(p)),
                    ),
                    // validate() has already rejected this.
                    (None, None) => return condition.validate().map(|()| false),
                };
                let case_sensitive = condition.case_sensitive.unwrap_or(false);
                match value {
                    Some(prop) => condition_matches_with_case(
                        label,
                        &condition.where_,
                        Some(prop),
                        case_sensitive,
                    ),
                    None => match condition.matches_if_absent {
                        Some(default) => Ok(default),
                        // Still ask the condition: `isNone` and `ne` are
                        // meaningful for a missing value.
                        None => condition_matches_with_case(
                            label,
                            &condition.where_,
                            None,
                            case_sensitive,
                        ),
                    },
                }
            }
            MetaGraphFilter::And(list) => {
                for nested in list {
                    if !nested.evaluate(fields, pairs)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            MetaGraphFilter::Or(list) => {
                for nested in list {
                    if nested.evaluate(fields, pairs)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            MetaGraphFilter::Not(inner) => Ok(!inner.evaluate(fields, pairs)?),
        }
    }
}

fn require_non_empty(op: &str, list: &[MetaGraphFilter]) -> Result<()> {
    if list.is_empty() {
        Err(GraphError::InvalidGqlFilter(format!("{op} expects a non-empty list")).into())
    } else {
        Ok(())
    }
}

impl NamespaceFilter {
    pub(crate) fn matches(&self, namespace: &Namespace) -> bool {
        match &self.path_contains {
            None => true,
            Some(needle) => contains_with_case(
                namespace.path_str(),
                needle,
                self.case_sensitive.unwrap_or(false),
            ),
        }
    }
}

impl NamespacedItemFilter {
    pub(crate) async fn matches(
        &self,
        item: &NamespacedItem,
        ctx: &Context<'_>,
        data: &Data,
    ) -> Result<bool> {
        match item {
            NamespacedItem::MetaGraph(g) => match &self.graphs {
                None => Ok(true),
                Some(filter) => filter.matches(g, ctx, data).await,
            },
            NamespacedItem::Namespace(n) => Ok(match &self.namespaces {
                None => true,
                Some(filter) => filter.matches(n),
            }),
        }
    }
}

// ─── Sort evaluation ─────────────────────────────────────────────────────────

/// The value one sort key extracts from one graph. Reduced to a comparable form
/// up front so the actual sort is a plain comparison and reads no more metadata.
#[derive(Clone, Debug, PartialEq)]
enum SortValue {
    Text(String),
    Number(f64),
    /// A position in an explicit `valueOrder`, with the value's natural ordering
    /// kept as a tiebreak. Every value the order didn't list shares one rank
    /// past the end of it, so the tiebreak is what separates them.
    Ranked(usize, Box<SortValue>),
    /// No value for this key on this graph.
    Absent,
}

impl SortValue {
    /// Ascending order. `Absent` always sorts last here; `reverse` on the sort
    /// key flips that along with everything else, which keeps a reversed sort
    /// the exact mirror of the forward one.
    fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (SortValue::Absent, SortValue::Absent) => Ordering::Equal,
            (SortValue::Absent, _) => Ordering::Greater,
            (_, SortValue::Absent) => Ordering::Less,
            (SortValue::Ranked(a, a_tie), SortValue::Ranked(b, b_tie)) => {
                a.cmp(b).then_with(|| a_tie.compare(b_tie))
            }
            (SortValue::Text(a), SortValue::Text(b)) => a.cmp(b),
            (SortValue::Number(a), SortValue::Number(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            // Mixed types only arise from inconsistently-typed metadata; fall
            // back to the string form so the order is at least deterministic.
            (a, b) => a.as_text().cmp(&b.as_text()),
        }
    }

    fn as_text(&self) -> String {
        match self {
            SortValue::Text(s) => s.clone(),
            SortValue::Number(n) => n.to_string(),
            SortValue::Ranked(rank, tie) => format!("{rank}:{}", tie.as_text()),
            SortValue::Absent => String::new(),
        }
    }
}

/// Places `prop` within an explicit `valueOrder`. Listed values take their
/// position; anything else lands in one bucket after them, ordered naturally.
fn rank_by_value_order(prop: &Prop, order: &[Value]) -> Result<SortValue> {
    for (rank, candidate) in order.iter().enumerate() {
        if Prop::try_from(candidate.clone())? == *prop {
            return Ok(SortValue::Ranked(rank, Box::new(SortValue::Absent)));
        }
    }
    Ok(SortValue::Ranked(
        order.len(),
        Box::new(prop_to_sort_value(prop)),
    ))
}

fn prop_to_sort_value(prop: &Prop) -> SortValue {
    match prop {
        Prop::U8(v) => SortValue::Number(*v as f64),
        Prop::U16(v) => SortValue::Number(*v as f64),
        Prop::U32(v) => SortValue::Number(*v as f64),
        Prop::U64(v) => SortValue::Number(*v as f64),
        Prop::I32(v) => SortValue::Number(*v as f64),
        Prop::I64(v) => SortValue::Number(*v as f64),
        Prop::F32(v) => SortValue::Number(*v as f64),
        Prop::F64(v) => SortValue::Number(*v),
        Prop::Bool(v) => SortValue::Number(if *v { 1.0 } else { 0.0 }),
        other => SortValue::Text(other.to_string()),
    }
}

impl MetaGraphSort {
    fn validate(&self) -> Result<()> {
        match (self.field.is_some(), self.metadata_key.is_some()) {
            (true, true) => Err(GraphError::InvalidGqlFilter(
                "a sort key sets either `field` or `metadataKey`, not both".into(),
            )
            .into()),
            (false, false) => Err(GraphError::InvalidGqlFilter(
                "a sort key must set one of `field` or `metadataKey`".into(),
            )
            .into()),
            _ => Ok(()),
        }
    }

    async fn value_for(
        &self,
        graph: &MetaGraph,
        ctx: &Context<'_>,
        data: &Data,
    ) -> Result<SortValue> {
        if let Some(field) = self.field {
            return Ok(match field_value(graph, field, ctx, data).await? {
                Some(prop) => prop_to_sort_value(&prop),
                None => SortValue::Absent,
            });
        }

        let key = self.metadata_key.as_deref().unwrap_or_default();
        let Some(prop) = graph.metadata_value(ctx, data, key).await? else {
            return Ok(SortValue::Absent);
        };

        match &self.value_order {
            None => Ok(prop_to_sort_value(&prop)),
            Some(order) => rank_by_value_order(&prop, order),
        }
    }
}

/// Orders `graphs` by `sort`, reading each sort value once per graph.
///
/// With no keys — `None` or empty — this is the default ordering: path, which is
/// unique per graph and therefore stable from page to page. Callers can always
/// call it unconditionally.
pub(crate) async fn sort_graphs(
    mut graphs: Vec<MetaGraph>,
    sort: Option<Vec<MetaGraphSort>>,
    ctx: &Context<'_>,
    data: &Data,
) -> Result<Vec<MetaGraph>> {
    let sort = sort.unwrap_or_default();
    for key in &sort {
        key.validate()?;
    }

    let mut keyed = Vec::with_capacity(graphs.len());
    for graph in graphs.drain(..) {
        let mut values = Vec::with_capacity(sort.len());
        for key in &sort {
            values.push(key.value_for(&graph, ctx, data).await?);
        }
        keyed.push((values, graph));
    }

    let graphs = blocking_compute(move || {
        keyed.sort_by(|(a, ga), (b, gb)| {
            for (index, key) in sort.iter().enumerate() {
                let ordering = match (a.get(index), b.get(index)) {
                    (Some(x), Some(y)) => x.compare(y),
                    _ => Ordering::Equal,
                };
                let ordering = if key.reverse == Some(true) {
                    ordering.reverse()
                } else {
                    ordering
                };
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            // Path is unique per graph, so equal sort keys still yield a stable,
            // page-to-page consistent order.
            ga.local_path().cmp(gb.local_path())
        });

        graphs.extend(keyed.into_iter().map(|(_, graph)| graph));
        graphs
    })
    .await;
    Ok(graphs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use raphtory_api::core::storage::arc_str::ArcStr;

    fn str_prop(s: &str) -> Prop {
        Prop::Str(ArcStr::from(s))
    }

    fn eq_bool(b: bool) -> PropCondition {
        PropCondition::Eq(Value::Bool(b))
    }

    // ─── condition evaluation ────────────────────────────────────────────────

    #[test]
    fn equality_matches_present_value() {
        assert!(condition_matches("archived", &eq_bool(true), Some(&Prop::Bool(true))).unwrap());
        assert!(!condition_matches("archived", &eq_bool(true), Some(&Prop::Bool(false))).unwrap());
    }

    #[test]
    fn equality_does_not_match_absent_value() {
        // The reason `matchesIfAbsent` exists: a graph with no `archived` key is
        // not selected by `archived == false` without it.
        assert!(!condition_matches("archived", &eq_bool(false), None).unwrap());
    }

    #[test]
    fn is_none_matches_absent_value() {
        let cond = PropCondition::IsNone(true);
        assert!(condition_matches("archived", &cond, None).unwrap());
        assert!(!condition_matches("archived", &cond, Some(&Prop::Bool(false))).unwrap());
    }

    #[test]
    fn and_requires_every_branch() {
        let cond = PropCondition::And(vec![
            PropCondition::Ge(Value::I64(10)),
            PropCondition::Le(Value::I64(20)),
        ]);
        assert!(condition_matches("score", &cond, Some(&Prop::I64(15))).unwrap());
        assert!(!condition_matches("score", &cond, Some(&Prop::I64(25))).unwrap());
    }

    #[test]
    fn or_requires_one_branch() {
        let cond = PropCondition::Or(vec![
            PropCondition::Eq(Value::Str("high".into())),
            PropCondition::Eq(Value::Str("critical".into())),
        ]);
        assert!(condition_matches("severity", &cond, Some(&str_prop("critical"))).unwrap());
        assert!(!condition_matches("severity", &cond, Some(&str_prop("low"))).unwrap());
    }

    #[test]
    fn not_inverts_its_branch() {
        let cond = PropCondition::Not(eq_bool(true).into());
        assert!(condition_matches("archived", &cond, Some(&Prop::Bool(false))).unwrap());
        assert!(!condition_matches("archived", &cond, Some(&Prop::Bool(true))).unwrap());
    }

    #[test]
    fn nested_composition_is_evaluated() {
        // (score >= 10 AND NOT severity == "low")
        let cond = PropCondition::And(vec![
            PropCondition::Ge(Value::I64(10)),
            PropCondition::Not(PropCondition::Eq(Value::I64(42)).into()),
        ]);
        assert!(condition_matches("score", &cond, Some(&Prop::I64(11))).unwrap());
        assert!(!condition_matches("score", &cond, Some(&Prop::I64(42))).unwrap());
        assert!(!condition_matches("score", &cond, Some(&Prop::I64(9))).unwrap());
    }

    #[test]
    fn contains_matches_substring() {
        let cond = PropCondition::Contains(Value::Str("risk".into()));
        assert!(condition_matches("kind", &cond, Some(&str_prop("credit risk"))).unwrap());
        assert!(!condition_matches("kind", &cond, Some(&str_prop("credit"))).unwrap());
    }

    // ─── name / path filtering ──────────────────────────────────────────────

    #[test]
    fn substring_match_honours_the_case_flag() {
        assert!(contains_with_case("Quarterly Report", "quarterly", false));
        assert!(contains_with_case("quarterly report", "REPORT", false));
        assert!(!contains_with_case("quarterly report", "annual", false));
        // Case-sensitive is opt-in.
        assert!(!contains_with_case("Quarterly Report", "quarterly", true));
        assert!(contains_with_case("Quarterly Report", "Quarterly", true));
    }

    // ─── case folding ───────────────────────────────────────────────────────

    fn ci(cond: PropCondition, value: &Prop) -> bool {
        condition_matches_with_case("name", &cond, Some(value), false).unwrap()
    }
    fn cs(cond: PropCondition, value: &Prop) -> bool {
        condition_matches_with_case("name", &cond, Some(value), true).unwrap()
    }

    #[test]
    fn case_insensitive_folds_both_sides() {
        // The bug this fixes: searching "Event" missed a graph named "event".
        let cond = || PropCondition::Contains(Value::Str("Event".into()));
        assert!(ci(cond(), &str_prop("my event log")));
        assert!(ci(cond(), &str_prop("MY EVENT LOG")));
        assert!(!cs(cond(), &str_prop("my event log")));
        assert!(cs(
            PropCondition::Contains(Value::Str("event".into())),
            &str_prop("my event log")
        ));
    }

    #[test]
    fn case_insensitive_applies_to_equality_too() {
        let cond = || PropCondition::Eq(Value::Str("Quarterly".into()));
        assert!(ci(cond(), &str_prop("quarterly")));
        assert!(!cs(cond(), &str_prop("quarterly")));
    }

    #[test]
    fn folding_leaves_non_text_values_alone() {
        // Numbers have no case; folding must not disturb them.
        assert!(ci(PropCondition::Gt(Value::I64(9)), &Prop::I64(10)));
        assert!(!ci(PropCondition::Gt(Value::I64(9)), &Prop::I64(2)));
        assert!(ci(PropCondition::Eq(Value::Bool(true)), &Prop::Bool(true)));
    }

    #[test]
    fn folding_recurses_into_is_in_candidates() {
        // `IsIn` carries its candidates in a list — folding must reach them.
        let cond = PropCondition::IsIn(Value::List(vec![
            Value::Str("Alpha".into()),
            Value::Str("Beta".into()),
        ]));
        assert!(ci(cond.clone(), &str_prop("beta")));
        assert!(!cs(cond, &str_prop("beta")));
    }

    #[test]
    fn folding_recurses_through_combinator_wrappers() {
        let cond = PropCondition::Or(vec![
            PropCondition::Eq(Value::Str("Alpha".into())),
            PropCondition::Not(PropCondition::Contains(Value::Str("ZZZ".into())).into()),
        ]);
        assert!(ci(cond.clone(), &str_prop("alpha")));
        // The Not branch also matches, so check a value only the folded Eq can hit.
        assert!(ci(
            PropCondition::Eq(Value::Str("Alpha".into())),
            &str_prop("ALPHA")
        ));
    }

    #[test]
    fn folding_leaves_temporal_and_decimal_encodings_alone() {
        // These are string-encoded but not free text; lowercasing them would be
        // meaningless, so they must pass through untouched.
        for value in [
            Value::DTime("2026-01-01T00:00:00Z".into()),
            Value::NDTime("2026-01-01T00:00:00".into()),
            Value::Decimal("1.5E3".into()),
        ] {
            let folded = lower_case_value(&value);
            assert_eq!(format!("{value:?}"), format!("{folded:?}"));
        }
    }

    #[test]
    fn folding_lowercases_object_entry_values_but_not_keys() {
        let value = Value::Object(vec![ObjectEntry {
            key: "Owner".to_string(),
            value: Value::Str("Alice".into()),
        }]);
        match lower_case_value(&value) {
            Value::Object(entries) => {
                assert_eq!(entries[0].key, "Owner");
                assert!(matches!(&entries[0].value, Value::Str(s) if s == "alice"));
            }
            other => panic!("expected an object, got {other:?}"),
        }
    }

    // ─── filter combinators ─────────────────────────────────────────────────

    fn metadata_leaf(key: &str, cond: PropCondition) -> MetaGraphFilter {
        metadata_leaf_with(key, cond, None)
    }

    fn metadata_leaf_cased(key: &str, cond: PropCondition, cased: bool) -> MetaGraphFilter {
        metadata_leaf_with(key, cond, Some(cased))
    }

    fn metadata_leaf_with(
        key: &str,
        cond: PropCondition,
        case_sensitive: Option<bool>,
    ) -> MetaGraphFilter {
        MetaGraphFilter::Condition(MetaGraphCondition {
            field: None,
            metadata_key: Some(key.to_string()),
            where_: cond,
            matches_if_absent: None,
            case_sensitive,
        })
    }

    fn field_leaf(field: MetaGraphField, cond: PropCondition) -> MetaGraphFilter {
        MetaGraphFilter::Condition(MetaGraphCondition {
            field: Some(field),
            metadata_key: None,
            where_: cond,
            matches_if_absent: None,
            case_sensitive: None,
        })
    }

    #[test]
    fn metadata_leaves_fold_case_by_default() {
        // Column filters on metadata (owner, severity, riskType) are user text,
        // so "High" must match "high" the way the name search does.
        let pairs = [("severity".to_string(), str_prop("high"))];
        let cond = || PropCondition::Contains(Value::Str("HIGH".into()));
        assert!(eval(&metadata_leaf("severity", cond()), &pairs));
        assert!(!eval(
            &metadata_leaf_cased("severity", cond(), true),
            &pairs
        ));
        assert!(eval(
            &metadata_leaf_cased("severity", cond(), false),
            &pairs
        ));
    }

    fn eval(filter: &MetaGraphFilter, pairs: &[(String, Prop)]) -> bool {
        filter.evaluate(&[], pairs).unwrap()
    }

    #[test]
    fn or_matches_when_either_branch_does() {
        let filter = MetaGraphFilter::Or(vec![
            metadata_leaf("severity", PropCondition::Eq(Value::Str("high".into()))),
            metadata_leaf("severity", PropCondition::Eq(Value::Str("critical".into()))),
        ]);
        assert!(eval(&filter, &[("severity".into(), str_prop("critical"))]));
        assert!(!eval(&filter, &[("severity".into(), str_prop("low"))]));
    }

    #[test]
    fn and_requires_both_branches() {
        let filter = MetaGraphFilter::And(vec![
            metadata_leaf("archived", eq_bool(false)),
            metadata_leaf("score", PropCondition::Gt(Value::I64(5))),
        ]);
        let archived_low = [
            ("archived".into(), Prop::Bool(false)),
            ("score".into(), Prop::I64(1)),
        ];
        let archived_high = [
            ("archived".into(), Prop::Bool(false)),
            ("score".into(), Prop::I64(9)),
        ];
        assert!(!eval(&filter, &archived_low));
        assert!(eval(&filter, &archived_high));
    }

    #[test]
    fn not_inverts_a_nested_filter() {
        let filter = MetaGraphFilter::Not(metadata_leaf("archived", eq_bool(true)).into());
        assert!(eval(&filter, &[("archived".into(), Prop::Bool(false))]));
        assert!(!eval(&filter, &[("archived".into(), Prop::Bool(true))]));
    }

    #[test]
    fn combinators_nest() {
        // archived == false AND (severity == high OR severity == critical)
        let filter = MetaGraphFilter::And(vec![
            metadata_leaf("archived", eq_bool(false)),
            MetaGraphFilter::Or(vec![
                metadata_leaf("severity", PropCondition::Eq(Value::Str("high".into()))),
                metadata_leaf("severity", PropCondition::Eq(Value::Str("critical".into()))),
            ]),
        ]);
        assert!(eval(
            &filter,
            &[
                ("archived".into(), Prop::Bool(false)),
                ("severity".into(), str_prop("high"))
            ]
        ));
        assert!(!eval(
            &filter,
            &[
                ("archived".into(), Prop::Bool(true)),
                ("severity".into(), str_prop("high"))
            ]
        ));
        assert!(!eval(
            &filter,
            &[
                ("archived".into(), Prop::Bool(false)),
                ("severity".into(), str_prop("low"))
            ]
        ));
    }

    #[test]
    fn a_condition_must_name_exactly_one_target() {
        // The cost of the Option pair: this is a runtime check rather than an
        // unrepresentable state, so it needs a test.
        let neither = MetaGraphFilter::Condition(MetaGraphCondition {
            field: None,
            metadata_key: None,
            where_: eq_bool(true),
            matches_if_absent: None,
            case_sensitive: None,
        });
        let both = MetaGraphFilter::Condition(MetaGraphCondition {
            field: Some(MetaGraphField::Name),
            metadata_key: Some("severity".to_string()),
            where_: eq_bool(true),
            matches_if_absent: None,
            case_sensitive: None,
        });
        assert!(neither.validate().is_err());
        assert!(both.validate().is_err());
        assert!(field_leaf(MetaGraphField::Name, eq_bool(true))
            .validate()
            .is_ok());
        assert!(metadata_leaf("severity", eq_bool(true)).validate().is_ok());
    }

    #[test]
    fn validation_reaches_conditions_short_circuiting_would_skip() {
        // `or` stops at the first match, so a malformed condition after one
        // would never be evaluated — validation must still catch it.
        let filter = MetaGraphFilter::Or(vec![
            metadata_leaf("archived", eq_bool(false)),
            MetaGraphFilter::Condition(MetaGraphCondition {
                field: None,
                metadata_key: None,
                where_: eq_bool(true),
                matches_if_absent: None,
                case_sensitive: None,
            }),
        ]);
        assert!(filter.validate().is_err());
    }

    #[test]
    fn empty_combinator_lists_are_rejected() {
        // An empty `and` would vacuously match everything and an empty `or`
        // nothing; both are almost certainly a client bug, so say so.
        assert!(MetaGraphFilter::And(vec![]).validate().is_err());
        assert!(MetaGraphFilter::Or(vec![]).validate().is_err());
    }

    #[test]
    fn only_referenced_fields_are_read() {
        // The point of collecting fields up front: a field mentioned twice is
        // resolved once, and unmentioned fields are never touched.
        let filter = MetaGraphFilter::And(vec![
            field_leaf(MetaGraphField::NodeCount, PropCondition::Gt(Value::I64(1))),
            field_leaf(MetaGraphField::NodeCount, PropCondition::Lt(Value::I64(9))),
        ]);
        let mut fields = Vec::new();
        filter.referenced_fields(&mut fields);
        assert_eq!(fields, vec![MetaGraphField::NodeCount]);
        assert!(!filter.uses_metadata());
    }

    // ─── sort values ────────────────────────────────────────────────────────

    #[test]
    fn absent_sorts_after_everything() {
        assert_eq!(
            SortValue::Absent.compare(&SortValue::Number(0.0)),
            Ordering::Greater
        );
        assert_eq!(
            SortValue::Text(String::new()).compare(&SortValue::Absent),
            Ordering::Less
        );
        assert_eq!(
            SortValue::Absent.compare(&SortValue::Absent),
            Ordering::Equal
        );
    }

    #[test]
    fn numbers_compare_numerically_not_lexically() {
        assert_eq!(
            SortValue::Number(9.0).compare(&SortValue::Number(10.0)),
            Ordering::Less
        );
    }

    #[test]
    fn value_order_ranks_listed_values() {
        let order = vec![
            Value::Str("critical".into()),
            Value::Str("high".into()),
            Value::Str("medium".into()),
            Value::Str("low".into()),
        ];
        let critical = rank_by_value_order(&str_prop("critical"), &order).unwrap();
        let low = rank_by_value_order(&str_prop("low"), &order).unwrap();
        // Alphabetically "critical" < "low" too, so also check a pair where the
        // explicit order disagrees with the alphabet.
        let high = rank_by_value_order(&str_prop("high"), &order).unwrap();
        let medium = rank_by_value_order(&str_prop("medium"), &order).unwrap();

        assert_eq!(critical.compare(&low), Ordering::Less);
        assert_eq!(high.compare(&medium), Ordering::Less);
        assert_eq!(medium.compare(&high), Ordering::Greater);
    }

    #[test]
    fn unlisted_values_sort_after_every_listed_one() {
        // A double-digit order is the case that a naive string-prefixed rank
        // gets wrong: rank 5 must still sort before the unlisted bucket at 12.
        let order: Vec<Value> = (0..12)
            .map(|i| Value::Str(format!("v{i:02}").into()))
            .collect();
        let listed = rank_by_value_order(&str_prop("v05"), &order).unwrap();
        let unlisted = rank_by_value_order(&str_prop("zzz"), &order).unwrap();

        assert_eq!(listed.compare(&unlisted), Ordering::Less);
        assert_eq!(unlisted.compare(&listed), Ordering::Greater);
    }

    #[test]
    fn unlisted_values_keep_their_natural_order_among_themselves() {
        let order = vec![Value::Str("critical".into())];
        let a = rank_by_value_order(&str_prop("aaa"), &order).unwrap();
        let b = rank_by_value_order(&str_prop("bbb"), &order).unwrap();
        assert_eq!(a.compare(&b), Ordering::Less);
    }

    #[test]
    fn numeric_metadata_ranks_by_value_order() {
        let order = vec![Value::I64(3), Value::I64(1), Value::I64(2)];
        let three = rank_by_value_order(&Prop::I64(3), &order).unwrap();
        let one = rank_by_value_order(&Prop::I64(1), &order).unwrap();
        assert_eq!(three.compare(&one), Ordering::Less);
    }

    #[test]
    fn bool_metadata_sorts_false_before_true() {
        assert_eq!(
            prop_to_sort_value(&Prop::Bool(false)).compare(&prop_to_sort_value(&Prop::Bool(true))),
            Ordering::Less
        );
    }

    // ─── sort key validation ────────────────────────────────────────────────

    #[test]
    fn numeric_field_conditions_compare_numerically() {
        // The regression this guards: node/edge counts arrive as integers, so
        // `gt 9` must not read "10" as less than "9" the way a string would.
        let cond = PropCondition::Gt(Value::U64(9));
        assert!(condition_matches("nodeCount", &cond, Some(&Prop::U64(10))).unwrap());
        assert!(!condition_matches("nodeCount", &cond, Some(&Prop::U64(2))).unwrap());
    }

    #[test]
    fn integer_comparisons_span_prop_variants() {
        // Numeric fields and metadata rarely agree on which integer variant they
        // store, and a client has no way to know: it sends one number. So a
        // filter written as `i64` must compare numerically against every integer
        // variant rather than silently failing to match.
        let eq = PropCondition::Eq(Value::I64(6));
        assert!(condition_matches("edgeCount", &eq, Some(&Prop::I64(6))).unwrap());
        assert!(condition_matches("edgeCount", &eq, Some(&Prop::U64(6))).unwrap());
        assert!(!condition_matches("edgeCount", &eq, Some(&Prop::U64(7))).unwrap());

        let gt = PropCondition::Gt(Value::I64(9));
        assert!(condition_matches("nodeCount", &gt, Some(&Prop::I64(10))).unwrap());
        assert!(condition_matches("nodeCount", &gt, Some(&Prop::U64(10))).unwrap());
        assert!(!condition_matches("nodeCount", &gt, Some(&Prop::U64(9))).unwrap());
    }

    #[test]
    fn between_maps_onto_an_and_of_bounds() {
        let cond = PropCondition::And(vec![
            PropCondition::Ge(Value::U64(10)),
            PropCondition::Le(Value::U64(20)),
        ]);
        assert!(condition_matches("edgeCount", &cond, Some(&Prop::U64(10))).unwrap());
        assert!(condition_matches("edgeCount", &cond, Some(&Prop::U64(20))).unwrap());
        assert!(!condition_matches("edgeCount", &cond, Some(&Prop::U64(21))).unwrap());
    }

    #[test]
    fn field_names_are_distinct() {
        // The name only labels the synthetic property a condition compiles
        // against, but a collision would make two fields indistinguishable.
        let fields = [
            MetaGraphField::Name,
            MetaGraphField::Path,
            MetaGraphField::Created,
            MetaGraphField::LastUpdated,
            MetaGraphField::NodeCount,
            MetaGraphField::EdgeCount,
        ];
        let names: Vec<_> = fields.iter().map(|f| field_name(*f)).collect();
        let mut unique = names.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(names.len(), unique.len());
    }

    fn sort_key(field: Option<MetaGraphField>, metadata_key: Option<&str>) -> MetaGraphSort {
        MetaGraphSort {
            field,
            metadata_key: metadata_key.map(str::to_string),
            value_order: None,
            reverse: None,
        }
    }

    #[test]
    fn sort_key_requires_exactly_one_target() {
        assert!(sort_key(Some(MetaGraphField::Name), None)
            .validate()
            .is_ok());
        assert!(sort_key(None, Some("score")).validate().is_ok());
        assert!(sort_key(None, None).validate().is_err());
        assert!(sort_key(Some(MetaGraphField::Name), Some("score"))
            .validate()
            .is_err());
    }
}
