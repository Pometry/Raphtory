//! Lowers the GraphQL filter wire types onto expression filters.
//!
//! The wire schema (`GqlNodeFilter` and friends) is unchanged; only the
//! target changes: instead of the composite filter enums, each condition
//! builds the corresponding typed expression and is erased to a [`DynFilter`].

use crate::model::graph::filtering::{
    translate_node_field_where, translate_prop_leaf_to_filter, GqlEdgeFilter,
    GqlExplodedEdgeFilter, GqlNodeFilter, NodeField, NodeFieldCondition, PropCondition,
};
use raphtory::{
    db::graph::views::filter::model::{
        edge_filter::{EdgeEndpointNodeFilter, Endpoint},
        exploded_edge_filter::ExplodedEdgeFilter,
        filter::FilterValue,
        latest_filter::Latest as LatestWrap,
        layered_filter::Layered,
        node_expr::{DynCreateOp, EntityAggOps},
        property_filter::PropertyFilterValue,
        snapshot_filter::{SnapshotAt as SnapshotAtWrap, SnapshotLatest as SnapshotLatestWrap},
        windowed_filter::Windowed,
        CombinedFilter, ComposableFilter, DynFilter, EdgeViewFilterOps, FilterOperator,
        NodeViewFilterOps, PropertyExprFactory,
    },
    errors::GraphError,
    prelude::{EdgeFilter, EntityExprFilterOps, Layer, NodeFilter, NodeFilterFactory},
};
use raphtory_api::core::{
    entities::properties::prop::{IntoProp, Prop},
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{ops::Deref, sync::Arc};

fn erased<F: CombinedFilter>(f: F) -> DynFilter {
    Arc::new(f) as DynFilter
}

fn combine_all(
    filters: impl IntoIterator<Item = Result<DynFilter, GraphError>>,
    or: bool,
    what: &str,
) -> Result<DynFilter, GraphError> {
    let mut it = filters.into_iter();
    let first = it.next().transpose()?.ok_or_else(|| {
        GraphError::InvalidGqlFilter(format!("Filter '{what}' requires non-empty list"))
    })?;
    it.try_fold(first, |acc, next| {
        Ok::<_, GraphError>(if or {
            Arc::new(acc.or(next?)) as DynFilter
        } else {
            Arc::new(acc.and(next?)) as DynFilter
        })
    })
}

/// Applies one translated leaf predicate to a value expression.
fn apply_leaf(
    lhs: Arc<dyn DynCreateOp>,
    op: FilterOperator,
    value: PropertyFilterValue,
) -> Result<DynFilter, GraphError> {
    use FilterOperator as FO;
    Ok(match (op, value) {
        (FO::Eq, PropertyFilterValue::Single(v)) => erased(lhs.eq(v)),
        (FO::Ne, PropertyFilterValue::Single(v)) => erased(lhs.ne(v)),
        (FO::Gt, PropertyFilterValue::Single(v)) => erased(lhs.gt(v)),
        (FO::Ge, PropertyFilterValue::Single(v)) => erased(lhs.ge(v)),
        (FO::Lt, PropertyFilterValue::Single(v)) => erased(lhs.lt(v)),
        (FO::Le, PropertyFilterValue::Single(v)) => erased(lhs.le(v)),
        (FO::StartsWith, PropertyFilterValue::Single(v)) => erased(lhs.starts_with(v)),
        (FO::EndsWith, PropertyFilterValue::Single(v)) => erased(lhs.ends_with(v)),
        (FO::Contains, PropertyFilterValue::Single(v)) => erased(lhs.contains(v)),
        (FO::NotContains, PropertyFilterValue::Single(v)) => erased(lhs.not_contains(v)),
        (
            FO::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            PropertyFilterValue::Single(v),
        ) => erased(lhs.fuzzy_search(v, levenshtein_distance, prefix_match)),
        (FO::IsIn, PropertyFilterValue::Set(values)) => {
            erased(lhs.is_in(values.deref().iter().cloned()))
        }
        (FO::IsNotIn, PropertyFilterValue::Set(values)) => {
            erased(lhs.is_not_in(values.deref().iter().cloned()))
        }
        (FO::IsSome, PropertyFilterValue::None) => erased(lhs.is_some()),
        (FO::IsNone, PropertyFilterValue::None) => erased(lhs.is_none()),
        (op, _) => {
            return Err(GraphError::InvalidGqlFilter(format!(
                "operator {op:?} received an incompatible value shape"
            )))
        }
    })
}

/// Walks a property condition tree over a value expression: wrapper conditions
/// extend the expression (leading form, outermost applied first), boolean
/// combinators branch, leaves become predicates.
fn lower_prop_condition(
    lhs: Arc<dyn DynCreateOp>,
    name_for_errors: &str,
    cond: &PropCondition,
) -> Result<DynFilter, GraphError> {
    use PropCondition::*;
    match cond {
        And(list) => combine_all(
            list.iter()
                .map(|c| lower_prop_condition(lhs.clone(), name_for_errors, c)),
            false,
            "and",
        ),
        Or(list) => combine_all(
            list.iter()
                .map(|c| lower_prop_condition(lhs.clone(), name_for_errors, c)),
            true,
            "or",
        ),
        Not(inner) => Ok(
            Arc::new(lower_prop_condition(lhs, name_for_errors, inner.deref())?.not()) as DynFilter,
        ),
        First(inner) => lower_prop_condition(
            Arc::new(EntityAggOps::first(lhs)),
            name_for_errors,
            inner.deref(),
        ),
        Last(inner) => lower_prop_condition(
            Arc::new(EntityAggOps::last(lhs)),
            name_for_errors,
            inner.deref(),
        ),
        Any(inner) => lower_prop_condition(
            Arc::new(EntityExprFilterOps::any(lhs)),
            name_for_errors,
            inner.deref(),
        ),
        All(inner) => lower_prop_condition(
            Arc::new(EntityExprFilterOps::all(lhs)),
            name_for_errors,
            inner.deref(),
        ),
        Sum(inner) => lower_prop_condition(
            Arc::new(EntityAggOps::sum(lhs)),
            name_for_errors,
            inner.deref(),
        ),
        Avg(inner) => lower_prop_condition(
            Arc::new(EntityAggOps::avg(lhs)),
            name_for_errors,
            inner.deref(),
        ),
        Min(inner) => lower_prop_condition(
            Arc::new(EntityAggOps::min(lhs)),
            name_for_errors,
            inner.deref(),
        ),
        Max(inner) => lower_prop_condition(
            Arc::new(EntityAggOps::max(lhs)),
            name_for_errors,
            inner.deref(),
        ),
        Len(inner) => lower_prop_condition(
            Arc::new(EntityAggOps::len(lhs)),
            name_for_errors,
            inner.deref(),
        ),
        leaf => {
            let (op, value) = translate_prop_leaf_to_filter(name_for_errors, leaf)?;
            apply_leaf(lhs, op, value)
        }
    }
}

/// Applies one translated built-in-field predicate to a field expression.
fn apply_field_leaf(
    lhs: Arc<dyn DynCreateOp>,
    op: FilterOperator,
    value: FilterValue,
) -> Result<DynFilter, GraphError> {
    use FilterOperator as FO;
    let single = |v: FilterValue| -> Result<Prop, GraphError> {
        Ok(match v {
            FilterValue::ID(gid) => gid.into_prop(),
            FilterValue::Single(s) => Prop::str(s),
            other => {
                return Err(GraphError::InvalidGqlFilter(format!(
                    "expected a single value, got {other:?}"
                )))
            }
        })
    };
    let set = |v: FilterValue| -> Result<Vec<Prop>, GraphError> {
        Ok(match v {
            FilterValue::IDSet(gids) => gids.iter().map(|g| g.clone().into_prop()).collect(),
            FilterValue::Set(strings) => strings.iter().map(|s| Prop::str(s.to_string())).collect(),
            other => {
                return Err(GraphError::InvalidGqlFilter(format!(
                    "expected a list of values, got {other:?}"
                )))
            }
        })
    };
    Ok(match op {
        FO::Eq => erased(lhs.eq(single(value)?)),
        FO::Ne => erased(lhs.ne(single(value)?)),
        FO::Gt => erased(lhs.gt(single(value)?)),
        FO::Ge => erased(lhs.ge(single(value)?)),
        FO::Lt => erased(lhs.lt(single(value)?)),
        FO::Le => erased(lhs.le(single(value)?)),
        FO::StartsWith => erased(lhs.starts_with(single(value)?)),
        FO::EndsWith => erased(lhs.ends_with(single(value)?)),
        FO::Contains => erased(lhs.contains(single(value)?)),
        FO::NotContains => erased(lhs.not_contains(single(value)?)),
        FO::FuzzySearch {
            levenshtein_distance,
            prefix_match,
        } => erased(lhs.fuzzy_search(single(value)?, levenshtein_distance, prefix_match)),
        FO::IsIn => erased(lhs.is_in(set(value)?)),
        FO::IsNotIn => erased(lhs.is_not_in(set(value)?)),
        FO::IsSome => erased(lhs.is_some()),
        FO::IsNone => erased(lhs.is_none()),
    })
}

fn node_field_lhs(field: NodeField) -> Arc<dyn DynCreateOp> {
    match field {
        NodeField::NodeId => Arc::new(NodeFilter.id()),
        NodeField::NodeName => Arc::new(NodeFilter.name()),
        NodeField::NodeType => Arc::new(NodeFilter.node_type()),
    }
}

fn node_field_filter(field: NodeField, cond: &NodeFieldCondition) -> Result<DynFilter, GraphError> {
    let (_, value, op) = translate_node_field_where(field, cond)?;
    apply_field_leaf(node_field_lhs(field), op, value)
}

pub(crate) fn lower_node_filter(filter: &GqlNodeFilter) -> Result<DynFilter, GraphError> {
    use GqlNodeFilter::*;
    Ok(match filter {
        Id(f) => node_field_filter(NodeField::NodeId, &f.where_)?,
        Name(f) => node_field_filter(NodeField::NodeName, &f.where_)?,
        NodeType(f) => node_field_filter(NodeField::NodeType, &f.where_)?,
        Degree(degree) => {
            let lhs: Arc<dyn DynCreateOp> = match degree.direction.into() {
                raphtory_api::core::Direction::BOTH => Arc::new(NodeFilter.degree()),
                raphtory_api::core::Direction::IN => Arc::new(NodeFilter.in_degree()),
                raphtory_api::core::Direction::OUT => Arc::new(NodeFilter.out_degree()),
            };
            let field_name: String = degree.direction.into();
            lower_prop_condition(lhs, &field_name, &degree.where_)?
        }
        Property(prop) => lower_prop_condition(
            Arc::new(PropertyExprFactory::property(&NodeFilter, &prop.name)),
            &prop.name,
            &prop.where_,
        )?,
        Metadata(prop) => lower_prop_condition(
            Arc::new(PropertyExprFactory::metadata(&NodeFilter, &prop.name)),
            &prop.name,
            &prop.where_,
        )?,
        TemporalProperty(prop) => {
            let temporal = PropertyExprFactory::property(&NodeFilter, &prop.name).temporal();
            lower_prop_condition(Arc::new(temporal), &prop.name, &prop.where_)?
        }
        And(filters) => combine_all(filters.iter().map(lower_node_filter), false, "and")?,
        Or(filters) => combine_all(filters.iter().map(lower_node_filter), true, "or")?,
        Not(inner) => Arc::new(lower_node_filter(inner.deref())?.not()) as DynFilter,
        Window(w) => erased(Windowed::new(
            w.start.clone().into_time(),
            w.end.clone().into_time(),
            lower_node_filter(w.expr.deref())?,
        )),
        At(t) => {
            let et = t.time.clone().into_time();
            erased(Windowed::new(
                et,
                EventTime::end(et.t().saturating_add(1)),
                lower_node_filter(t.expr.deref())?,
            ))
        }
        Before(t) => erased(Windowed::new(
            EventTime::start(i64::MIN),
            EventTime::end(t.time.clone().into_time().t()),
            lower_node_filter(t.expr.deref())?,
        )),
        After(t) => erased(Windowed::new(
            EventTime::start(t.time.clone().into_time().t().saturating_add(1)),
            EventTime::end(i64::MAX),
            lower_node_filter(t.expr.deref())?,
        )),
        Latest(u) => erased(LatestWrap::new(lower_node_filter(u.expr.deref())?)),
        SnapshotAt(t) => erased(SnapshotAtWrap::new(
            t.time.clone().into_time(),
            lower_node_filter(t.expr.deref())?,
        )),
        SnapshotLatest(u) => erased(SnapshotLatestWrap::new(lower_node_filter(u.expr.deref())?)),
        Layers(l) => erased(Layered::new(
            Layer::from(l.names.clone()),
            lower_node_filter(l.expr.deref())?,
        )),
        IsActive(true) => erased(NodeFilter.is_active()),
        IsActive(false) => Arc::new(erased(NodeFilter.is_active()).not()) as DynFilter,
    })
}

fn edge_prop_lhs(exploded: bool, kind: PropKind, name: &str) -> Arc<dyn DynCreateOp> {
    match (exploded, kind) {
        (false, PropKind::Property) => Arc::new(PropertyExprFactory::property(&EdgeFilter, name)),
        (false, PropKind::Metadata) => Arc::new(PropertyExprFactory::metadata(&EdgeFilter, name)),
        (false, PropKind::Temporal) => {
            Arc::new(PropertyExprFactory::property(&EdgeFilter, name).temporal())
        }
        (true, PropKind::Property) => {
            Arc::new(PropertyExprFactory::property(&ExplodedEdgeFilter, name))
        }
        (true, PropKind::Metadata) => {
            Arc::new(PropertyExprFactory::metadata(&ExplodedEdgeFilter, name))
        }
        (true, PropKind::Temporal) => {
            Arc::new(PropertyExprFactory::property(&ExplodedEdgeFilter, name).temporal())
        }
    }
}

#[derive(Clone, Copy)]
enum PropKind {
    Property,
    Metadata,
    Temporal,
}

pub(crate) fn lower_edge_filter(filter: &GqlEdgeFilter) -> Result<DynFilter, GraphError> {
    use GqlEdgeFilter::*;
    Ok(match filter {
        Src(inner) => erased(EdgeEndpointNodeFilter {
            endpoint: Endpoint::Src,
            inner: lower_node_filter(inner.deref())?,
        }),
        Dst(inner) => erased(EdgeEndpointNodeFilter {
            endpoint: Endpoint::Dst,
            inner: lower_node_filter(inner.deref())?,
        }),
        Property(prop) => lower_prop_condition(
            edge_prop_lhs(false, PropKind::Property, &prop.name),
            &prop.name,
            &prop.where_,
        )?,
        Metadata(prop) => lower_prop_condition(
            edge_prop_lhs(false, PropKind::Metadata, &prop.name),
            &prop.name,
            &prop.where_,
        )?,
        TemporalProperty(prop) => lower_prop_condition(
            edge_prop_lhs(false, PropKind::Temporal, &prop.name),
            &prop.name,
            &prop.where_,
        )?,
        And(filters) => combine_all(filters.iter().map(lower_edge_filter), false, "and")?,
        Or(filters) => combine_all(filters.iter().map(lower_edge_filter), true, "or")?,
        Not(inner) => Arc::new(lower_edge_filter(inner.deref())?.not()) as DynFilter,
        Window(w) => erased(Windowed::new(
            w.start.clone().into_time(),
            w.end.clone().into_time(),
            lower_edge_filter(w.expr.deref())?,
        )),
        At(t) => {
            let et = t.time.clone().into_time();
            erased(Windowed::new(
                et,
                EventTime::end(et.t().saturating_add(1)),
                lower_edge_filter(t.expr.deref())?,
            ))
        }
        Before(t) => erased(Windowed::new(
            EventTime::start(i64::MIN),
            EventTime::end(t.time.clone().into_time().t()),
            lower_edge_filter(t.expr.deref())?,
        )),
        After(t) => erased(Windowed::new(
            EventTime::start(t.time.clone().into_time().t().saturating_add(1)),
            EventTime::end(i64::MAX),
            lower_edge_filter(t.expr.deref())?,
        )),
        Latest(u) => erased(LatestWrap::new(lower_edge_filter(u.expr.deref())?)),
        SnapshotAt(t) => erased(SnapshotAtWrap::new(
            t.time.clone().into_time(),
            lower_edge_filter(t.expr.deref())?,
        )),
        SnapshotLatest(u) => erased(SnapshotLatestWrap::new(lower_edge_filter(u.expr.deref())?)),
        Layers(l) => erased(Layered::new(
            Layer::from(l.names.clone()),
            lower_edge_filter(l.expr.deref())?,
        )),
        IsActive(v) => bool_leaf(erased(EdgeFilter.is_active()), *v),
        IsValid(v) => bool_leaf(erased(EdgeFilter.is_valid()), *v),
        IsDeleted(v) => bool_leaf(erased(EdgeFilter.is_deleted()), *v),
        IsSelfLoop(v) => bool_leaf(erased(EdgeFilter.is_self_loop()), *v),
    })
}

fn bool_leaf(filter: DynFilter, wanted: bool) -> DynFilter {
    if wanted {
        filter
    } else {
        Arc::new(filter.not()) as DynFilter
    }
}

pub(crate) fn lower_exploded_edge_filter(
    filter: &GqlExplodedEdgeFilter,
) -> Result<DynFilter, GraphError> {
    use GqlExplodedEdgeFilter::*;
    Ok(match filter {
        Src(inner) => erased(EdgeEndpointNodeFilter {
            endpoint: Endpoint::Src,
            inner: lower_node_filter(inner.deref())?,
        }),
        Dst(inner) => erased(EdgeEndpointNodeFilter {
            endpoint: Endpoint::Dst,
            inner: lower_node_filter(inner.deref())?,
        }),
        Property(prop) => lower_prop_condition(
            edge_prop_lhs(true, PropKind::Property, &prop.name),
            &prop.name,
            &prop.where_,
        )?,
        Metadata(prop) => lower_prop_condition(
            edge_prop_lhs(true, PropKind::Metadata, &prop.name),
            &prop.name,
            &prop.where_,
        )?,
        TemporalProperty(prop) => lower_prop_condition(
            edge_prop_lhs(true, PropKind::Temporal, &prop.name),
            &prop.name,
            &prop.where_,
        )?,
        And(filters) => combine_all(filters.iter().map(lower_exploded_edge_filter), false, "and")?,
        Or(filters) => combine_all(filters.iter().map(lower_exploded_edge_filter), true, "or")?,
        Not(inner) => Arc::new(lower_exploded_edge_filter(inner.deref())?.not()) as DynFilter,
        Window(w) => erased(Windowed::new(
            w.start.clone().into_time(),
            w.end.clone().into_time(),
            lower_exploded_edge_filter(w.expr.deref())?,
        )),
        At(t) => {
            let et = t.time.clone().into_time();
            erased(Windowed::new(
                et,
                EventTime::end(et.t().saturating_add(1)),
                lower_exploded_edge_filter(t.expr.deref())?,
            ))
        }
        Before(t) => erased(Windowed::new(
            EventTime::start(i64::MIN),
            EventTime::end(t.time.clone().into_time().t()),
            lower_exploded_edge_filter(t.expr.deref())?,
        )),
        After(t) => erased(Windowed::new(
            EventTime::start(t.time.clone().into_time().t().saturating_add(1)),
            EventTime::end(i64::MAX),
            lower_exploded_edge_filter(t.expr.deref())?,
        )),
        Latest(u) => erased(LatestWrap::new(lower_exploded_edge_filter(u.expr.deref())?)),
        SnapshotAt(t) => erased(SnapshotAtWrap::new(
            t.time.clone().into_time(),
            lower_exploded_edge_filter(t.expr.deref())?,
        )),
        SnapshotLatest(u) => erased(SnapshotLatestWrap::new(lower_exploded_edge_filter(
            u.expr.deref(),
        )?)),
        Layers(l) => erased(Layered::new(
            Layer::from(l.names.clone()),
            lower_exploded_edge_filter(l.expr.deref())?,
        )),
        IsActive(v) => bool_leaf(erased(ExplodedEdgeFilter.is_active()), *v),
        IsValid(v) => bool_leaf(erased(ExplodedEdgeFilter.is_valid()), *v),
        IsDeleted(v) => bool_leaf(erased(ExplodedEdgeFilter.is_deleted()), *v),
        IsSelfLoop(v) => bool_leaf(erased(ExplodedEdgeFilter.is_self_loop()), *v),
    })
}
