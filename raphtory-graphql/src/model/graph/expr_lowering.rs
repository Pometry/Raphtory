//! Converts the legacy GraphQL filter grammar onto the tree grammar.
//!
//! The legacy grammar (`GqlNodeFilter` and friends) says "left side, operator,
//! constant" and wraps *filters* in views. The tree grammar says the same thing
//! with expressions on both sides and views on the *reads*. This module is the
//! only place that knows both spellings, and it works purely on the wire types:
//! constants stay [`Value`]s, so a `{"var": …}` or `{"claim": …}` placeholder
//! in a stored permission grant survives the conversion untouched and is
//! substituted later, exactly as before.

use crate::model::graph::{
    filter_expr_input::{
        GqlCmp, GqlEndpoint, GqlEntity, GqlExpr, GqlFilterExpr, GqlFuzzyCmp, GqlMembership,
        GqlNodeField, GqlRead, GqlScope, GqlTarget, GqlViewOp,
    },
    filtering::{
        FuzzySearchExpr, GqlEdgeFilter, GqlExplodedEdgeFilter, GqlFilter, GqlGraphFilter,
        GqlNodeFilter, NodeFieldCondition, PropCondition, Window, Wrapped,
    },
    property::Value,
    timeindex::GqlTimeInput,
};
use raphtory::errors::GraphError;
use std::ops::Deref;

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidGqlFilter(msg.into())
}

fn all(
    items: impl Iterator<Item = Result<GqlFilterExpr, GraphError>>,
    what: &str,
) -> Result<Vec<GqlFilterExpr>, GraphError> {
    let items = items.collect::<Result<Vec<_>, _>>()?;
    if items.is_empty() {
        return Err(invalid(format!("Filter '{what}' requires non-empty list")));
    }
    Ok(items)
}

// ── views and endpoints distribute onto the reads ────────────────────────────

/// What a scope rewrite may change: the entity, the view chain, the endpoint.
type ScopeEdit<'a> =
    &'a dyn Fn(&mut GqlEntity, &mut Option<Vec<GqlViewOp>>, &mut Option<GqlEndpoint>);

fn edit_scope(scope: GqlScope, f: ScopeEdit) -> GqlScope {
    let GqlScope {
        mut entity,
        mut views,
        mut endpoint,
    } = scope;
    f(&mut entity, &mut views, &mut endpoint);
    GqlScope {
        entity,
        views,
        endpoint,
    }
}

fn map_scopes(filter: GqlFilterExpr, f: ScopeEdit) -> GqlFilterExpr {
    use GqlFilterExpr as F;
    let cmp = |c: GqlCmp| GqlCmp {
        lhs: map_expr_scopes(c.lhs, f),
        rhs: map_expr_scopes(c.rhs, f),
    };
    let wrapped = |e: Wrapped<GqlExpr>| Wrapped::from(map_expr_scopes(e.deref().clone(), f));
    let member = |m: GqlMembership| GqlMembership {
        expr: map_expr_scopes(m.expr, f),
        values: m.values,
    };
    match filter {
        F::Eq(c) => F::Eq(cmp(c)),
        F::Ne(c) => F::Ne(cmp(c)),
        F::Lt(c) => F::Lt(cmp(c)),
        F::Le(c) => F::Le(cmp(c)),
        F::Gt(c) => F::Gt(cmp(c)),
        F::Ge(c) => F::Ge(cmp(c)),
        F::StartsWith(c) => F::StartsWith(cmp(c)),
        F::EndsWith(c) => F::EndsWith(cmp(c)),
        F::Contains(c) => F::Contains(cmp(c)),
        F::NotContains(c) => F::NotContains(cmp(c)),
        F::FuzzySearch(z) => F::FuzzySearch(GqlFuzzyCmp {
            lhs: map_expr_scopes(z.lhs, f),
            rhs: map_expr_scopes(z.rhs, f),
            levenshtein_distance: z.levenshtein_distance,
            prefix_match: z.prefix_match,
        }),
        F::IsSome(e) => F::IsSome(wrapped(e)),
        F::IsNone(e) => F::IsNone(wrapped(e)),
        F::IsIn(m) => F::IsIn(member(m)),
        F::IsNotIn(m) => F::IsNotIn(member(m)),
        F::IsActive(s) => F::IsActive(edit_scope(s, f)),
        F::IsValid(s) => F::IsValid(edit_scope(s, f)),
        F::IsDeleted(s) => F::IsDeleted(edit_scope(s, f)),
        F::IsSelfLoop(s) => F::IsSelfLoop(edit_scope(s, f)),
        F::View(ops) => F::View(ops),
        F::And(items) => F::And(items.into_iter().map(|i| map_scopes(i, f)).collect()),
        F::Or(items) => F::Or(items.into_iter().map(|i| map_scopes(i, f)).collect()),
        F::Not(inner) => F::Not(Wrapped::from(map_scopes(inner.deref().clone(), f))),
    }
}

fn map_expr_scopes(expr: GqlExpr, f: ScopeEdit) -> GqlExpr {
    use GqlExpr as E;
    let wrapped = |e: Wrapped<GqlExpr>| Wrapped::from(map_expr_scopes(e.deref().clone(), f));
    match expr {
        E::Const(v) => E::Const(v),
        E::Read(read) => {
            let GqlRead {
                mut entity,
                mut views,
                mut endpoint,
                target,
            } = read;
            f(&mut entity, &mut views, &mut endpoint);
            E::Read(GqlRead {
                entity,
                views,
                endpoint,
                target,
            })
        }
        E::Temporal(e) => E::Temporal(wrapped(e)),
        E::Sum(e) => E::Sum(wrapped(e)),
        E::Avg(e) => E::Avg(wrapped(e)),
        E::Min(e) => E::Min(wrapped(e)),
        E::Max(e) => E::Max(wrapped(e)),
        E::First(e) => E::First(wrapped(e)),
        E::Last(e) => E::Last(wrapped(e)),
        E::Len(e) => E::Len(wrapped(e)),
        E::Any(e) => E::Any(wrapped(e)),
        E::All(e) => E::All(wrapped(e)),
    }
}

/// A legacy view wraps a whole filter; on the tree it scopes every read inside.
fn scoped(filter: GqlFilterExpr, view: GqlViewOp) -> GqlFilterExpr {
    map_scopes(filter, &|_, views, _| {
        views.get_or_insert_with(Vec::new).push(view.clone())
    })
}

/// A legacy `src`/`dst` wraps a node filter; on the tree every read inside
/// becomes an edge read through that endpoint.
fn through(filter: GqlFilterExpr, entity: GqlEntity, endpoint: GqlEndpoint) -> GqlFilterExpr {
    map_scopes(filter, &|e, _, ep| {
        *e = entity;
        *ep = Some(endpoint);
    })
}

// ── leaves ───────────────────────────────────────────────────────────────────

fn read(entity: GqlEntity, target: GqlTarget) -> GqlExpr {
    GqlExpr::Read(GqlRead {
        entity,
        views: None,
        endpoint: None,
        target,
    })
}

fn cmp(lhs: GqlExpr, value: &Value) -> GqlCmp {
    GqlCmp {
        lhs,
        rhs: GqlExpr::Const(value.clone()),
    }
}

fn fuzzy(lhs: GqlExpr, f: &FuzzySearchExpr) -> GqlFilterExpr {
    GqlFilterExpr::FuzzySearch(GqlFuzzyCmp {
        lhs,
        rhs: GqlExpr::Const(Value::Str(f.value.clone())),
        levenshtein_distance: f.levenshtein_distance,
        prefix_match: f.prefix_match,
    })
}

fn membership(lhs: GqlExpr, values: &Value) -> GqlMembership {
    GqlMembership {
        expr: lhs,
        values: values.clone(),
    }
}

fn presence(lhs: GqlExpr, some: bool) -> GqlFilterExpr {
    if some {
        GqlFilterExpr::IsSome(Wrapped::from(lhs))
    } else {
        GqlFilterExpr::IsNone(Wrapped::from(lhs))
    }
}

/// A legacy property condition: qualifiers and aggregates wrap the *condition*,
/// so each one moves onto the expression they qualify; `and`/`or`/`not` inside
/// a condition become combinators over copies of the expression.
fn prop_condition(lhs: GqlExpr, cond: &PropCondition) -> Result<GqlFilterExpr, GraphError> {
    use GqlFilterExpr as F;
    use PropCondition::*;
    let over = |wrap: fn(Wrapped<GqlExpr>) -> GqlExpr, inner: &PropCondition| {
        prop_condition(wrap(Wrapped::from(lhs.clone())), inner)
    };
    Ok(match cond {
        Eq(v) => F::Eq(cmp(lhs, v)),
        Ne(v) => F::Ne(cmp(lhs, v)),
        Gt(v) => F::Gt(cmp(lhs, v)),
        Ge(v) => F::Ge(cmp(lhs, v)),
        Lt(v) => F::Lt(cmp(lhs, v)),
        Le(v) => F::Le(cmp(lhs, v)),
        StartsWith(v) => F::StartsWith(cmp(lhs, v)),
        EndsWith(v) => F::EndsWith(cmp(lhs, v)),
        Contains(v) => F::Contains(cmp(lhs, v)),
        NotContains(v) => F::NotContains(cmp(lhs, v)),
        FuzzySearch(f) => fuzzy(lhs, f),
        IsIn(v) => F::IsIn(membership(lhs, v)),
        IsNotIn(v) => F::IsNotIn(membership(lhs, v)),
        // `isSome: false` is `isNone`, and the other way round.
        IsSome(wanted) => presence(lhs, *wanted),
        IsNone(wanted) => presence(lhs, !*wanted),
        And(list) => F::And(all(
            list.iter().map(|c| prop_condition(lhs.clone(), c)),
            "and",
        )?),
        Or(list) => F::Or(all(
            list.iter().map(|c| prop_condition(lhs.clone(), c)),
            "or",
        )?),
        Not(inner) => F::Not(Wrapped::from(prop_condition(lhs, inner.deref())?)),
        First(inner) => over(GqlExpr::First, inner)?,
        Last(inner) => over(GqlExpr::Last, inner)?,
        Sum(inner) => over(GqlExpr::Sum, inner)?,
        Avg(inner) => over(GqlExpr::Avg, inner)?,
        Min(inner) => over(GqlExpr::Min, inner)?,
        Max(inner) => over(GqlExpr::Max, inner)?,
        Len(inner) => over(GqlExpr::Len, inner)?,
        Any(inner) => over(GqlExpr::Any, inner)?,
        All(inner) => over(GqlExpr::All, inner)?,
    })
}

/// A legacy condition on a built-in node field.
fn field_condition(lhs: GqlExpr, cond: &NodeFieldCondition) -> GqlFilterExpr {
    use GqlFilterExpr as F;
    use NodeFieldCondition::*;
    match cond {
        Eq(v) => F::Eq(cmp(lhs, v)),
        Ne(v) => F::Ne(cmp(lhs, v)),
        Gt(v) => F::Gt(cmp(lhs, v)),
        Ge(v) => F::Ge(cmp(lhs, v)),
        Lt(v) => F::Lt(cmp(lhs, v)),
        Le(v) => F::Le(cmp(lhs, v)),
        StartsWith(v) => F::StartsWith(cmp(lhs, v)),
        EndsWith(v) => F::EndsWith(cmp(lhs, v)),
        Contains(v) => F::Contains(cmp(lhs, v)),
        NotContains(v) => F::NotContains(cmp(lhs, v)),
        FuzzySearch(f) => fuzzy(lhs, f),
        IsIn(v) => F::IsIn(membership(lhs, v)),
        IsNotIn(v) => F::IsNotIn(membership(lhs, v)),
    }
}

fn bool_leaf(filter: GqlFilterExpr, wanted: bool) -> GqlFilterExpr {
    if wanted {
        filter
    } else {
        GqlFilterExpr::Not(Wrapped::from(filter))
    }
}

fn structural(entity: GqlEntity) -> GqlScope {
    GqlScope {
        entity,
        views: None,
        endpoint: None,
    }
}

fn window(start: &GqlTimeInput, end: &GqlTimeInput) -> GqlViewOp {
    GqlViewOp::Window(Window {
        start: start.clone(),
        end: end.clone(),
    })
}

// ── the three entity grammars ────────────────────────────────────────────────

pub(crate) fn lower_node_filter(filter: &GqlNodeFilter) -> Result<GqlFilterExpr, GraphError> {
    use GqlNodeFilter::*;
    let entity = GqlEntity::Node;
    let field = |f: GqlNodeField| read(entity, GqlTarget::Field(f));
    Ok(match filter {
        Id(f) => field_condition(field(GqlNodeField::Id), &f.where_),
        Name(f) => field_condition(field(GqlNodeField::Name), &f.where_),
        NodeType(f) => field_condition(field(GqlNodeField::NodeType), &f.where_),
        Degree(d) => prop_condition(read(entity, GqlTarget::Degree(d.direction)), &d.where_)?,
        Property(p) => {
            prop_condition(read(entity, GqlTarget::Property(p.name.clone())), &p.where_)?
        }
        Metadata(p) => {
            prop_condition(read(entity, GqlTarget::Metadata(p.name.clone())), &p.where_)?
        }
        TemporalProperty(p) => prop_condition(
            GqlExpr::Temporal(Wrapped::from(read(
                entity,
                GqlTarget::Property(p.name.clone()),
            ))),
            &p.where_,
        )?,
        And(list) => GqlFilterExpr::And(all(list.iter().map(lower_node_filter), "and")?),
        Or(list) => GqlFilterExpr::Or(all(list.iter().map(lower_node_filter), "or")?),
        Not(inner) => GqlFilterExpr::Not(Wrapped::from(lower_node_filter(inner.deref())?)),
        Window(w) => scoped(lower_node_filter(w.expr.deref())?, window(&w.start, &w.end)),
        At(t) => scoped(
            lower_node_filter(t.expr.deref())?,
            GqlViewOp::At(t.time.clone()),
        ),
        Before(t) => scoped(
            lower_node_filter(t.expr.deref())?,
            GqlViewOp::Before(t.time.clone()),
        ),
        After(t) => scoped(
            lower_node_filter(t.expr.deref())?,
            GqlViewOp::After(t.time.clone()),
        ),
        Latest(u) => scoped(lower_node_filter(u.expr.deref())?, GqlViewOp::Latest(true)),
        SnapshotAt(t) => scoped(
            lower_node_filter(t.expr.deref())?,
            GqlViewOp::SnapshotAt(t.time.clone()),
        ),
        SnapshotLatest(u) => scoped(
            lower_node_filter(u.expr.deref())?,
            GqlViewOp::SnapshotLatest(true),
        ),
        Layers(l) => scoped(
            lower_node_filter(l.expr.deref())?,
            GqlViewOp::Layers(l.names.clone()),
        ),
        IsActive(wanted) => bool_leaf(GqlFilterExpr::IsActive(structural(entity)), *wanted),
    })
}

/// The edge and exploded-edge grammars are the same enum shape over two types;
/// one body serves both, parameterised by the entity the reads belong to.
macro_rules! lower_edge_like {
    ($name:ident, $ty:ident, $entity:expr) => {
        pub(crate) fn $name(filter: &$ty) -> Result<GqlFilterExpr, GraphError> {
            use $ty::*;
            let entity = $entity;
            let pred = |mk: fn(GqlScope) -> GqlFilterExpr, wanted: bool| {
                bool_leaf(mk(structural(entity)), wanted)
            };
            Ok(match filter {
                Src(inner) => through(lower_node_filter(inner.deref())?, entity, GqlEndpoint::Src),
                Dst(inner) => through(lower_node_filter(inner.deref())?, entity, GqlEndpoint::Dst),
                Property(p) => {
                    prop_condition(read(entity, GqlTarget::Property(p.name.clone())), &p.where_)?
                }
                Metadata(p) => {
                    prop_condition(read(entity, GqlTarget::Metadata(p.name.clone())), &p.where_)?
                }
                TemporalProperty(p) => prop_condition(
                    GqlExpr::Temporal(Wrapped::from(read(
                        entity,
                        GqlTarget::Property(p.name.clone()),
                    ))),
                    &p.where_,
                )?,
                And(list) => GqlFilterExpr::And(all(list.iter().map($name), "and")?),
                Or(list) => GqlFilterExpr::Or(all(list.iter().map($name), "or")?),
                Not(inner) => GqlFilterExpr::Not(Wrapped::from($name(inner.deref())?)),
                Window(w) => scoped($name(w.expr.deref())?, window(&w.start, &w.end)),
                At(t) => scoped($name(t.expr.deref())?, GqlViewOp::At(t.time.clone())),
                Before(t) => scoped($name(t.expr.deref())?, GqlViewOp::Before(t.time.clone())),
                After(t) => scoped($name(t.expr.deref())?, GqlViewOp::After(t.time.clone())),
                Latest(u) => scoped($name(u.expr.deref())?, GqlViewOp::Latest(true)),
                SnapshotAt(t) => scoped(
                    $name(t.expr.deref())?,
                    GqlViewOp::SnapshotAt(t.time.clone()),
                ),
                SnapshotLatest(u) => {
                    scoped($name(u.expr.deref())?, GqlViewOp::SnapshotLatest(true))
                }
                Layers(l) => scoped($name(l.expr.deref())?, GqlViewOp::Layers(l.names.clone())),
                IsActive(v) => pred(GqlFilterExpr::IsActive, *v),
                IsValid(v) => pred(GqlFilterExpr::IsValid, *v),
                IsDeleted(v) => pred(GqlFilterExpr::IsDeleted, *v),
                IsSelfLoop(v) => pred(GqlFilterExpr::IsSelfLoop, *v),
            })
        }
    };
}

lower_edge_like!(lower_edge_filter, GqlEdgeFilter, GqlEntity::Edge);
lower_edge_like!(
    lower_exploded_edge_filter,
    GqlExplodedEdgeFilter,
    GqlEntity::ExplodedEdge
);

/// A legacy graph view nests inner-first: `window { expr: latest }` is the
/// latest state, then the window. The tree lists ops in application order.
pub(crate) fn lower_graph_filter(filter: &GqlGraphFilter) -> Result<Vec<GqlViewOp>, GraphError> {
    use GqlGraphFilter::*;
    let inner = |expr: &Option<Wrapped<GqlGraphFilter>>| {
        expr.as_ref()
            .map(|e| lower_graph_filter(e.deref()))
            .unwrap_or_else(|| Ok(Vec::new()))
    };
    let (mut ops, op) = match filter {
        Window(w) => (inner(&w.expr)?, window(&w.start, &w.end)),
        At(t) => (inner(&t.expr)?, GqlViewOp::At(t.time.clone())),
        Before(t) => (inner(&t.expr)?, GqlViewOp::Before(t.time.clone())),
        After(t) => (inner(&t.expr)?, GqlViewOp::After(t.time.clone())),
        Latest(u) => (inner(&u.expr)?, GqlViewOp::Latest(true)),
        SnapshotAt(t) => (inner(&t.expr)?, GqlViewOp::SnapshotAt(t.time.clone())),
        SnapshotLatest(u) => (inner(&u.expr)?, GqlViewOp::SnapshotLatest(true)),
        Layers(l) => (inner(&l.expr)?, GqlViewOp::Layers(l.names.clone())),
    };
    ops.push(op);
    Ok(ops)
}

/// Any filter, in the tree grammar. A filter already written as a tree passes
/// through; every legacy spelling is converted.
pub(crate) fn lower_filter(filter: &GqlFilter) -> Result<GqlFilterExpr, GraphError> {
    use GqlFilter::*;
    Ok(match filter {
        GqlFilter::Expr(tree) => tree.clone(),
        Node(f) => lower_node_filter(f)?,
        Edge(f) => lower_edge_filter(f)?,
        ExplodedEdge(f) => lower_exploded_edge_filter(f)?,
        Graph(g) => GqlFilterExpr::View(lower_graph_filter(g)?),
        And(list) => GqlFilterExpr::And(all(list.iter().map(lower_filter), "and")?),
        Or(list) => GqlFilterExpr::Or(all(list.iter().map(lower_filter), "or")?),
        Not(inner) => GqlFilterExpr::Not(Wrapped::from(lower_filter(inner.deref())?)),
        Window(w) => GqlFilterExpr::View(lower_graph_filter(&GqlGraphFilter::Window(w.clone()))?),
        At(t) => GqlFilterExpr::View(lower_graph_filter(&GqlGraphFilter::At(t.clone()))?),
        Before(t) => GqlFilterExpr::View(lower_graph_filter(&GqlGraphFilter::Before(t.clone()))?),
        After(t) => GqlFilterExpr::View(lower_graph_filter(&GqlGraphFilter::After(t.clone()))?),
        Latest(u) => GqlFilterExpr::View(lower_graph_filter(&GqlGraphFilter::Latest(u.clone()))?),
        SnapshotAt(t) => {
            GqlFilterExpr::View(lower_graph_filter(&GqlGraphFilter::SnapshotAt(t.clone()))?)
        }
        SnapshotLatest(u) => GqlFilterExpr::View(lower_graph_filter(
            &GqlGraphFilter::SnapshotLatest(u.clone()),
        )?),
        Layers(l) => GqlFilterExpr::View(lower_graph_filter(&GqlGraphFilter::Layers(l.clone()))?),
    })
}
