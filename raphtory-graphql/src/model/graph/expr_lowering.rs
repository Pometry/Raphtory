//! Lowers the legacy GraphQL filter grammar onto the filter tree.
//!
//! The legacy grammar (`GqlNodeFilter` and friends) says "left side, operator,
//! constant" and wraps *filters* in views. The tree says the same thing with
//! expressions on both sides and views on the *reads*. This module is the only
//! place that knows both spellings: every legacy value becomes a tree here and
//! is compiled like any other tree, so nothing downstream sees the legacy shape.

use crate::model::graph::filtering::{
    translate_node_field_where, translate_prop_leaf_to_filter, GqlEdgeFilter,
    GqlExplodedEdgeFilter, GqlFilter, GqlGraphFilter, GqlNodeFilter, NodeField, NodeFieldCondition,
    PropCondition,
};
use raphtory::{
    db::graph::views::filter::model::{
        edge_filter::Endpoint,
        filter::FilterValue,
        property_filter::PropertyFilterValue,
        tree::{
            Agg, CmpOp, Entity, Expr, Field, FilterExpr, Qual, Scope, StrOp, Structural, Target,
            ViewOp,
        },
        FilterOperator,
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::properties::prop::{IntoProp, Prop},
    utils::time::IntoTime,
};
use std::ops::Deref;

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidGqlFilter(msg.into())
}

fn non_empty(items: Vec<FilterExpr>, what: &str) -> Result<Vec<FilterExpr>, GraphError> {
    if items.is_empty() {
        return Err(invalid(format!("Filter '{what}' requires non-empty list")));
    }
    Ok(items)
}

fn all(
    items: impl Iterator<Item = Result<FilterExpr, GraphError>>,
    what: &str,
) -> Result<Vec<FilterExpr>, GraphError> {
    non_empty(items.collect::<Result<Vec<_>, _>>()?, what)
}

// ── views and endpoints distribute onto the reads ────────────────────────────

fn map_scopes(filter: FilterExpr, f: &dyn Fn(&mut Scope)) -> FilterExpr {
    let expr = |e: Expr| map_expr_scopes(e, f);
    match filter {
        FilterExpr::Cmp { op, lhs, rhs } => FilterExpr::Cmp {
            op,
            lhs: expr(lhs),
            rhs: expr(rhs),
        },
        FilterExpr::Str { op, lhs, rhs } => FilterExpr::Str {
            op,
            lhs: expr(lhs),
            rhs: expr(rhs),
        },
        FilterExpr::IsSome(e) => FilterExpr::IsSome(expr(e)),
        FilterExpr::IsNone(e) => FilterExpr::IsNone(expr(e)),
        FilterExpr::In {
            expr: e,
            values,
            negated,
        } => FilterExpr::In {
            expr: expr(e),
            values,
            negated,
        },
        FilterExpr::Structural { mut scope, pred } => {
            f(&mut scope);
            FilterExpr::Structural { scope, pred }
        }
        FilterExpr::View(ops) => FilterExpr::View(ops),
        FilterExpr::And(items) => {
            FilterExpr::And(items.into_iter().map(|i| map_scopes(i, f)).collect())
        }
        FilterExpr::Or(items) => {
            FilterExpr::Or(items.into_iter().map(|i| map_scopes(i, f)).collect())
        }
        FilterExpr::Not(inner) => FilterExpr::Not(Box::new(map_scopes(*inner, f))),
        FilterExpr::Opaque(o) => FilterExpr::Opaque(o),
    }
}

fn map_expr_scopes(expr: Expr, f: &dyn Fn(&mut Scope)) -> Expr {
    match expr {
        Expr::Const(v) => Expr::Const(v),
        Expr::Read { mut scope, target } => {
            f(&mut scope);
            Expr::Read { scope, target }
        }
        Expr::Temporal(e) => Expr::Temporal(Box::new(map_expr_scopes(*e, f))),
        Expr::Agg(a, e) => Expr::Agg(a, Box::new(map_expr_scopes(*e, f))),
        Expr::Qual(q, e) => Expr::Qual(q, Box::new(map_expr_scopes(*e, f))),
    }
}

/// A legacy view wraps a whole filter; on the tree it scopes every read inside.
fn scoped(filter: FilterExpr, view: ViewOp) -> FilterExpr {
    map_scopes(filter, &|scope| scope.views.push(view.clone()))
}

/// A legacy `src`/`dst` wraps a node filter; on the tree every read inside
/// becomes an edge read through that endpoint.
fn through(filter: FilterExpr, entity: Entity, endpoint: Endpoint) -> FilterExpr {
    map_scopes(filter, &|scope| {
        scope.entity = entity;
        scope.endpoint = Some(endpoint);
    })
}

// ── leaves ───────────────────────────────────────────────────────────────────

fn cmp(op: CmpOp, lhs: Expr, rhs: Prop) -> FilterExpr {
    FilterExpr::Cmp {
        op,
        lhs,
        rhs: Expr::Const(rhs),
    }
}

fn str_op(op: StrOp, lhs: Expr, rhs: Prop) -> FilterExpr {
    FilterExpr::Str {
        op,
        lhs,
        rhs: Expr::Const(rhs),
    }
}

/// A legacy `operator + value` on an expression, with the value already in
/// property-filter shape (a single value, a set, or nothing).
fn leaf(
    lhs: Expr,
    op: FilterOperator,
    value: PropertyFilterValue,
) -> Result<FilterExpr, GraphError> {
    use FilterOperator as FO;
    use PropertyFilterValue as V;
    Ok(match (op, value) {
        (FO::Eq, V::Single(v)) => cmp(CmpOp::Eq, lhs, v),
        (FO::Ne, V::Single(v)) => cmp(CmpOp::Ne, lhs, v),
        (FO::Gt, V::Single(v)) => cmp(CmpOp::Gt, lhs, v),
        (FO::Ge, V::Single(v)) => cmp(CmpOp::Ge, lhs, v),
        (FO::Lt, V::Single(v)) => cmp(CmpOp::Lt, lhs, v),
        (FO::Le, V::Single(v)) => cmp(CmpOp::Le, lhs, v),
        (FO::StartsWith, V::Single(v)) => str_op(StrOp::StartsWith, lhs, v),
        (FO::EndsWith, V::Single(v)) => str_op(StrOp::EndsWith, lhs, v),
        (FO::Contains, V::Single(v)) => str_op(StrOp::Contains, lhs, v),
        (FO::NotContains, V::Single(v)) => str_op(StrOp::NotContains, lhs, v),
        (
            FO::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            V::Single(v),
        ) => str_op(
            StrOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            lhs,
            v,
        ),
        (FO::IsIn, V::Set(values)) => FilterExpr::In {
            expr: lhs,
            values: values.iter().cloned().collect(),
            negated: false,
        },
        (FO::IsNotIn, V::Set(values)) => FilterExpr::In {
            expr: lhs,
            values: values.iter().cloned().collect(),
            negated: true,
        },
        (FO::IsSome, V::None) => FilterExpr::IsSome(lhs),
        (FO::IsNone, V::None) => FilterExpr::IsNone(lhs),
        (op, _) => {
            return Err(invalid(format!(
                "operator {op:?} received an incompatible value shape"
            )))
        }
    })
}

/// The same for a built-in field, whose legacy value is a string or an id.
fn field_leaf(lhs: Expr, op: FilterOperator, value: FilterValue) -> Result<FilterExpr, GraphError> {
    let single = match value {
        FilterValue::ID(gid) => PropertyFilterValue::Single(gid.into_prop()),
        FilterValue::Single(s) => PropertyFilterValue::Single(Prop::str(s)),
        FilterValue::IDSet(gids) => PropertyFilterValue::Set(std::sync::Arc::new(
            gids.iter().map(|g| g.clone().into_prop()).collect(),
        )),
        FilterValue::Set(strings) => PropertyFilterValue::Set(std::sync::Arc::new(
            strings.iter().map(|s| Prop::str(s.to_string())).collect(),
        )),
    };
    let single = match (&op, single) {
        (FilterOperator::IsSome | FilterOperator::IsNone, _) => PropertyFilterValue::None,
        (_, v) => v,
    };
    leaf(lhs, op, single)
}

/// A legacy property condition: qualifiers and aggregates wrap the *condition*,
/// so each one moves onto the expression they qualify; `and`/`or`/`not` inside
/// a condition become combinators over copies of the expression.
fn prop_condition(lhs: Expr, name: &str, cond: &PropCondition) -> Result<FilterExpr, GraphError> {
    use PropCondition::*;
    let over = |wrap: fn(Box<Expr>) -> Expr, inner: &PropCondition| {
        prop_condition(wrap(Box::new(lhs.clone())), name, inner)
    };
    match cond {
        And(list) => Ok(FilterExpr::And(all(
            list.iter().map(|c| prop_condition(lhs.clone(), name, c)),
            "and",
        )?)),
        Or(list) => Ok(FilterExpr::Or(all(
            list.iter().map(|c| prop_condition(lhs.clone(), name, c)),
            "or",
        )?)),
        Not(inner) => Ok(FilterExpr::Not(Box::new(prop_condition(
            lhs,
            name,
            inner.deref(),
        )?))),
        First(inner) => over(|e| Expr::Agg(Agg::First, e), inner),
        Last(inner) => over(|e| Expr::Agg(Agg::Last, e), inner),
        Sum(inner) => over(|e| Expr::Agg(Agg::Sum, e), inner),
        Avg(inner) => over(|e| Expr::Agg(Agg::Avg, e), inner),
        Min(inner) => over(|e| Expr::Agg(Agg::Min, e), inner),
        Max(inner) => over(|e| Expr::Agg(Agg::Max, e), inner),
        Len(inner) => over(|e| Expr::Agg(Agg::Len, e), inner),
        Any(inner) => over(|e| Expr::Qual(Qual::Any, e), inner),
        All(inner) => over(|e| Expr::Qual(Qual::All, e), inner),
        leaf_cond => {
            let (op, value) = translate_prop_leaf_to_filter(name, leaf_cond)?;
            leaf(lhs, op, value)
        }
    }
}

fn read(entity: Entity, target: Target) -> Expr {
    Expr::Read {
        scope: Scope::new(entity),
        target,
    }
}

fn node_field(
    field: NodeField,
    tree_field: Field,
    cond: &NodeFieldCondition,
) -> Result<FilterExpr, GraphError> {
    let (_, value, op) = translate_node_field_where(field, cond)?;
    field_leaf(read(Entity::Node, Target::Field(tree_field)), op, value)
}

fn bool_leaf(filter: FilterExpr, wanted: bool) -> FilterExpr {
    if wanted {
        filter
    } else {
        FilterExpr::Not(Box::new(filter))
    }
}

// ── the three entity grammars ────────────────────────────────────────────────

pub(crate) fn lower_node_filter(filter: &GqlNodeFilter) -> Result<FilterExpr, GraphError> {
    use GqlNodeFilter::*;
    let entity = Entity::Node;
    Ok(match filter {
        Id(f) => node_field(NodeField::NodeId, Field::Id, &f.where_)?,
        Name(f) => node_field(NodeField::NodeName, Field::Name, &f.where_)?,
        NodeType(f) => node_field(NodeField::NodeType, Field::NodeType, &f.where_)?,
        Degree(d) => prop_condition(
            read(entity, Target::Degree(d.direction.into())),
            &String::from(d.direction),
            &d.where_,
        )?,
        Property(p) => prop_condition(
            read(entity, Target::Property(p.name.clone())),
            &p.name,
            &p.where_,
        )?,
        Metadata(p) => prop_condition(
            read(entity, Target::Metadata(p.name.clone())),
            &p.name,
            &p.where_,
        )?,
        TemporalProperty(p) => prop_condition(
            Expr::Temporal(Box::new(read(entity, Target::Property(p.name.clone())))),
            &p.name,
            &p.where_,
        )?,
        And(list) => FilterExpr::And(all(list.iter().map(lower_node_filter), "and")?),
        Or(list) => FilterExpr::Or(all(list.iter().map(lower_node_filter), "or")?),
        Not(inner) => FilterExpr::Not(Box::new(lower_node_filter(inner.deref())?)),
        Window(w) => scoped(
            lower_node_filter(w.expr.deref())?,
            ViewOp::Window {
                start: w.start.clone().into_time(),
                end: w.end.clone().into_time(),
            },
        ),
        At(t) => scoped(
            lower_node_filter(t.expr.deref())?,
            ViewOp::At(t.time.clone().into_time()),
        ),
        Before(t) => scoped(
            lower_node_filter(t.expr.deref())?,
            ViewOp::Before(t.time.clone().into_time()),
        ),
        After(t) => scoped(
            lower_node_filter(t.expr.deref())?,
            ViewOp::After(t.time.clone().into_time()),
        ),
        Latest(u) => scoped(lower_node_filter(u.expr.deref())?, ViewOp::Latest),
        SnapshotAt(t) => scoped(
            lower_node_filter(t.expr.deref())?,
            ViewOp::SnapshotAt(t.time.clone().into_time()),
        ),
        SnapshotLatest(u) => scoped(lower_node_filter(u.expr.deref())?, ViewOp::SnapshotLatest),
        Layers(l) => scoped(
            lower_node_filter(l.expr.deref())?,
            ViewOp::Layers(l.names.clone()),
        ),
        IsActive(wanted) => bool_leaf(
            FilterExpr::Structural {
                scope: Scope::new(entity),
                pred: Structural::IsActive,
            },
            *wanted,
        ),
    })
}

/// The edge and exploded-edge grammars are the same enum shape over two types;
/// one body serves both, parameterised by the entity the reads belong to.
macro_rules! lower_edge_like {
    ($name:ident, $ty:ident, $entity:expr) => {
        pub(crate) fn $name(filter: &$ty) -> Result<FilterExpr, GraphError> {
            use $ty::*;
            let entity = $entity;
            let structural = |pred: Structural, wanted: bool| {
                bool_leaf(
                    FilterExpr::Structural {
                        scope: Scope::new(entity),
                        pred,
                    },
                    wanted,
                )
            };
            Ok(match filter {
                Src(inner) => through(lower_node_filter(inner.deref())?, entity, Endpoint::Src),
                Dst(inner) => through(lower_node_filter(inner.deref())?, entity, Endpoint::Dst),
                Property(p) => prop_condition(
                    read(entity, Target::Property(p.name.clone())),
                    &p.name,
                    &p.where_,
                )?,
                Metadata(p) => prop_condition(
                    read(entity, Target::Metadata(p.name.clone())),
                    &p.name,
                    &p.where_,
                )?,
                TemporalProperty(p) => prop_condition(
                    Expr::Temporal(Box::new(read(entity, Target::Property(p.name.clone())))),
                    &p.name,
                    &p.where_,
                )?,
                And(list) => FilterExpr::And(all(list.iter().map($name), "and")?),
                Or(list) => FilterExpr::Or(all(list.iter().map($name), "or")?),
                Not(inner) => FilterExpr::Not(Box::new($name(inner.deref())?)),
                Window(w) => scoped(
                    $name(w.expr.deref())?,
                    ViewOp::Window {
                        start: w.start.clone().into_time(),
                        end: w.end.clone().into_time(),
                    },
                ),
                At(t) => scoped(
                    $name(t.expr.deref())?,
                    ViewOp::At(t.time.clone().into_time()),
                ),
                Before(t) => scoped(
                    $name(t.expr.deref())?,
                    ViewOp::Before(t.time.clone().into_time()),
                ),
                After(t) => scoped(
                    $name(t.expr.deref())?,
                    ViewOp::After(t.time.clone().into_time()),
                ),
                Latest(u) => scoped($name(u.expr.deref())?, ViewOp::Latest),
                SnapshotAt(t) => scoped(
                    $name(t.expr.deref())?,
                    ViewOp::SnapshotAt(t.time.clone().into_time()),
                ),
                SnapshotLatest(u) => scoped($name(u.expr.deref())?, ViewOp::SnapshotLatest),
                Layers(l) => scoped($name(l.expr.deref())?, ViewOp::Layers(l.names.clone())),
                IsActive(v) => structural(Structural::IsActive, *v),
                IsValid(v) => structural(Structural::IsValid, *v),
                IsDeleted(v) => structural(Structural::IsDeleted, *v),
                IsSelfLoop(v) => structural(Structural::IsSelfLoop, *v),
            })
        }
    };
}

lower_edge_like!(lower_edge_filter, GqlEdgeFilter, Entity::Edge);
lower_edge_like!(
    lower_exploded_edge_filter,
    GqlExplodedEdgeFilter,
    Entity::ExplodedEdge
);

/// A legacy graph view nests inner-first: `window { expr: latest }` is the
/// latest state, then the window. The tree lists ops in application order.
pub(crate) fn lower_graph_filter(filter: &GqlGraphFilter) -> Result<Vec<ViewOp>, GraphError> {
    use GqlGraphFilter::*;
    let inner = |expr: &Option<crate::model::graph::filtering::Wrapped<GqlGraphFilter>>| {
        expr.as_ref()
            .map(|e| lower_graph_filter(e.deref()))
            .unwrap_or_else(|| Ok(Vec::new()))
    };
    let (mut ops, op) = match filter {
        Window(w) => (
            inner(&w.expr)?,
            ViewOp::Window {
                start: w.start.clone().into_time(),
                end: w.end.clone().into_time(),
            },
        ),
        At(t) => (inner(&t.expr)?, ViewOp::At(t.time.clone().into_time())),
        Before(t) => (inner(&t.expr)?, ViewOp::Before(t.time.clone().into_time())),
        After(t) => (inner(&t.expr)?, ViewOp::After(t.time.clone().into_time())),
        Latest(u) => (inner(&u.expr)?, ViewOp::Latest),
        SnapshotAt(t) => (
            inner(&t.expr)?,
            ViewOp::SnapshotAt(t.time.clone().into_time()),
        ),
        SnapshotLatest(u) => (inner(&u.expr)?, ViewOp::SnapshotLatest),
        Layers(l) => (inner(&l.expr)?, ViewOp::Layers(l.names.clone())),
    };
    ops.push(op);
    Ok(ops)
}

/// Any legacy filter, as a tree.
pub(crate) fn lower_filter(filter: &GqlFilter) -> Result<FilterExpr, GraphError> {
    use GqlFilter::*;
    Ok(match filter {
        GqlFilter::Expr(tree) => FilterExpr::try_from(tree.clone())?,
        Node(f) => lower_node_filter(f)?,
        Edge(f) => lower_edge_filter(f)?,
        ExplodedEdge(f) => lower_exploded_edge_filter(f)?,
        Graph(g) => FilterExpr::View(lower_graph_filter(g)?),
        And(list) => FilterExpr::And(all(list.iter().map(lower_filter), "and")?),
        Or(list) => FilterExpr::Or(all(list.iter().map(lower_filter), "or")?),
        Not(inner) => FilterExpr::Not(Box::new(lower_filter(inner.deref())?)),
        Window(w) => FilterExpr::View(lower_graph_filter(&GqlGraphFilter::Window(w.clone()))?),
        At(t) => FilterExpr::View(lower_graph_filter(&GqlGraphFilter::At(t.clone()))?),
        Before(t) => FilterExpr::View(lower_graph_filter(&GqlGraphFilter::Before(t.clone()))?),
        After(t) => FilterExpr::View(lower_graph_filter(&GqlGraphFilter::After(t.clone()))?),
        Latest(u) => FilterExpr::View(lower_graph_filter(&GqlGraphFilter::Latest(u.clone()))?),
        SnapshotAt(t) => {
            FilterExpr::View(lower_graph_filter(&GqlGraphFilter::SnapshotAt(t.clone()))?)
        }
        SnapshotLatest(u) => FilterExpr::View(lower_graph_filter(
            &GqlGraphFilter::SnapshotLatest(u.clone()),
        )?),
        Layers(l) => FilterExpr::View(lower_graph_filter(&GqlGraphFilter::Layers(l.clone()))?),
    })
}
