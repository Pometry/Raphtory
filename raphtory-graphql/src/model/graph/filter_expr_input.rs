//! The GraphQL form of the filter tree.
//!
//! One input type per node of [`tree::FilterExpr`] / [`tree::Expr`], named the
//! same way, so a filter written in python, rust or a GraphQL document is the
//! same tree spelled in three syntaxes. Both sides of a comparison are
//! expressions; a constant is the `const` expression.
//!
//! ```graphql
//! filter(expr: { gt: { lhs: { read: { entity: NODE, target: { degree: BOTH } } },
//!                      rhs: { read: { entity: NODE, target: { degree: IN } } } } })
//! ```

use crate::model::graph::{
    filtering::{Window, Wrapped},
    property::Value,
    timeindex::GqlTimeInput,
};
use dynamic_graphql::{Enum, InputObject, OneOfInput};
use raphtory::{
    db::{
        api::{
            state::NodeOp,
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                edge_filter::Endpoint,
                tree::{
                    self, Agg, CmpOp, Entity, Field, FilterExpr, Qual, Scope, StrOp, Structural,
                    Target, ViewOp, OPAQUE_FILTER_ERROR,
                },
                DynFilter,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::properties::prop::Prop,
    storage::timeindex::EventTime,
    utils::time::{InputTime, IntoTime},
    Direction,
};
use serde::{Deserialize, Serialize};
use std::{ops::Deref, sync::Arc};

#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[graphql(name = "Entity")]
pub enum GqlEntity {
    Node,
    Edge,
    ExplodedEdge,
}

#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[graphql(name = "Endpoint")]
pub enum GqlEndpoint {
    Src,
    Dst,
}

#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[graphql(name = "NodeFieldName")]
pub enum GqlNodeField {
    Id,
    Name,
    NodeType,
}

/// One view restriction, applied in list order.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "ViewOp")]
pub enum GqlViewOp {
    Window(Window),
    At(GqlTimeInput),
    After(GqlTimeInput),
    Before(GqlTimeInput),
    Latest(bool),
    SnapshotAt(GqlTimeInput),
    SnapshotLatest(bool),
    Layers(Vec<String>),
}

/// The direction a node degree counts.
#[derive(Enum, Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DegreeDirection {
    In,
    Out,
    Both,
}

impl From<DegreeDirection> for Direction {
    fn from(d: DegreeDirection) -> Self {
        match d {
            DegreeDirection::In => Direction::IN,
            DegreeDirection::Out => Direction::OUT,
            DegreeDirection::Both => Direction::BOTH,
        }
    }
}

/// What a read selects on its entity.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "Target")]
pub enum GqlTarget {
    /// A built-in node field.
    Field(GqlNodeField),
    /// A node degree in a direction.
    Degree(DegreeDirection),
    /// A property, by name.
    Property(String),
    /// A metadata entry, by name.
    Metadata(String),
}

/// Where a value is read: the entity, the views to read it through, and for an
/// edge optionally one of its endpoint nodes.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "Scope")]
pub struct GqlScope {
    pub entity: GqlEntity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<Vec<GqlViewOp>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<GqlEndpoint>,
}

/// A value read from an entity.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "Read")]
pub struct GqlRead {
    pub entity: GqlEntity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<Vec<GqlViewOp>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<GqlEndpoint>,
    pub target: GqlTarget,
}

/// A value: what stands on either side of a comparison.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "Expr")]
pub enum GqlExpr {
    /// A literal.
    Const(Value),
    /// A field, degree, property or metadata read from an entity.
    Read(GqlRead),
    /// The full history of a property instead of its latest value.
    Temporal(Wrapped<GqlExpr>),
    Sum(Wrapped<GqlExpr>),
    Avg(Wrapped<GqlExpr>),
    Min(Wrapped<GqlExpr>),
    Max(Wrapped<GqlExpr>),
    First(Wrapped<GqlExpr>),
    Last(Wrapped<GqlExpr>),
    Len(Wrapped<GqlExpr>),
    /// The predicate holds if it holds for any element.
    Any(Wrapped<GqlExpr>),
    /// The predicate holds if it holds for every element.
    All(Wrapped<GqlExpr>),
}

/// Two expressions to compare.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "Cmp")]
pub struct GqlCmp {
    pub lhs: GqlExpr,
    pub rhs: GqlExpr,
}

#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "FuzzyCmp")]
pub struct GqlFuzzyCmp {
    pub lhs: GqlExpr,
    pub rhs: GqlExpr,
    pub levenshtein_distance: usize,
    pub prefix_match: bool,
}

/// A membership test. `values` is a list; a policy may also leave a single
/// placeholder here (`{"var": …}`) that resolves to the list per caller.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "Membership")]
pub struct GqlMembership {
    pub expr: GqlExpr,
    pub values: Value,
}

/// The filter itself: a yes/no over an entity.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "FilterExpr")]
pub enum GqlFilter {
    Eq(GqlCmp),
    Ne(GqlCmp),
    Lt(GqlCmp),
    Le(GqlCmp),
    Gt(GqlCmp),
    Ge(GqlCmp),
    StartsWith(GqlCmp),
    EndsWith(GqlCmp),
    Contains(GqlCmp),
    NotContains(GqlCmp),
    FuzzySearch(GqlFuzzyCmp),
    IsSome(Wrapped<GqlExpr>),
    IsNone(Wrapped<GqlExpr>),
    IsIn(GqlMembership),
    IsNotIn(GqlMembership),
    IsActive(GqlScope),
    IsValid(GqlScope),
    IsDeleted(GqlScope),
    IsSelfLoop(GqlScope),
    /// A graph-level view with no predicate: the result is the view.
    View(Vec<GqlViewOp>),
    And(Vec<GqlFilter>),
    Or(Vec<GqlFilter>),
    Not(Wrapped<GqlFilter>),
}

// ── GraphQL → tree ───────────────────────────────────────────────────────────

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidGqlFilter(msg.into())
}

impl From<GqlEntity> for Entity {
    fn from(e: GqlEntity) -> Self {
        match e {
            GqlEntity::Node => Entity::Node,
            GqlEntity::Edge => Entity::Edge,
            GqlEntity::ExplodedEdge => Entity::ExplodedEdge,
        }
    }
}

impl From<GqlEndpoint> for Endpoint {
    fn from(e: GqlEndpoint) -> Self {
        match e {
            GqlEndpoint::Src => Endpoint::Src,
            GqlEndpoint::Dst => Endpoint::Dst,
        }
    }
}

impl From<GqlNodeField> for Field {
    fn from(f: GqlNodeField) -> Self {
        match f {
            GqlNodeField::Id => Field::Id,
            GqlNodeField::Name => Field::Name,
            GqlNodeField::NodeType => Field::NodeType,
        }
    }
}

impl From<GqlViewOp> for ViewOp {
    fn from(op: GqlViewOp) -> Self {
        match op {
            GqlViewOp::Window(w) => ViewOp::Window {
                start: w.start.into_time(),
                end: w.end.into_time(),
            },
            GqlViewOp::At(t) => ViewOp::At(t.into_time()),
            GqlViewOp::After(t) => ViewOp::After(t.into_time()),
            GqlViewOp::Before(t) => ViewOp::Before(t.into_time()),
            GqlViewOp::Latest(_) => ViewOp::Latest,
            GqlViewOp::SnapshotAt(t) => ViewOp::SnapshotAt(t.into_time()),
            GqlViewOp::SnapshotLatest(_) => ViewOp::SnapshotLatest,
            GqlViewOp::Layers(names) => ViewOp::Layers(names),
        }
    }
}

fn scope(entity: GqlEntity, views: Option<Vec<GqlViewOp>>, endpoint: Option<GqlEndpoint>) -> Scope {
    Scope {
        entity: entity.into(),
        views: views
            .unwrap_or_default()
            .into_iter()
            .map(ViewOp::from)
            .collect(),
        endpoint: endpoint.map(Endpoint::from),
    }
}

impl From<GqlScope> for Scope {
    fn from(s: GqlScope) -> Self {
        scope(s.entity, s.views, s.endpoint)
    }
}

impl From<GqlTarget> for Target {
    fn from(t: GqlTarget) -> Self {
        match t {
            GqlTarget::Field(f) => Target::Field(f.into()),
            GqlTarget::Degree(d) => Target::Degree(d.into()),
            GqlTarget::Property(name) => Target::Property(name),
            GqlTarget::Metadata(name) => Target::Metadata(name),
        }
    }
}

fn prop(value: Value) -> Result<Prop, GraphError> {
    Prop::try_from(value).map_err(|e| invalid(format!("invalid constant: {e}")))
}

fn inner(expr: &Wrapped<GqlExpr>) -> Result<Box<tree::Expr>, GraphError> {
    Ok(Box::new(tree::Expr::try_from(expr.deref().clone())?))
}

impl TryFrom<GqlExpr> for tree::Expr {
    type Error = GraphError;

    fn try_from(expr: GqlExpr) -> Result<Self, Self::Error> {
        use tree::Expr as E;
        Ok(match expr {
            GqlExpr::Const(value) => E::Const(prop(value)?),
            GqlExpr::Read(read) => E::Read {
                scope: scope(read.entity, read.views, read.endpoint),
                target: read.target.into(),
            },
            GqlExpr::Temporal(e) => E::Temporal(inner(&e)?),
            GqlExpr::Sum(e) => E::Agg(Agg::Sum, inner(&e)?),
            GqlExpr::Avg(e) => E::Agg(Agg::Avg, inner(&e)?),
            GqlExpr::Min(e) => E::Agg(Agg::Min, inner(&e)?),
            GqlExpr::Max(e) => E::Agg(Agg::Max, inner(&e)?),
            GqlExpr::First(e) => E::Agg(Agg::First, inner(&e)?),
            GqlExpr::Last(e) => E::Agg(Agg::Last, inner(&e)?),
            GqlExpr::Len(e) => E::Agg(Agg::Len, inner(&e)?),
            GqlExpr::Any(e) => E::Qual(Qual::Any, inner(&e)?),
            GqlExpr::All(e) => E::Qual(Qual::All, inner(&e)?),
        })
    }
}

fn cmp(op: CmpOp, c: GqlCmp) -> Result<tree::FilterExpr, GraphError> {
    Ok(tree::FilterExpr::Cmp {
        op,
        lhs: c.lhs.try_into()?,
        rhs: c.rhs.try_into()?,
    })
}

fn str_op(op: StrOp, c: GqlCmp) -> Result<tree::FilterExpr, GraphError> {
    Ok(tree::FilterExpr::Str {
        op,
        lhs: c.lhs.try_into()?,
        rhs: c.rhs.try_into()?,
    })
}

fn membership(m: GqlMembership, negated: bool) -> Result<tree::FilterExpr, GraphError> {
    let op = if negated { "isNotIn" } else { "isIn" };
    let values = match m.values {
        Value::List(items) => items.into_iter().map(prop).collect::<Result<Vec<_>, _>>()?,
        other => return Err(invalid(format!("{op} requires a list value, got {other}"))),
    };
    Ok(tree::FilterExpr::In {
        expr: m.expr.try_into()?,
        values,
        negated,
    })
}

fn structural(s: GqlScope, pred: Structural) -> tree::FilterExpr {
    tree::FilterExpr::Structural {
        scope: s.into(),
        pred,
    }
}

impl TryFrom<GqlFilter> for tree::FilterExpr {
    type Error = GraphError;

    fn try_from(filter: GqlFilter) -> Result<Self, Self::Error> {
        use tree::FilterExpr as F;
        Ok(match filter {
            GqlFilter::Eq(c) => cmp(CmpOp::Eq, c)?,
            GqlFilter::Ne(c) => cmp(CmpOp::Ne, c)?,
            GqlFilter::Lt(c) => cmp(CmpOp::Lt, c)?,
            GqlFilter::Le(c) => cmp(CmpOp::Le, c)?,
            GqlFilter::Gt(c) => cmp(CmpOp::Gt, c)?,
            GqlFilter::Ge(c) => cmp(CmpOp::Ge, c)?,
            GqlFilter::StartsWith(c) => str_op(StrOp::StartsWith, c)?,
            GqlFilter::EndsWith(c) => str_op(StrOp::EndsWith, c)?,
            GqlFilter::Contains(c) => str_op(StrOp::Contains, c)?,
            GqlFilter::NotContains(c) => str_op(StrOp::NotContains, c)?,
            GqlFilter::FuzzySearch(f) => F::Str {
                op: StrOp::FuzzySearch {
                    levenshtein_distance: f.levenshtein_distance,
                    prefix_match: f.prefix_match,
                },
                lhs: f.lhs.try_into()?,
                rhs: f.rhs.try_into()?,
            },
            GqlFilter::IsSome(e) => F::IsSome(e.deref().clone().try_into()?),
            GqlFilter::IsNone(e) => F::IsNone(e.deref().clone().try_into()?),
            GqlFilter::IsIn(m) => membership(m, false)?,
            GqlFilter::IsNotIn(m) => membership(m, true)?,
            GqlFilter::IsActive(s) => structural(s, Structural::IsActive),
            GqlFilter::IsValid(s) => structural(s, Structural::IsValid),
            GqlFilter::IsDeleted(s) => structural(s, Structural::IsDeleted),
            GqlFilter::IsSelfLoop(s) => structural(s, Structural::IsSelfLoop),
            GqlFilter::View(ops) => F::View(ops.into_iter().map(ViewOp::from).collect()),
            GqlFilter::And(items) => F::And(
                items
                    .into_iter()
                    .map(F::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            GqlFilter::Or(items) => F::Or(
                items
                    .into_iter()
                    .map(F::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            GqlFilter::Not(inner) => F::Not(Box::new(inner.deref().clone().try_into()?)),
        })
    }
}

// ── tree → GraphQL ───────────────────────────────────────────────────────────
//
// Clients build trees and send them; this is the spelling they send.

impl From<Entity> for GqlEntity {
    fn from(e: Entity) -> Self {
        match e {
            Entity::Node => GqlEntity::Node,
            Entity::Edge => GqlEntity::Edge,
            Entity::ExplodedEdge => GqlEntity::ExplodedEdge,
        }
    }
}

impl From<Endpoint> for GqlEndpoint {
    fn from(e: Endpoint) -> Self {
        match e {
            Endpoint::Src => GqlEndpoint::Src,
            Endpoint::Dst => GqlEndpoint::Dst,
        }
    }
}

impl From<Field> for GqlNodeField {
    fn from(f: Field) -> Self {
        match f {
            Field::Id => GqlNodeField::Id,
            Field::Name => GqlNodeField::Name,
            Field::NodeType => GqlNodeField::NodeType,
        }
    }
}

fn time(t: EventTime) -> GqlTimeInput {
    GqlTimeInput(InputTime::Indexed(t.0, t.1))
}

impl From<&ViewOp> for GqlViewOp {
    fn from(op: &ViewOp) -> Self {
        match op {
            ViewOp::Window { start, end } => GqlViewOp::Window(Window {
                start: time(*start),
                end: time(*end),
            }),
            ViewOp::At(t) => GqlViewOp::At(time(*t)),
            ViewOp::After(t) => GqlViewOp::After(time(*t)),
            ViewOp::Before(t) => GqlViewOp::Before(time(*t)),
            ViewOp::Latest => GqlViewOp::Latest(true),
            ViewOp::SnapshotAt(t) => GqlViewOp::SnapshotAt(time(*t)),
            ViewOp::SnapshotLatest => GqlViewOp::SnapshotLatest(true),
            ViewOp::Layers(names) => GqlViewOp::Layers(names.clone()),
        }
    }
}

fn views(v: &[ViewOp]) -> Option<Vec<GqlViewOp>> {
    (!v.is_empty()).then(|| v.iter().map(GqlViewOp::from).collect())
}

impl From<&Scope> for GqlScope {
    fn from(s: &Scope) -> Self {
        GqlScope {
            entity: s.entity.into(),
            views: views(&s.views),
            endpoint: s.endpoint.map(GqlEndpoint::from),
        }
    }
}

fn direction(d: Direction) -> DegreeDirection {
    match d {
        Direction::IN => DegreeDirection::In,
        Direction::OUT => DegreeDirection::Out,
        Direction::BOTH => DegreeDirection::Both,
    }
}

impl From<&Target> for GqlTarget {
    fn from(t: &Target) -> Self {
        match t {
            Target::Field(f) => GqlTarget::Field((*f).into()),
            Target::Degree(d) => GqlTarget::Degree(direction(*d)),
            Target::Property(name) => GqlTarget::Property(name.clone()),
            Target::Metadata(name) => GqlTarget::Metadata(name.clone()),
        }
    }
}

fn value(p: &Prop) -> Result<Value, GraphError> {
    Value::try_from(p).map_err(|e| invalid(format!("constant has no wire form: {e}")))
}

fn wrapped(e: &tree::Expr) -> Result<Wrapped<GqlExpr>, GraphError> {
    Ok(Wrapped::from(GqlExpr::try_from(e)?))
}

impl TryFrom<&tree::Expr> for GqlExpr {
    type Error = GraphError;

    fn try_from(expr: &tree::Expr) -> Result<Self, Self::Error> {
        use tree::Expr as E;
        Ok(match expr {
            E::Const(p) => GqlExpr::Const(value(p)?),
            E::Read { scope, target } => GqlExpr::Read(GqlRead {
                entity: scope.entity.into(),
                views: views(&scope.views),
                endpoint: scope.endpoint.map(GqlEndpoint::from),
                target: target.into(),
            }),
            E::Temporal(e) => GqlExpr::Temporal(wrapped(e)?),
            E::Agg(Agg::Sum, e) => GqlExpr::Sum(wrapped(e)?),
            E::Agg(Agg::Avg, e) => GqlExpr::Avg(wrapped(e)?),
            E::Agg(Agg::Min, e) => GqlExpr::Min(wrapped(e)?),
            E::Agg(Agg::Max, e) => GqlExpr::Max(wrapped(e)?),
            E::Agg(Agg::First, e) => GqlExpr::First(wrapped(e)?),
            E::Agg(Agg::Last, e) => GqlExpr::Last(wrapped(e)?),
            E::Agg(Agg::Len, e) => GqlExpr::Len(wrapped(e)?),
            E::Qual(Qual::Any, e) => GqlExpr::Any(wrapped(e)?),
            E::Qual(Qual::All, e) => GqlExpr::All(wrapped(e)?),
        })
    }
}

fn gql_cmp(lhs: &tree::Expr, rhs: &tree::Expr) -> Result<GqlCmp, GraphError> {
    Ok(GqlCmp {
        lhs: lhs.try_into()?,
        rhs: rhs.try_into()?,
    })
}

impl TryFrom<&tree::FilterExpr> for GqlFilter {
    type Error = GraphError;

    fn try_from(filter: &tree::FilterExpr) -> Result<Self, Self::Error> {
        use tree::FilterExpr as F;
        Ok(match filter {
            F::Opaque(_) => return Err(invalid(OPAQUE_FILTER_ERROR)),
            F::Cmp { op, lhs, rhs } => {
                let c = gql_cmp(lhs, rhs)?;
                match op {
                    CmpOp::Eq => GqlFilter::Eq(c),
                    CmpOp::Ne => GqlFilter::Ne(c),
                    CmpOp::Lt => GqlFilter::Lt(c),
                    CmpOp::Le => GqlFilter::Le(c),
                    CmpOp::Gt => GqlFilter::Gt(c),
                    CmpOp::Ge => GqlFilter::Ge(c),
                }
            }
            F::Str { op, lhs, rhs } => match op {
                StrOp::StartsWith => GqlFilter::StartsWith(gql_cmp(lhs, rhs)?),
                StrOp::EndsWith => GqlFilter::EndsWith(gql_cmp(lhs, rhs)?),
                StrOp::Contains => GqlFilter::Contains(gql_cmp(lhs, rhs)?),
                StrOp::NotContains => GqlFilter::NotContains(gql_cmp(lhs, rhs)?),
                StrOp::FuzzySearch {
                    levenshtein_distance,
                    prefix_match,
                } => GqlFilter::FuzzySearch(GqlFuzzyCmp {
                    lhs: lhs.try_into()?,
                    rhs: rhs.try_into()?,
                    levenshtein_distance: *levenshtein_distance,
                    prefix_match: *prefix_match,
                }),
            },
            F::IsSome(e) => GqlFilter::IsSome(wrapped(e)?),
            F::IsNone(e) => GqlFilter::IsNone(wrapped(e)?),
            F::In {
                expr,
                values,
                negated,
            } => {
                let m = GqlMembership {
                    expr: expr.try_into()?,
                    values: Value::List(values.iter().map(value).collect::<Result<Vec<_>, _>>()?),
                };
                if *negated {
                    GqlFilter::IsNotIn(m)
                } else {
                    GqlFilter::IsIn(m)
                }
            }
            F::Structural { scope, pred } => {
                let s = GqlScope::from(scope);
                match pred {
                    Structural::IsActive => GqlFilter::IsActive(s),
                    Structural::IsValid => GqlFilter::IsValid(s),
                    Structural::IsDeleted => GqlFilter::IsDeleted(s),
                    Structural::IsSelfLoop => GqlFilter::IsSelfLoop(s),
                }
            }
            F::View(ops) => GqlFilter::View(ops.iter().map(GqlViewOp::from).collect()),
            F::And(items) => GqlFilter::And(
                items
                    .iter()
                    .map(GqlFilter::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            F::Or(items) => GqlFilter::Or(
                items
                    .iter()
                    .map(GqlFilter::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            F::Not(inner) => GqlFilter::Not(Wrapped::from(GqlFilter::try_from(inner.deref())?)),
        })
    }
}

impl TryFrom<FilterExpr> for GqlFilter {
    type Error = GraphError;

    fn try_from(tree: FilterExpr) -> Result<Self, Self::Error> {
        GqlFilter::try_from(&tree)
    }
}

/// The compiled filter, for callers that apply one filter to several handles.
impl TryFrom<GqlFilter> for DynFilter {
    type Error = GraphError;

    fn try_from(value: GqlFilter) -> Result<Self, Self::Error> {
        FilterExpr::try_from(value)?.compile()
    }
}

impl CreateFilter for GqlFilter {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph;

    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        FilterExpr::try_from(self)?.create_filter(graph, filtered)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        FilterExpr::try_from(self)?.create_node_filter(graph, filtered)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        FilterExpr::try_from(self.clone())?.filter_graph_view(graph)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(target: Target) -> tree::Expr {
        tree::Expr::Read {
            scope: Scope::new(Entity::Node),
            target,
        }
    }

    #[test]
    fn a_tree_survives_the_trip_through_the_wire_type_and_json() {
        let tree = tree::FilterExpr::And(vec![
            tree::FilterExpr::Cmp {
                op: CmpOp::Gt,
                lhs: tree::Expr::Agg(
                    Agg::Sum,
                    Box::new(tree::Expr::Temporal(Box::new(read(Target::Property(
                        "score".into(),
                    ))))),
                ),
                rhs: tree::Expr::Const(Prop::F64(10.0)),
            },
            tree::FilterExpr::Cmp {
                op: CmpOp::Gt,
                lhs: read(Target::Degree(Direction::BOTH)),
                rhs: read(Target::Degree(Direction::IN)),
            },
            tree::FilterExpr::Structural {
                scope: Scope::new(Entity::Edge)
                    .with_view(ViewOp::Window {
                        start: EventTime::from(0),
                        end: EventTime::from(5),
                    })
                    .with_view(ViewOp::Layers(vec!["knows".into()])),
                pred: Structural::IsActive,
            },
            tree::FilterExpr::Not(Box::new(tree::FilterExpr::In {
                expr: tree::Expr::Read {
                    scope: Scope::new(Entity::Edge).through(Endpoint::Src),
                    target: Target::Field(Field::Name),
                },
                values: vec![Prop::str("alice"), Prop::str("bob")],
                negated: false,
            })),
            tree::FilterExpr::View(vec![ViewOp::Latest]),
        ]);

        let wire = GqlFilter::try_from(&tree).unwrap();
        let json = serde_json::to_string(&wire).unwrap();
        let wire_back: GqlFilter = serde_json::from_str(&json).unwrap();
        let tree_back = tree::FilterExpr::try_from(wire_back).unwrap();
        assert_eq!(tree_back, tree);
    }

    #[test]
    fn the_json_spelling_is_the_documented_one() {
        let tree = tree::FilterExpr::Cmp {
            op: CmpOp::Gt,
            lhs: read(Target::Degree(Direction::BOTH)),
            rhs: read(Target::Degree(Direction::IN)),
        };
        let wire = GqlFilter::try_from(&tree).unwrap();
        assert_eq!(
            serde_json::to_value(&wire).unwrap(),
            serde_json::json!({
                "gt": {
                    "lhs": { "read": { "entity": "NODE", "target": { "degree": "BOTH" } } },
                    "rhs": { "read": { "entity": "NODE", "target": { "degree": "IN" } } }
                }
            })
        );
    }
}
