//! A filter as data.
//!
//! The typed expression API (`NodeFilter.property("score").gt(4)`) is what
//! rust callers write and what the engine compiles. Everything that has to
//! *carry* a filter — a python object, a GraphQL request, a stored permission
//! grant — needs the same filter as plain data instead. [`Expr`] and
//! [`FilterExpr`] are that data: one node per operation of the typed API, with
//! both sides of a comparison allowed to be expressions.
//!
//! A tree is built wherever the filter is written and compiled wherever it
//! runs, by [`FilterExpr::compile`], which replays the tree onto the typed
//! factories. Because the tree mirrors the API one to one, "what runs" and
//! "what is sent" cannot mean different things.

use crate::{
    db::{
        api::{
            state::NodeOp,
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                and_filter::AndFilter,
                dyn_factory::{DynEdgeFilterFactory, DynNodeFilterFactory},
                edge_filter::{EdgeEndpointNodeFilter, EdgeEndpointWrapper, EdgeFilter, Endpoint},
                exploded_edge_filter::ExplodedEdgeFilter,
                graph_filter::GraphFilter,
                layered_filter::layer_label,
                node_expr::{DynCreateOp, DynTemporal},
                node_filter::NodeFilter,
                not_filter::NotFilter,
                or_filter::OrFilter,
                DynCreateFilter, DynView, ViewWrapOps,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::{EntityAggOps, EntityExprFilterOps, Layer},
};
use raphtory_api::core::{
    entities::properties::prop::Prop,
    storage::timeindex::{AsTime, EventTime},
    Direction,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display},
    sync::Arc,
};

/// Which kind of entity an expression reads from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Entity {
    Node,
    Edge,
    ExplodedEdge,
}

/// One view restriction, in the order it was applied.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewOp {
    Window { start: EventTime, end: EventTime },
    At(EventTime),
    After(EventTime),
    Before(EventTime),
    Latest,
    SnapshotAt(EventTime),
    SnapshotLatest,
    Layers(Vec<String>),
}

/// A built-in node field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Field {
    Id,
    Name,
    NodeType,
}

/// A reduction over a list-valued expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Agg {
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    Len,
}

/// How the elements of a list-valued expression must satisfy a predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Qual {
    Any,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrOp {
    StartsWith,
    EndsWith,
    Contains,
    NotContains,
    FuzzySearch {
        levenshtein_distance: usize,
        prefix_match: bool,
    },
}

/// A predicate on the state of an entity rather than on a value it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Structural {
    IsActive,
    IsValid,
    IsDeleted,
    IsSelfLoop,
}

/// Where a value is read: which entity, through which views, and for an edge
/// optionally through one of its endpoint nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Scope {
    pub entity: Entity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub views: Vec<ViewOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<Endpoint>,
}

impl Scope {
    pub fn new(entity: Entity) -> Self {
        Scope {
            entity,
            views: Vec::new(),
            endpoint: None,
        }
    }

    pub fn with_view(mut self, op: ViewOp) -> Self {
        self.views.push(op);
        self
    }

    pub fn through(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = Some(endpoint);
        self
    }
}

/// What a leaf reads from its scope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Target {
    Field(Field),
    Degree(Direction),
    Property(String),
    Metadata(String),
}

/// A value: what stands on either side of a comparison.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expr {
    Const(Prop),
    Read { scope: Scope, target: Target },
    Temporal(Box<Expr>),
    Agg(Agg, Box<Expr>),
    Qual(Qual, Box<Expr>),
}

/// A yes/no: the filter itself.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterExpr {
    Cmp {
        op: CmpOp,
        lhs: Expr,
        rhs: Expr,
    },
    Str {
        op: StrOp,
        lhs: Expr,
        rhs: Expr,
    },
    IsSome(Expr),
    IsNone(Expr),
    In {
        expr: Expr,
        values: Vec<Prop>,
        negated: bool,
    },
    Structural {
        scope: Scope,
        pred: Structural,
    },
    /// A graph-level view with no predicate: the result *is* the view.
    View(Vec<ViewOp>),
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Not(Box<FilterExpr>),
    /// A filter over in-process state (a node-state column) that has no wire
    /// form: it runs where it was built and cannot be sent anywhere.
    Opaque(OpaqueFilter),
}

/// An already compiled filter carried inside a tree. It exists for filters
/// built from data that lives only in this process, so it runs but does not
/// serialise: asking for its wire form is an error, not a guess.
#[derive(Clone)]
pub struct OpaqueFilter(pub Arc<dyn DynCreateFilter>);

pub const OPAQUE_FILTER_ERROR: &str =
    "this filter has no server-side form; it was built from in-process state";

impl fmt::Debug for OpaqueFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpaqueFilter")
    }
}

impl PartialEq for OpaqueFilter {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Serialize for OpaqueFilter {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(OPAQUE_FILTER_ERROR))
    }
}

impl<'de> Deserialize<'de> for OpaqueFilter {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(OPAQUE_FILTER_ERROR))
    }
}

impl FilterExpr {
    /// Whether any part of this filter tests edges. An edge test says nothing
    /// about which nodes belong in a node collection, so a node-collection
    /// subscript refuses such a filter.
    pub fn tests_edges(&self) -> bool {
        let edge_scope = |scope: &Scope| scope.entity != Entity::Node;
        fn expr_tests_edges(expr: &Expr, edge_scope: &dyn Fn(&Scope) -> bool) -> bool {
            match expr {
                Expr::Const(_) => false,
                Expr::Read { scope, .. } => edge_scope(scope),
                Expr::Temporal(e) | Expr::Agg(_, e) | Expr::Qual(_, e) => {
                    expr_tests_edges(e, edge_scope)
                }
            }
        }
        match self {
            FilterExpr::Cmp { lhs, rhs, .. } | FilterExpr::Str { lhs, rhs, .. } => {
                expr_tests_edges(lhs, &edge_scope) || expr_tests_edges(rhs, &edge_scope)
            }
            FilterExpr::IsSome(e) | FilterExpr::IsNone(e) | FilterExpr::In { expr: e, .. } => {
                expr_tests_edges(e, &edge_scope)
            }
            FilterExpr::Structural { scope, .. } => edge_scope(scope),
            FilterExpr::View(_) | FilterExpr::Opaque(_) => false,
            FilterExpr::And(items) | FilterExpr::Or(items) => items.iter().any(Self::tests_edges),
            FilterExpr::Not(inner) => inner.tests_edges(),
        }
    }
}

// ── compiling ────────────────────────────────────────────────────────────────

/// A partly compiled value. Property reads keep the ability to switch to
/// their history until an aggregate or qualifier is applied.
enum Compiled {
    Op(Arc<dyn DynCreateOp>),
    Property(Arc<dyn DynTemporal>),
}

impl Compiled {
    fn op(self) -> Arc<dyn DynCreateOp> {
        match self {
            Compiled::Op(op) => op,
            Compiled::Property(prop) => prop,
        }
    }

    /// An endpoint read is a node read the edge evaluates on the node at that
    /// end. The wrapping happens here, at the read, so that qualifiers and
    /// aggregates applied above it see an edge expression and are compiled the
    /// way an edge filter compiles them.
    fn through(self, endpoint: Endpoint) -> Self {
        match self {
            Compiled::Op(op) => Compiled::Op(Arc::new(EdgeEndpointWrapper::new(op, endpoint))),
            Compiled::Property(prop) => {
                Compiled::Property(Arc::new(EdgeEndpointWrapper::new(prop, endpoint)))
            }
        }
    }

    fn map_op(self, f: impl FnOnce(Arc<dyn DynCreateOp>) -> Arc<dyn DynCreateOp>) -> Self {
        Compiled::Op(f(self.op()))
    }
}

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidFilter(msg.into())
}

fn node_factory(views: &[ViewOp]) -> Arc<dyn DynNodeFilterFactory> {
    let mut f: Arc<dyn DynNodeFilterFactory> = Arc::new(NodeFilter);
    for op in views {
        f = match op {
            ViewOp::Window { start, end } => f.window(*start, *end),
            ViewOp::At(t) => f.at(*t),
            ViewOp::After(t) => f.after(*t),
            ViewOp::Before(t) => f.before(*t),
            ViewOp::Latest => Arc::new(f.latest()),
            ViewOp::SnapshotAt(t) => Arc::new(f.snapshot_at(*t)),
            ViewOp::SnapshotLatest => Arc::new(f.snapshot_latest()),
            ViewOp::Layers(names) => Arc::new(f.layer(names.clone())),
        };
    }
    f
}

fn edge_factory(entity: Entity, views: &[ViewOp]) -> Arc<dyn DynEdgeFilterFactory> {
    let mut f: Arc<dyn DynEdgeFilterFactory> = match entity {
        Entity::ExplodedEdge => Arc::new(ExplodedEdgeFilter),
        _ => Arc::new(EdgeFilter),
    };
    for op in views {
        f = match op {
            ViewOp::Window { start, end } => f.dyn_window(*start, *end),
            ViewOp::At(t) => f.dyn_at(*t),
            ViewOp::After(t) => f.dyn_after(*t),
            ViewOp::Before(t) => f.dyn_before(*t),
            ViewOp::Latest => f.dyn_latest(),
            ViewOp::SnapshotAt(t) => f.dyn_snapshot_at(*t),
            ViewOp::SnapshotLatest => f.dyn_snapshot_latest(),
            ViewOp::Layers(names) => f.dyn_layer(names.clone()),
        };
    }
    f
}

/// The graph-level view a chain of view ops describes, applied in order.
pub fn compile_view(views: &[ViewOp]) -> DynView {
    let mut v: DynView = Arc::new(GraphFilter);
    for op in views {
        v = match op {
            ViewOp::Window { start, end } => v.window(*start, *end),
            ViewOp::At(t) => v.at(*t),
            ViewOp::After(t) => v.after(*t),
            ViewOp::Before(t) => v.before(*t),
            ViewOp::Latest => Arc::new(v.latest()),
            ViewOp::SnapshotAt(t) => Arc::new(v.snapshot_at(*t)),
            ViewOp::SnapshotLatest => Arc::new(v.snapshot_latest()),
            ViewOp::Layers(names) => Arc::new(v.layer(Layer::from(names.clone()))),
        };
    }
    v
}

fn read_node(f: &Arc<dyn DynNodeFilterFactory>, target: &Target) -> Compiled {
    match target {
        Target::Field(Field::Id) => Compiled::Op(f.dyn_id()),
        Target::Field(Field::Name) => Compiled::Op(f.dyn_name()),
        Target::Field(Field::NodeType) => Compiled::Op(f.dyn_node_type()),
        Target::Degree(Direction::BOTH) => Compiled::Op(f.dyn_degree()),
        Target::Degree(Direction::IN) => Compiled::Op(f.dyn_in_degree()),
        Target::Degree(Direction::OUT) => Compiled::Op(f.dyn_out_degree()),
        Target::Property(name) => Compiled::Property(f.dyn_property(name.clone())),
        Target::Metadata(name) => Compiled::Op(f.dyn_metadata(name.clone())),
    }
}

impl Expr {
    /// The erased, compilable form of this value.
    pub fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        Ok(self.compile_inner()?.op())
    }

    fn compile_inner(&self) -> Result<Compiled, GraphError> {
        match self {
            Expr::Const(value) => Ok(Compiled::Op(Arc::new(value.clone()))),
            Expr::Read { scope, target } => match (scope.entity, scope.endpoint) {
                (Entity::Node, Some(_)) => {
                    Err(invalid("a node expression has no src()/dst() endpoint"))
                }
                (Entity::Node, None) => Ok(read_node(&node_factory(&scope.views), target)),
                // An endpoint read is a node read, scoped by the same views,
                // that the edge evaluates on the node at that end.
                (Entity::Edge | Entity::ExplodedEdge, Some(endpoint)) => {
                    Ok(read_node(&node_factory(&scope.views), target).through(endpoint))
                }
                (entity, None) => {
                    let f = edge_factory(entity, &scope.views);
                    Ok(match target {
                        Target::Property(name) => Compiled::Property(f.dyn_property(name.clone())),
                        Target::Metadata(name) => Compiled::Op(f.dyn_metadata(name.clone())),
                        Target::Field(_) | Target::Degree(_) => {
                            return Err(invalid(
                                "an edge has no fields or degree; read them through src() or dst()",
                            ))
                        }
                    })
                }
            },
            Expr::Temporal(inner) => match inner.compile_inner()? {
                Compiled::Property(prop) => Ok(Compiled::Op(prop.temporal())),
                Compiled::Op(_) => Err(invalid("temporal() applies to a property")),
            },
            Expr::Agg(agg, inner) => Ok(inner.compile_inner()?.map_op(|op| match agg {
                Agg::Sum => Arc::new(op.sum()),
                Agg::Avg => Arc::new(op.avg()),
                Agg::Min => Arc::new(op.min()),
                Agg::Max => Arc::new(op.max()),
                Agg::First => Arc::new(op.first()),
                Agg::Last => Arc::new(op.last()),
                Agg::Len => Arc::new(op.len()),
            })),
            Expr::Qual(qual, inner) => Ok(inner.compile_inner()?.map_op(|op| match qual {
                Qual::Any => Arc::new(op.any()),
                Qual::All => Arc::new(op.all()),
            })),
        }
    }
}

/// Compile a comparison whose right-hand side is either a constant or an
/// expression; both go through the same typed method, a constant being the
/// expression of its own value.
macro_rules! binary {
    ($lhs:expr, $rhs:expr, $method:ident $(, $arg:expr)*) => {{
        let lhs = $lhs.compile()?;
        let rhs: Arc<dyn DynCreateOp> = $rhs.compile()?;
        let filter: Arc<dyn DynCreateFilter> = Arc::new(lhs.$method(rhs $(, $arg)*));
        filter
    }};
}

impl FilterExpr {
    /// The erased, applicable form of this filter.
    pub fn compile(&self) -> Result<Arc<dyn DynCreateFilter>, GraphError> {
        Ok(match self {
            FilterExpr::Cmp { op, lhs, rhs } => match op {
                CmpOp::Eq => binary!(lhs, rhs, eq),
                CmpOp::Ne => binary!(lhs, rhs, ne),
                CmpOp::Lt => binary!(lhs, rhs, lt),
                CmpOp::Le => binary!(lhs, rhs, le),
                CmpOp::Gt => binary!(lhs, rhs, gt),
                CmpOp::Ge => binary!(lhs, rhs, ge),
            },
            FilterExpr::Str { op, lhs, rhs } => match op {
                StrOp::StartsWith => binary!(lhs, rhs, starts_with),
                StrOp::EndsWith => binary!(lhs, rhs, ends_with),
                StrOp::Contains => binary!(lhs, rhs, contains),
                StrOp::NotContains => binary!(lhs, rhs, not_contains),
                StrOp::FuzzySearch {
                    levenshtein_distance,
                    prefix_match,
                } => binary!(lhs, rhs, fuzzy_search, *levenshtein_distance, *prefix_match),
            },
            FilterExpr::IsSome(expr) => Arc::new(expr.compile()?.is_some()),
            FilterExpr::IsNone(expr) => Arc::new(expr.compile()?.is_none()),
            FilterExpr::In {
                expr,
                values,
                negated,
            } => {
                let lhs = expr.compile()?;
                if *negated {
                    Arc::new(lhs.is_not_in(values.clone()))
                } else {
                    Arc::new(lhs.is_in(values.clone()))
                }
            }
            FilterExpr::Structural { scope, pred } => {
                // Through an endpoint, the predicate is a node predicate
                // evaluated on the node at that end of the edge.
                if let Some(endpoint) = scope.endpoint {
                    if scope.entity == Entity::Node {
                        return Err(invalid("a node has no src()/dst() endpoint"));
                    }
                    if *pred != Structural::IsActive {
                        return Err(invalid(format!("{pred} is an edge predicate")));
                    }
                    return Ok(Arc::new(EdgeEndpointNodeFilter {
                        endpoint,
                        inner: node_factory(&scope.views).dyn_is_active(),
                    }));
                }
                match (scope.entity, pred) {
                    (Entity::Node, Structural::IsActive) => {
                        node_factory(&scope.views).dyn_is_active()
                    }
                    (Entity::Node, other) => {
                        return Err(invalid(format!("{other} is an edge predicate")))
                    }
                    (entity, pred) => {
                        let f = edge_factory(entity, &scope.views);
                        match pred {
                            Structural::IsActive => f.dyn_is_active(),
                            Structural::IsValid => f.dyn_is_valid(),
                            Structural::IsDeleted => f.dyn_is_deleted(),
                            Structural::IsSelfLoop => f.dyn_is_self_loop(),
                        }
                    }
                }
            }
            FilterExpr::View(views) => {
                if views.is_empty() {
                    return Err(invalid("a view filter needs at least one view"));
                }
                compile_view(views)
            }
            FilterExpr::And(items) => combine(items, "and", |left, right| {
                Arc::new(AndFilter { left, right })
            })?,
            FilterExpr::Or(items) => combine(items, "or", |left, right| {
                Arc::new(OrFilter { left, right })
            })?,
            FilterExpr::Not(inner) => Arc::new(NotFilter(inner.compile()?)),
            FilterExpr::Opaque(filter) => filter.0.clone(),
        })
    }
}

/// Fold a list of operands pairwise, left to right. An empty list has no
/// meaning either way (`and` of nothing is not "everything", `or` of nothing
/// is not "nothing" the caller asked for), so it is refused.
fn combine(
    items: &[FilterExpr],
    name: &str,
    join: impl Fn(Arc<dyn DynCreateFilter>, Arc<dyn DynCreateFilter>) -> Arc<dyn DynCreateFilter>,
) -> Result<Arc<dyn DynCreateFilter>, GraphError> {
    let mut compiled = items.iter().map(FilterExpr::compile);
    let first = compiled
        .next()
        .ok_or_else(|| invalid(format!("`{name}` needs at least one operand")))??;
    compiled.try_fold(first, |acc, next| Ok(join(acc, next?)))
}

/// A tree is a filter in its own right: applying it compiles it first.
impl CreateFilter for FilterExpr {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

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
        self.compile()?.create_filter(graph, filtered)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        self.compile()?.create_node_filter(graph, filtered)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.compile()?.filter_graph_view(graph)
    }
}

// ── printing ─────────────────────────────────────────────────────────────────

impl Display for ViewOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViewOp::Window { start, end } => write!(f, "WINDOW[{}..{}]", start.t(), end.t()),
            ViewOp::At(t) => write!(f, "AT[{}]", t.t()),
            ViewOp::After(t) => write!(f, "AFTER[{}]", t.t()),
            ViewOp::Before(t) => write!(f, "BEFORE[{}]", t.t()),
            ViewOp::Latest => write!(f, "LATEST"),
            ViewOp::SnapshotAt(t) => write!(f, "SNAPSHOT_AT[{}]", t.t()),
            ViewOp::SnapshotLatest => write!(f, "SNAPSHOT_LATEST"),
            ViewOp::Layers(names) => {
                write!(f, "LAYER[{}]", layer_label(&Layer::from(names.clone())))
            }
        }
    }
}

impl Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Target::Field(Field::Id) => write!(f, "id"),
            Target::Field(Field::Name) => write!(f, "name"),
            Target::Field(Field::NodeType) => write!(f, "node_type"),
            Target::Degree(Direction::BOTH) => write!(f, "degree"),
            Target::Degree(Direction::IN) => write!(f, "in_degree"),
            Target::Degree(Direction::OUT) => write!(f, "out_degree"),
            Target::Property(name) => write!(f, "{name}"),
            Target::Metadata(name) => write!(f, "metadata({name})"),
        }
    }
}

/// Wraps `inner` in the scope's views (innermost first) and endpoint.
fn scoped(f: &mut fmt::Formatter<'_>, scope: &Scope, inner: &str) -> fmt::Result {
    let mut text = inner.to_string();
    for view in &scope.views {
        text = format!("{view}({text})");
    }
    match scope.endpoint {
        Some(Endpoint::Src) => write!(f, "SRC({text})"),
        Some(Endpoint::Dst) => write!(f, "DST({text})"),
        None => write!(f, "{text}"),
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Const(value) => write!(f, "{value}"),
            Expr::Read { scope, target } => scoped(f, scope, &target.to_string()),
            Expr::Temporal(inner) => write!(f, "temporal({inner})"),
            Expr::Agg(agg, inner) => write!(f, "{agg}({inner})"),
            Expr::Qual(qual, inner) => write!(f, "{qual}({inner})"),
        }
    }
}

impl Display for Agg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Agg::Sum => "sum",
            Agg::Avg => "avg",
            Agg::Min => "min",
            Agg::Max => "max",
            Agg::First => "first",
            Agg::Last => "last",
            Agg::Len => "len",
        })
    }
}

impl Display for Qual {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Qual::Any => "any",
            Qual::All => "all",
        })
    }
}

impl Display for CmpOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CmpOp::Eq => "==",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        })
    }
}

impl Display for StrOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StrOp::StartsWith => write!(f, "STARTS_WITH"),
            StrOp::EndsWith => write!(f, "ENDS_WITH"),
            StrOp::Contains => write!(f, "CONTAINS"),
            StrOp::NotContains => write!(f, "NOT_CONTAINS"),
            StrOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            } => write!(f, "FUZZY_SEARCH[{levenshtein_distance}, {prefix_match}]"),
        }
    }
}

impl Display for Structural {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Structural::IsActive => "IS_ACTIVE",
            Structural::IsValid => "IS_VALID",
            Structural::IsDeleted => "IS_DELETED",
            Structural::IsSelfLoop => "IS_SELF_LOOP",
        })
    }
}

impl Display for FilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let list = |items: &[FilterExpr], sep: &str| {
            items
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(sep)
        };
        match self {
            FilterExpr::Cmp { op, lhs, rhs } => write!(f, "{lhs} {op} {rhs}"),
            FilterExpr::Str { op, lhs, rhs } => write!(f, "{lhs} {op} {rhs}"),
            FilterExpr::IsSome(expr) => write!(f, "{expr} IS_SOME"),
            FilterExpr::IsNone(expr) => write!(f, "{expr} IS_NONE"),
            FilterExpr::In {
                expr,
                values,
                negated,
            } => {
                let values = values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let op = if *negated { "NOT_IN" } else { "IN" };
                write!(f, "{expr} {op} [{values}]")
            }
            FilterExpr::Structural { scope, pred } => scoped(f, scope, &pred.to_string()),
            FilterExpr::View(views) => {
                let views = views.iter().map(|v| v.to_string()).collect::<Vec<_>>();
                write!(f, "VIEW({})", views.join(" . "))
            }
            FilterExpr::And(items) => write!(f, "({})", list(items, " AND ")),
            FilterExpr::Or(items) => write!(f, "({})", list(items, " OR ")),
            FilterExpr::Not(inner) => write!(f, "NOT({inner})"),
            FilterExpr::Opaque(_) => write!(f, "<local-only filter>"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            api::view::Filter,
            graph::views::filter::model::{
                edge_filter::EdgeFilter, node_filter::NodeFilter, PropertyExprFactory, ViewWrapOps,
            },
        },
        prelude::{AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS},
    };
    use raphtory_api::core::entities::properties::prop::IntoProp;

    /// alice.score 3@0 7@2 9@6 · bob.score 5@1 2@7 · carol none · dave.score 1@2 1@3
    /// alice→bob [knows] @1 @4 · bob→carol [works] @2 · carol→dave [knows] @6
    fn graph() -> Graph {
        let g = Graph::new();
        for (t, name, score) in [
            (0, "alice", 3.0),
            (2, "alice", 7.0),
            (6, "alice", 9.0),
            (1, "bob", 5.0),
            (7, "bob", 2.0),
            (2, "dave", 1.0),
            (3, "dave", 1.0),
        ] {
            g.add_node(t, name, [("score", score.into_prop())], None, None)
                .unwrap();
        }
        g.add_node(0, "carol", NO_PROPS, None, None).unwrap();
        g.add_edge(1, "alice", "bob", NO_PROPS, Some("knows"))
            .unwrap();
        g.add_edge(4, "alice", "bob", NO_PROPS, Some("knows"))
            .unwrap();
        g.add_edge(2, "bob", "carol", NO_PROPS, Some("works"))
            .unwrap();
        g.add_edge(6, "carol", "dave", NO_PROPS, Some("knows"))
            .unwrap();
        g
    }

    fn nodes(g: &Graph, filter: Arc<dyn DynCreateFilter>) -> Vec<String> {
        let mut names: Vec<String> = g
            .filter(filter)
            .unwrap()
            .nodes()
            .iter()
            .map(|n| n.name())
            .collect();
        names.sort();
        names
    }

    fn edges(g: &Graph, filter: Arc<dyn DynCreateFilter>) -> Vec<String> {
        let mut ids: Vec<String> = g
            .filter(filter)
            .unwrap()
            .edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect();
        ids.sort();
        ids
    }

    fn node(target: Target) -> Expr {
        Expr::Read {
            scope: Scope::new(Entity::Node),
            target,
        }
    }

    fn score() -> Expr {
        node(Target::Property("score".into()))
    }

    fn cmp(op: CmpOp, lhs: Expr, rhs: Expr) -> FilterExpr {
        FilterExpr::Cmp { op, lhs, rhs }
    }

    fn c(v: impl Into<Prop>) -> Expr {
        Expr::Const(v.into())
    }

    #[test]
    fn a_constant_comparison_matches_the_typed_api() {
        let g = graph();
        let tree = cmp(CmpOp::Gt, score(), c(4.0));
        assert_eq!(nodes(&g, tree.compile().unwrap()), vec!["alice"]);
        let typed: Arc<dyn DynCreateFilter> = Arc::new(NodeFilter.property("score").gt(4.0));
        assert_eq!(nodes(&g, typed), vec!["alice"]);
        assert_eq!(tree.to_string(), "score > 4");
    }

    #[test]
    fn views_scope_the_read_not_the_result() {
        let g = graph();
        let tree = cmp(
            CmpOp::Gt,
            Expr::Read {
                scope: Scope::new(Entity::Node).with_view(ViewOp::Window {
                    start: EventTime::from(0),
                    end: EventTime::from(5),
                }),
                target: Target::Property("score".into()),
            },
            c(4.0),
        );
        // inside [0,5): alice's latest score is 7, bob's is 5
        assert_eq!(nodes(&g, tree.compile().unwrap()), vec!["alice", "bob"]);
        let typed: Arc<dyn DynCreateFilter> =
            Arc::new(NodeFilter.window(0, 5).property("score").gt(4.0));
        assert_eq!(nodes(&g, typed), vec!["alice", "bob"]);
        assert_eq!(tree.to_string(), "WINDOW[0..5](score) > 4");
    }

    #[test]
    fn both_sides_may_be_expressions() {
        let g = graph();
        let tree = cmp(
            CmpOp::Gt,
            node(Target::Degree(Direction::BOTH)),
            node(Target::Degree(Direction::IN)),
        );
        assert_eq!(
            nodes(&g, tree.compile().unwrap()),
            vec!["alice", "bob", "carol"]
        );
        assert_eq!(tree.to_string(), "degree > in_degree");
    }

    #[test]
    fn temporal_aggregates_and_qualifiers() {
        let g = graph();
        let sum = cmp(
            CmpOp::Gt,
            Expr::Agg(Agg::Sum, Box::new(Expr::Temporal(Box::new(score())))),
            c(10.0),
        );
        assert_eq!(nodes(&g, sum.compile().unwrap()), vec!["alice"]);
        assert_eq!(sum.to_string(), "sum(temporal(score)) > 10");

        let any = cmp(
            CmpOp::Gt,
            Expr::Qual(Qual::Any, Box::new(Expr::Temporal(Box::new(score())))),
            c(4.0),
        );
        assert_eq!(nodes(&g, any.compile().unwrap()), vec!["alice", "bob"]);

        let all = cmp(
            CmpOp::Gt,
            Expr::Qual(Qual::All, Box::new(Expr::Temporal(Box::new(score())))),
            c(4.0),
        );
        assert!(nodes(&g, all.compile().unwrap()).is_empty());

        let len = cmp(
            CmpOp::Eq,
            Expr::Agg(Agg::Len, Box::new(Expr::Temporal(Box::new(score())))),
            c(2u64),
        );
        assert_eq!(nodes(&g, len.compile().unwrap()), vec!["bob", "dave"]);
    }

    #[test]
    fn temporal_needs_a_property() {
        let tree = Expr::Temporal(Box::new(node(Target::Degree(Direction::BOTH))));
        assert!(tree.compile().is_err());
    }

    #[test]
    fn combinators_and_presence() {
        let g = graph();
        let tree = FilterExpr::And(vec![
            FilterExpr::IsSome(score()),
            FilterExpr::Not(Box::new(FilterExpr::Str {
                op: StrOp::StartsWith,
                lhs: node(Target::Field(Field::Name)),
                rhs: c("a"),
            })),
        ]);
        assert_eq!(nodes(&g, tree.compile().unwrap()), vec!["bob", "dave"]);
        assert_eq!(
            tree.to_string(),
            "(score IS_SOME AND NOT(name STARTS_WITH a))"
        );

        let none = FilterExpr::IsNone(score());
        assert_eq!(nodes(&g, none.compile().unwrap()), vec!["carol"]);

        let set = FilterExpr::In {
            expr: node(Target::Field(Field::Name)),
            values: vec!["alice".into(), "dave".into()],
            negated: false,
        };
        assert_eq!(nodes(&g, set.compile().unwrap()), vec!["alice", "dave"]);
        assert!(FilterExpr::And(vec![]).compile().is_err());
        assert!(FilterExpr::Or(vec![]).compile().is_err());
    }

    #[test]
    fn edge_endpoints_and_structure() {
        let g = graph();
        let src_name = FilterExpr::Cmp {
            op: CmpOp::Eq,
            lhs: Expr::Read {
                scope: Scope::new(Entity::Edge).through(Endpoint::Src),
                target: Target::Field(Field::Name),
            },
            rhs: c("alice"),
        };
        assert_eq!(edges(&g, src_name.compile().unwrap()), vec!["alice->bob"]);
        let typed: Arc<dyn DynCreateFilter> = Arc::new(EdgeFilter::src().name().eq("alice"));
        assert_eq!(edges(&g, typed), vec!["alice->bob"]);
        assert_eq!(src_name.to_string(), "SRC(name) == alice");

        let endpoint_score = FilterExpr::Cmp {
            op: CmpOp::Gt,
            lhs: Expr::Read {
                scope: Scope::new(Entity::Edge).through(Endpoint::Dst),
                target: Target::Property("score".into()),
            },
            rhs: Expr::Read {
                scope: Scope::new(Entity::Edge).through(Endpoint::Src),
                target: Target::Property("score".into()),
            },
        };
        // dst score > src score: alice(9)->bob(2) no; bob(2)->carol(none) no; carol(none)->dave no
        assert!(edges(&g, endpoint_score.compile().unwrap()).is_empty());

        let layered = FilterExpr::Structural {
            scope: Scope::new(Entity::Edge).with_view(ViewOp::Layers(vec!["works".into()])),
            pred: Structural::IsActive,
        };
        assert_eq!(edges(&g, layered.compile().unwrap()), vec!["bob->carol"]);
        assert_eq!(layered.to_string(), "LAYER[works](IS_ACTIVE)");
    }

    #[test]
    fn a_view_alone_is_the_result() {
        let g = graph();
        let tree = FilterExpr::View(vec![
            ViewOp::Window {
                start: EventTime::from(0),
                end: EventTime::from(5),
            },
            ViewOp::Latest,
        ]);
        assert_eq!(tree.to_string(), "VIEW(WINDOW[0..5] . LATEST)");
        assert_eq!(edges(&g, tree.compile().unwrap()), vec!["alice->bob"]);
        assert!(FilterExpr::View(vec![]).compile().is_err());
    }

    #[test]
    fn trees_round_trip_through_json() {
        let tree = FilterExpr::And(vec![
            cmp(
                CmpOp::Gt,
                Expr::Agg(Agg::Sum, Box::new(Expr::Temporal(Box::new(score())))),
                c(10.0),
            ),
            FilterExpr::Structural {
                scope: Scope::new(Entity::Edge).with_view(ViewOp::Layers(vec!["works".into()])),
                pred: Structural::IsActive,
            },
            FilterExpr::View(vec![ViewOp::Latest]),
        ]);
        let json = serde_json::to_string(&tree).unwrap();
        let back: FilterExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(back, tree);
    }
}
