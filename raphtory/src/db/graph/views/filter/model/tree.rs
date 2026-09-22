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
                edge_filter::Endpoint,
                expr::{
                    EdgeLeaf, ExplodedEdgeLeaf, Expr as SplitExpr, FilterExpr as SplitFilter, Leaf,
                    NodeLeaf,
                },
                layered_filter::layer_label,
                node_expr::DynCreateOp,
                DynCreateFilter,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::Layer,
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
    /// Whether a view appears anywhere in this filter.
    pub fn has_view(&self) -> bool {
        match self {
            FilterExpr::View(_) => true,
            FilterExpr::And(items) | FilterExpr::Or(items) => items.iter().any(Self::has_view),
            FilterExpr::Not(inner) => inner.has_view(),
            _ => false,
        }
    }

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

// ── compiling: through the entity-split tree ────────────────────────────────
//
// This tree predates the per-entity one in `model::expr`; python and GraphQL
// still speak it. It compiles by converting to that tree, so there is one
// compiler and one set of checks. A qualifier written on a value here
// (`x.any() == v`) becomes the qualifier on the comparison there
// (`(x == v).any()`).

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidFilter(msg.into())
}

/// A leaf of the per-entity tree built from this tree's read.
trait FromOldRead: Leaf {
    fn from_read(scope: &Scope, target: &Target) -> Result<Self, GraphError>;
    fn structural(scope: &Scope, pred: Structural) -> Result<Self, GraphError>;
    /// Switch a property read to its history.
    fn temporal(self) -> Result<Self, GraphError>;
}

fn not_a_property() -> GraphError {
    invalid("temporal() applies to a property")
}

impl FromOldRead for NodeLeaf {
    fn from_read(scope: &Scope, target: &Target) -> Result<Self, GraphError> {
        if scope.endpoint.is_some() {
            return Err(invalid("a node expression has no src()/dst() endpoint"));
        }
        let views = scope.views.clone();
        Ok(match target {
            Target::Field(field) => NodeLeaf::Field {
                views,
                field: *field,
            },
            Target::Degree(direction) => NodeLeaf::Degree {
                views,
                direction: *direction,
            },
            Target::Property(name) => NodeLeaf::property(views, name.clone(), false),
            Target::Metadata(name) => NodeLeaf::metadata(views, name.clone()),
        })
    }

    fn structural(scope: &Scope, pred: Structural) -> Result<Self, GraphError> {
        if scope.endpoint.is_some() {
            return Err(invalid("a node has no src()/dst() endpoint"));
        }
        match pred {
            Structural::IsActive => Ok(NodeLeaf::is_active(scope.views.clone())),
            other => Err(invalid(format!("{other} is an edge predicate"))),
        }
    }

    fn temporal(self) -> Result<Self, GraphError> {
        match self {
            NodeLeaf::Property {
                views,
                name,
                temporal: false,
            } => Ok(NodeLeaf::Property {
                views,
                name,
                temporal: true,
            }),
            _ => Err(not_a_property()),
        }
    }
}

impl FromOldRead for EdgeLeaf {
    fn from_read(scope: &Scope, target: &Target) -> Result<Self, GraphError> {
        // An endpoint read is a node read, scoped by the same views, that the
        // edge evaluates on the node at that end.
        if let Some(endpoint) = scope.endpoint {
            let node_scope = Scope {
                entity: Entity::Node,
                views: scope.views.clone(),
                endpoint: None,
            };
            let inner = Box::new(SplitExpr::Read(NodeLeaf::from_read(&node_scope, target)?));
            return Ok(match endpoint {
                Endpoint::Src => EdgeLeaf::Src(inner),
                Endpoint::Dst => EdgeLeaf::Dst(inner),
            });
        }
        let views = scope.views.clone();
        match target {
            Target::Property(name) => Ok(EdgeLeaf::property(views, name.clone(), false)),
            Target::Metadata(name) => Ok(EdgeLeaf::metadata(views, name.clone())),
            Target::Field(_) | Target::Degree(_) => Err(invalid(
                "an edge has no fields or degree; read them through src() or dst()",
            )),
        }
    }

    fn structural(scope: &Scope, pred: Structural) -> Result<Self, GraphError> {
        if let Some(endpoint) = scope.endpoint {
            if pred != Structural::IsActive {
                return Err(invalid(format!("{pred} is an edge predicate")));
            }
            let inner = Box::new(SplitExpr::Read(NodeLeaf::is_active(scope.views.clone())));
            return Ok(match endpoint {
                Endpoint::Src => EdgeLeaf::Src(inner),
                Endpoint::Dst => EdgeLeaf::Dst(inner),
            });
        }
        let views = scope.views.clone();
        Ok(match pred {
            Structural::IsActive => EdgeLeaf::IsActive { views },
            Structural::IsValid => EdgeLeaf::IsValid { views },
            Structural::IsDeleted => EdgeLeaf::IsDeleted { views },
            Structural::IsSelfLoop => EdgeLeaf::IsSelfLoop { views },
        })
    }

    fn temporal(self) -> Result<Self, GraphError> {
        match self {
            EdgeLeaf::Property {
                views,
                name,
                temporal: false,
            } => Ok(EdgeLeaf::Property {
                views,
                name,
                temporal: true,
            }),
            EdgeLeaf::Src(inner) => Ok(EdgeLeaf::Src(Box::new(temporal_read(*inner)?))),
            EdgeLeaf::Dst(inner) => Ok(EdgeLeaf::Dst(Box::new(temporal_read(*inner)?))),
            _ => Err(not_a_property()),
        }
    }
}

impl FromOldRead for ExplodedEdgeLeaf {
    fn from_read(scope: &Scope, target: &Target) -> Result<Self, GraphError> {
        if scope.endpoint.is_some() {
            return Err(invalid("an exploded edge has no src()/dst() endpoint"));
        }
        let views = scope.views.clone();
        match target {
            Target::Property(name) => Ok(ExplodedEdgeLeaf::property(views, name.clone(), false)),
            Target::Metadata(name) => Ok(ExplodedEdgeLeaf::metadata(views, name.clone())),
            Target::Field(_) | Target::Degree(_) => Err(invalid(
                "an edge has no fields or degree; read them through src() or dst()",
            )),
        }
    }

    fn structural(scope: &Scope, pred: Structural) -> Result<Self, GraphError> {
        if scope.endpoint.is_some() {
            return Err(invalid("an exploded edge has no src()/dst() endpoint"));
        }
        let views = scope.views.clone();
        Ok(match pred {
            Structural::IsActive => ExplodedEdgeLeaf::IsActive { views },
            Structural::IsValid => ExplodedEdgeLeaf::IsValid { views },
            Structural::IsDeleted => ExplodedEdgeLeaf::IsDeleted { views },
            Structural::IsSelfLoop => ExplodedEdgeLeaf::IsSelfLoop { views },
        })
    }

    fn temporal(self) -> Result<Self, GraphError> {
        match self {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal: false,
            } => Ok(ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal: true,
            }),
            _ => Err(not_a_property()),
        }
    }
}

/// `temporal()` on a read: the read must be a property's latest value.
fn temporal_read<L: FromOldRead>(expr: SplitExpr<L>) -> Result<SplitExpr<L>, GraphError> {
    match expr {
        SplitExpr::Read(leaf) => Ok(SplitExpr::Read(leaf.temporal()?)),
        _ => Err(not_a_property()),
    }
}

/// Convert a value, collecting the qualifiers written on it (innermost first)
/// so the predicate around it can take them.
fn convert_value<L: FromOldRead>(
    expr: &Expr,
    quals: &mut Vec<Qual>,
) -> Result<SplitExpr<L>, GraphError> {
    Ok(match expr {
        Expr::Const(value) => SplitExpr::Const(value.clone()),
        Expr::Read { scope, target } => SplitExpr::Read(L::from_read(scope, target)?),
        Expr::Temporal(inner) => temporal_read(convert_value(inner, quals)?)?,
        Expr::Agg(agg, inner) => SplitExpr::Agg(*agg, Box::new(convert_value(inner, quals)?)),
        Expr::Qual(qual, inner) => {
            let value = convert_value(inner, quals)?;
            quals.push(*qual);
            value
        }
    })
}

/// Wrap a predicate in the qualifiers its value carried. The innermost
/// qualifier written collapses the outermost list level, so it goes on last.
fn qualify<L: Leaf>(mut pred: SplitExpr<L>, quals: Vec<Qual>) -> SplitExpr<L> {
    for qual in quals.iter().rev() {
        pred = match qual {
            Qual::Any => SplitExpr::Any(Box::new(pred)),
            Qual::All => SplitExpr::All(Box::new(pred)),
        };
    }
    pred
}

fn convert_predicate<L: FromOldRead>(filter: &FilterExpr) -> Result<SplitExpr<L>, GraphError> {
    let mut quals = Vec::new();
    let pred = match filter {
        FilterExpr::Cmp { op, lhs, rhs } => SplitExpr::Cmp(
            *op,
            Box::new(convert_value(lhs, &mut quals)?),
            Box::new(convert_value(rhs, &mut quals)?),
        ),
        FilterExpr::Str { op, lhs, rhs } => SplitExpr::Str(
            op.clone(),
            Box::new(convert_value(lhs, &mut quals)?),
            Box::new(convert_value(rhs, &mut quals)?),
        ),
        FilterExpr::IsSome(e) => SplitExpr::IsSome(Box::new(convert_value(e, &mut quals)?)),
        FilterExpr::IsNone(e) => SplitExpr::IsNone(Box::new(convert_value(e, &mut quals)?)),
        FilterExpr::In {
            expr,
            values,
            negated,
        } => SplitExpr::In {
            expr: Box::new(convert_value(expr, &mut quals)?),
            values: values.clone(),
            negated: *negated,
        },
        FilterExpr::Structural { scope, pred } => SplitExpr::Read(L::structural(scope, *pred)?),
        FilterExpr::View(_)
        | FilterExpr::And(_)
        | FilterExpr::Or(_)
        | FilterExpr::Not(_)
        | FilterExpr::Opaque(_) => unreachable!("handled by FilterExpr::to_split"),
    };
    Ok(qualify(pred, quals))
}

/// The entity a value reads from, if it reads at all.
fn value_entity(expr: &Expr) -> Option<Entity> {
    match expr {
        Expr::Const(_) => None,
        Expr::Read { scope, .. } => Some(scope.entity),
        Expr::Temporal(e) | Expr::Agg(_, e) | Expr::Qual(_, e) => value_entity(e),
    }
}

fn predicate_entity(filter: &FilterExpr) -> Result<Entity, GraphError> {
    let entity = match filter {
        FilterExpr::Cmp { lhs, rhs, .. } | FilterExpr::Str { lhs, rhs, .. } => {
            value_entity(lhs).or_else(|| value_entity(rhs))
        }
        FilterExpr::IsSome(e) | FilterExpr::IsNone(e) | FilterExpr::In { expr: e, .. } => {
            value_entity(e)
        }
        FilterExpr::Structural { scope, .. } => Some(scope.entity),
        _ => None,
    };
    entity.ok_or_else(|| invalid("a comparison needs an entity value on at least one side"))
}

impl FilterExpr {
    /// This filter as the per-entity tree.
    pub fn to_split(&self) -> Result<SplitFilter, GraphError> {
        Ok(match self {
            FilterExpr::View(ops) => SplitFilter::View(ops.clone()),
            FilterExpr::And(items) => {
                SplitFilter::And(items.iter().map(Self::to_split).collect::<Result<_, _>>()?)
            }
            FilterExpr::Or(items) => {
                SplitFilter::Or(items.iter().map(Self::to_split).collect::<Result<_, _>>()?)
            }
            FilterExpr::Not(inner) => SplitFilter::Not(Box::new(inner.to_split()?)),
            FilterExpr::Opaque(filter) => SplitFilter::Opaque(filter.clone()),
            predicate => match predicate_entity(predicate)? {
                Entity::Node => SplitFilter::Node(convert_predicate(predicate)?),
                Entity::Edge => SplitFilter::Edge(convert_predicate(predicate)?),
                Entity::ExplodedEdge => SplitFilter::ExplodedEdge(convert_predicate(predicate)?),
            },
        })
    }

    /// The erased, applicable form of this filter.
    pub fn compile(&self) -> Result<Arc<dyn DynCreateFilter>, GraphError> {
        self.to_split()?.compile()
    }
}

impl Expr {
    /// The erased, compilable form of this value. A qualifier written on the
    /// value has no meaning without the comparison that follows it and is
    /// left out here; the value it qualifies is what compiles.
    pub fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        let mut quals = Vec::new();
        match value_entity(self).unwrap_or(Entity::Node) {
            Entity::Node => convert_value::<NodeLeaf>(self, &mut quals)?.compile_value(),
            Entity::Edge => convert_value::<EdgeLeaf>(self, &mut quals)?.compile_value(),
            Entity::ExplodedEdge => {
                convert_value::<ExplodedEdgeLeaf>(self, &mut quals)?.compile_value()
            }
        }
    }
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
                edge_filter::EdgeFilter, node_filter::NodeFilter, windowed_filter::Windowed,
                EdgeViewFilterOps, PropertyExprFactory, ViewWrapOps,
            },
        },
        prelude::{
            AdditionOps, EdgeViewOps, EntityAggOps, EntityExprFilterOps, Graph, GraphViewOps,
            NodeViewOps, TimeOps, NO_PROPS,
        },
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
    fn before_and_at_agree_with_the_graph_views() {
        // alice→bob @2 (first event at 2) · carol→dave @2 (second event at 2) · eve→fay @5
        let g = Graph::new();
        g.add_edge(2, "alice", "bob", NO_PROPS, None).unwrap();
        g.add_edge(2, "carol", "dave", NO_PROPS, None).unwrap();
        g.add_edge(5, "eve", "fay", NO_PROPS, None).unwrap();
        fn edge_names<'graph, G: GraphViewOps<'graph>>(g: &G) -> Vec<String> {
            let mut ids: Vec<String> = g
                .edges()
                .iter()
                .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                .collect();
            ids.sort();
            ids
        }
        let view = |op: ViewOp| FilterExpr::View(vec![op]).compile().unwrap();
        let typed = |f: Windowed<EdgeFilter>| Arc::new(f.is_active()) as Arc<dyn DynCreateFilter>;
        let none: [&str; 0] = [];
        let at_two = ["alice->bob", "carol->dave"];

        // `before(t)` excludes every event at `t`, like the graph view does.
        assert_eq!(edge_names(&g.before(2)), none);
        assert_eq!(edges(&g, view(ViewOp::Before(EventTime::start(2)))), none);
        assert_eq!(edges(&g, typed(EdgeFilter.before(2))), none);
        assert_eq!(edge_names(&g.before(3)), at_two);
        assert_eq!(edges(&g, view(ViewOp::Before(EventTime::start(3)))), at_two);

        // `at(t)` covers the whole timestamp, even when handed a time that sits
        // between two events at `t`.
        let mid_two = EventTime::start(2).set_event_id(1);
        assert_eq!(edge_names(&g.at(mid_two)), at_two);
        assert_eq!(edges(&g, view(ViewOp::At(mid_two))), at_two);
        assert_eq!(edges(&g, typed(EdgeFilter.at(mid_two))), at_two);

        // A window bound that carries an event id is honoured, as the graph view does.
        let from_second_event = EventTime::start(2).set_event_id(1);
        assert_eq!(
            edge_names(&g.window(from_second_event, EventTime::start(3))),
            ["carol->dave"]
        );
        assert_eq!(
            edges(
                &g,
                view(ViewOp::Window {
                    start: from_second_event,
                    end: EventTime::start(3),
                }),
            ),
            ["carol->dave"]
        );
        assert_eq!(
            edges(&g, typed(EdgeFilter.window(from_second_event, 3))),
            ["carol->dave"]
        );

        // `after(t)` excludes `t` and everything before it.
        assert_eq!(edge_names(&g.after(2)), ["eve->fay"]);
        assert_eq!(
            edges(&g, view(ViewOp::After(EventTime::start(2)))),
            ["eve->fay"]
        );
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

    #[test]
    fn an_opaque_filter_refuses_to_serialise() {
        let compiled = FilterExpr::View(vec![ViewOp::Latest]).compile().unwrap();
        let opaque = FilterExpr::Opaque(OpaqueFilter(compiled));
        let err = serde_json::to_string(&opaque).unwrap_err();
        assert!(err.to_string().contains(OPAQUE_FILTER_ERROR), "{err}");
        assert!(!opaque.tests_edges());
    }

    #[test]
    fn an_exploded_edge_has_no_endpoint() {
        let scope = Scope::new(Entity::ExplodedEdge).through(Endpoint::Src);
        let read = FilterExpr::Cmp {
            op: CmpOp::Eq,
            lhs: Expr::Read {
                scope: scope.clone(),
                target: Target::Field(Field::Name),
            },
            rhs: Expr::Const(Prop::str("alice")),
        };
        assert!(read.compile().is_err());
        let active = FilterExpr::Structural {
            scope,
            pred: Structural::IsActive,
        };
        assert!(active.compile().is_err());
    }

    #[test]
    fn a_view_leg_restricts_the_whole_filter() {
        // alice's only update (t=6) and bob's (t=1) are inside [0, 7); dave's (t=8) is not.
        let g = Graph::new();
        g.add_node(6, "alice", [("score", Prop::F64(9.0))], None, None)
            .unwrap();
        g.add_node(1, "bob", [("score", Prop::F64(5.0))], None, None)
            .unwrap();
        g.add_node(8, "dave", [("score", Prop::F64(10.0))], None, None)
            .unwrap();
        g.add_node(0, "carol", NO_PROPS, None, None).unwrap();
        let score_gt_4 = FilterExpr::Cmp {
            op: CmpOp::Gt,
            lhs: Expr::Read {
                scope: Scope::new(Entity::Node),
                target: Target::Property("score".into()),
            },
            rhs: Expr::Const(Prop::F64(4.0)),
        };
        let window = FilterExpr::View(vec![ViewOp::Window {
            start: EventTime::start(0),
            end: EventTime::end(7),
        }]);
        // The view applies first and the predicate runs inside it.
        let tree = FilterExpr::And(vec![window.clone(), score_gt_4.clone()]);
        assert_eq!(nodes(&g, tree.compile().unwrap()), ["alice", "bob"]);
        // A predicate every node passes still leaves the view's members only.
        let named = FilterExpr::IsSome(Expr::Read {
            scope: Scope::new(Entity::Node),
            target: Target::Field(Field::Name),
        });
        let all_in_window = FilterExpr::And(vec![window.clone(), named]);
        assert_eq!(
            nodes(&g, all_in_window.compile().unwrap()),
            ["alice", "bob", "carol"]
        );
        // Nested `and`s flatten, so the view still reaches the top.
        let nested = FilterExpr::And(vec![
            FilterExpr::And(vec![window.clone()]),
            score_gt_4.clone(),
        ]);
        assert_eq!(nodes(&g, nested.compile().unwrap()), ["alice", "bob"]);
        // Under `or` or `not` a view has no meaning the engine can give it.
        assert!(FilterExpr::Or(vec![window.clone(), score_gt_4.clone()])
            .compile()
            .is_err());
        assert!(FilterExpr::Not(Box::new(window.clone())).compile().is_err());
        assert!(FilterExpr::Or(vec![
            FilterExpr::And(vec![window, score_gt_4.clone()]),
            score_gt_4
        ])
        .compile()
        .is_err());
    }
}
