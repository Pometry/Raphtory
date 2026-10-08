//! The GraphQL form of the filter expression tree.
//!
//! Each entity has its own expression type, [`NodeExpr`], [`EdgeExpr`] and
//! [`ExplodedEdgeExpr`], declared in full with its operands (`NodeComparison`,
//! `NodeStringTest`, …) and mirroring the core [`expr::Expr`] variant for
//! variant. What differs between entities is only what a `read` can name
//! ([`NodeRead`], [`EdgeRead`], [`ExplodedEdgeRead`]). The conversion to and
//! from the tree is written once, over [`EntityInput`], which takes each
//! entity's expression apart into the shared [`ExprShape`]. So a filter
//! written in python, rust or a GraphQL document is the same tree spelled
//! three ways.
//!
//! ```graphql
//! filter(expr: { node: { cmp: { op: GT, lhs: { read: { property: "score" } }, rhs: { const: { i64: 4 } } } } })
//! filter(expr: { node: { cmp: { op: GT, lhs: { read: { field: DEGREE } }, rhs: { read: { field: IN_DEGREE } } } } })
//! filter(expr: { node: { str: { op: FUZZY, lhs: { read: { field: NAME } }, rhs: { const: { str: "alise" } }, levenshteinDistance: 1, prefixMatch: false } } })
//! filter(expr: { node: { quantified: { op: ANY, expr: { cmp: { op: GT, lhs: { read: { temporalProperty: "score" } }, rhs: { const: { i64: 8 } } } } } } })
//! filter(expr: { edge: { cmp: { op: EQ, lhs: { read: { src: { read: { field: NAME } } } }, rhs: { const: { str: "alice" } } } } })
//! ```
//!
//! A read names exactly one term (`property`, `temporalProperty`, `metadata` or
//! `field`; on an edge also `src` or `dst`) and may carry `views`, which scope
//! that term: `{ read: { property: "score", views: [{ window: { start: 0, end: 2 } }, { kind: LATEST }] } }`.
//! A view is one of `window`, `at`, `after`, `before`, `snapshotAt`, `layers`,
//! `excludeLayers` (or `excludeLayer` for one name), `shrinkStart`, `shrinkEnd`,
//! `excludeNodes`, `subgraph`, `subgraphNodeTypes`, or a `kind` that takes no
//! argument (`LATEST`, `SNAPSHOT_LATEST`, `VALID`, `DEFAULT_LAYER`), each the
//! graph view of the same name applied to the view built so far.
//!
//! The serde form is the GraphQL spelling, so the JSON a stored grant holds and
//! the variables a client sends are the same text as a GraphQL literal.

use crate::model::graph::{
    filtering::{Window, Wrapped},
    node_id::GqlNodeId,
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
                expr::{
                    self, Agg, EdgeLeaf, ExplodedEdgeLeaf, Field, Leaf, NodeLeaf, ViewOp,
                    OPAQUE_FILTER_ERROR,
                },
                filter_operator::{BinaryOp, StringOp},
            },
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::EventTime,
    utils::time::{InputTime, IntoTime},
    Direction,
};
use serde::{Deserialize, Serialize};
use std::{ops::Deref, sync::Arc};

// ── the operators ────────────────────────────────────────────────────────────

/// How two values compare.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CmpOp {
    /// `lhs == rhs`.
    Eq,
    /// `lhs != rhs`.
    Ne,
    /// `lhs < rhs`.
    Lt,
    /// `lhs <= rhs`.
    Le,
    /// `lhs > rhs`.
    Gt,
    /// `lhs >= rhs`.
    Ge,
}

impl From<CmpOp> for BinaryOp {
    fn from(op: CmpOp) -> Self {
        match op {
            CmpOp::Eq => BinaryOp::Eq,
            CmpOp::Ne => BinaryOp::Ne,
            CmpOp::Lt => BinaryOp::Lt,
            CmpOp::Le => BinaryOp::Le,
            CmpOp::Gt => BinaryOp::Gt,
            CmpOp::Ge => BinaryOp::Ge,
        }
    }
}

impl From<BinaryOp> for CmpOp {
    fn from(op: BinaryOp) -> Self {
        match op {
            BinaryOp::Eq => CmpOp::Eq,
            BinaryOp::Ne => CmpOp::Ne,
            BinaryOp::Lt => CmpOp::Lt,
            BinaryOp::Le => CmpOp::Le,
            BinaryOp::Gt => CmpOp::Gt,
            BinaryOp::Ge => CmpOp::Ge,
        }
    }
}

/// How one string is tested against another.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrOp {
    /// The string `lhs` starts with the string `rhs`.
    StartsWith,
    /// The string `lhs` ends with the string `rhs`.
    EndsWith,
    /// The string `lhs` contains the string `rhs`.
    Contains,
    /// The string `lhs` does not contain the string `rhs`.
    NotContains,
    /// The string `lhs` is within `levenshteinDistance` edits of `rhs`,
    /// optionally matching by prefix; the only test that takes those two.
    Fuzzy,
}

/// How the values of a list are reduced to one.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AggOp {
    /// The sum of the innermost list.
    Sum,
    /// The mean of the innermost list.
    Avg,
    /// The smallest element of the innermost list.
    Min,
    /// The largest element of the innermost list.
    Max,
    /// The first element of the innermost list.
    First,
    /// The last element of the innermost list.
    Last,
    /// The number of elements of the innermost list.
    Len,
    /// The earliest update of a temporal history.
    Earliest,
    /// The latest update of a temporal history.
    Latest,
}

impl From<AggOp> for Agg {
    fn from(op: AggOp) -> Self {
        match op {
            AggOp::Sum => Agg::Sum,
            AggOp::Avg => Agg::Avg,
            AggOp::Min => Agg::Min,
            AggOp::Max => Agg::Max,
            AggOp::First => Agg::First,
            AggOp::Last => Agg::Last,
            AggOp::Len => Agg::Len,
            AggOp::Earliest => Agg::Earliest,
            AggOp::Latest => Agg::Latest,
        }
    }
}

impl From<Agg> for AggOp {
    fn from(op: Agg) -> Self {
        match op {
            Agg::Sum => AggOp::Sum,
            Agg::Avg => AggOp::Avg,
            Agg::Min => AggOp::Min,
            Agg::Max => AggOp::Max,
            Agg::First => AggOp::First,
            Agg::Last => AggOp::Last,
            Agg::Len => AggOp::Len,
            Agg::Earliest => AggOp::Earliest,
            Agg::Latest => AggOp::Latest,
        }
    }
}

/// Whether a value is there.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PresenceOp {
    /// The value is present.
    IsSome,
    /// The value is absent.
    IsNone,
}

/// How an element-wise yes/no over a list becomes one yes/no.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Quantifier {
    /// Holds when the yes/no inside holds for any element.
    Any,
    /// Holds when the yes/no inside holds for every element.
    All,
}

/// A built-in node term that takes no argument.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeField {
    /// The node's name.
    Name,
    /// The node's id.
    Id,
    /// The node's type.
    NodeType,
    /// The number of edges at the node, in and out.
    Degree,
    /// The number of edges into the node.
    InDegree,
    /// The number of edges out of the node.
    OutDegree,
    /// Whether the node is active.
    IsActive,
}

impl From<NodeField> for NodeLeaf {
    fn from(f: NodeField) -> Self {
        let views = Vec::new();
        let field = |field| NodeLeaf::Field {
            views: Vec::new(),
            field,
        };
        let degree = |direction| NodeLeaf::Degree {
            views: Vec::new(),
            direction,
        };
        match f {
            NodeField::Name => field(Field::Name),
            NodeField::Id => field(Field::Id),
            NodeField::NodeType => field(Field::NodeType),
            NodeField::Degree => degree(Direction::BOTH),
            NodeField::InDegree => degree(Direction::IN),
            NodeField::OutDegree => degree(Direction::OUT),
            NodeField::IsActive => NodeLeaf::IsActive { views },
        }
    }
}

impl From<Field> for NodeField {
    fn from(f: Field) -> Self {
        match f {
            Field::Id => NodeField::Id,
            Field::Name => NodeField::Name,
            Field::NodeType => NodeField::NodeType,
        }
    }
}

impl From<Direction> for NodeField {
    fn from(d: Direction) -> Self {
        match d {
            Direction::BOTH => NodeField::Degree,
            Direction::IN => NodeField::InDegree,
            Direction::OUT => NodeField::OutDegree,
        }
    }
}

/// A built-in edge term that takes no argument; exploded edges have the same.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EdgeField {
    /// Whether the edge is active.
    IsActive,
    /// Whether the edge is valid (on a persistent graph, its last update is an
    /// addition).
    IsValid,
    /// Whether the edge is deleted.
    IsDeleted,
    /// Whether the edge is a self loop.
    IsSelfLoop,
}

impl From<EdgeField> for EdgeLeaf {
    fn from(f: EdgeField) -> Self {
        let views = Vec::new();
        match f {
            EdgeField::IsActive => EdgeLeaf::IsActive { views },
            EdgeField::IsValid => EdgeLeaf::IsValid { views },
            EdgeField::IsDeleted => EdgeLeaf::IsDeleted { views },
            EdgeField::IsSelfLoop => EdgeLeaf::IsSelfLoop { views },
        }
    }
}

impl From<EdgeField> for ExplodedEdgeLeaf {
    fn from(f: EdgeField) -> Self {
        let views = Vec::new();
        match f {
            EdgeField::IsActive => ExplodedEdgeLeaf::IsActive { views },
            EdgeField::IsValid => ExplodedEdgeLeaf::IsValid { views },
            EdgeField::IsDeleted => ExplodedEdgeLeaf::IsDeleted { views },
            EdgeField::IsSelfLoop => ExplodedEdgeLeaf::IsSelfLoop { views },
        }
    }
}

// ── views ────────────────────────────────────────────────────────────────────

/// A view that takes no argument.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ViewKind {
    /// At the latest time.
    Latest,
    /// Everything up to and including the latest time.
    SnapshotLatest,
    /// Only the edges that are valid in the view (on a persistent graph, whose
    /// last update is an addition).
    Valid,
    /// Only the default layer.
    DefaultLayer,
}

impl From<ViewKind> for ViewOp {
    fn from(kind: ViewKind) -> Self {
        match kind {
            ViewKind::Latest => ViewOp::Latest,
            ViewKind::SnapshotLatest => ViewOp::SnapshotLatest,
            ViewKind::Valid => ViewOp::Valid,
            ViewKind::DefaultLayer => ViewOp::DefaultLayer,
        }
    }
}

/// One view restriction, applied in list order.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "ViewOp")]
pub enum GqlViewOp {
    /// Between `start` (inclusive) and `end` (exclusive).
    Window(Window),
    /// At one time.
    At(GqlTimeInput),
    /// Strictly after a time.
    After(GqlTimeInput),
    /// Strictly before a time.
    Before(GqlTimeInput),
    /// A view that takes no argument: `LATEST`, `SNAPSHOT_LATEST`, `VALID` or
    /// `DEFAULT_LAYER`.
    Kind(ViewKind),
    /// Everything up to and including a time; written `snapshotAt: t`.
    SnapshotAt(GqlTimeInput),
    /// Only the named layers.
    Layers(Vec<String>),
    /// Every layer except the named ones.
    ExcludeLayers(Vec<String>),
    /// Every layer except the named one; the same view as `excludeLayers`
    /// with one name.
    ExcludeLayer(String),
    /// The window's start moved to a time when that is later; the window only
    /// ever narrows.
    ShrinkStart(GqlTimeInput),
    /// The window's end moved to a time when that is earlier; the window only
    /// ever narrows.
    ShrinkEnd(GqlTimeInput),
    /// Every node except the named ones, with their edges; an id the view does
    /// not hold changes nothing.
    ExcludeNodes(Vec<GqlNodeId>),
    /// Only the named nodes and the edges between them; an id the view does
    /// not hold is skipped.
    Subgraph(Vec<GqlNodeId>),
    /// Only the nodes of the named types and the edges between them.
    SubgraphNodeTypes(Vec<String>),
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
            GqlViewOp::Kind(kind) => kind.into(),
            GqlViewOp::SnapshotAt(t) => ViewOp::SnapshotAt(t.into_time()),
            GqlViewOp::Layers(names) => ViewOp::Layers(names),
            GqlViewOp::ExcludeLayers(names) => ViewOp::ExcludeLayers(names),
            GqlViewOp::ExcludeLayer(name) => ViewOp::ExcludeLayers(vec![name]),
            GqlViewOp::ShrinkStart(t) => ViewOp::ShrinkStart(t.into_time()),
            GqlViewOp::ShrinkEnd(t) => ViewOp::ShrinkEnd(t.into_time()),
            GqlViewOp::ExcludeNodes(ids) => {
                ViewOp::ExcludeNodes(ids.into_iter().map(GID::from).collect())
            }
            GqlViewOp::Subgraph(ids) => ViewOp::Subgraph(ids.into_iter().map(GID::from).collect()),
            GqlViewOp::SubgraphNodeTypes(types) => ViewOp::SubgraphNodeTypes(types),
        }
    }
}

fn time(t: EventTime) -> GqlTimeInput {
    GqlTimeInput(InputTime::Indexed(t.0, t.1))
}

fn node_ids(ids: &[GID]) -> Vec<GqlNodeId> {
    ids.iter().cloned().map(GqlNodeId).collect()
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
            ViewOp::Latest => GqlViewOp::Kind(ViewKind::Latest),
            ViewOp::SnapshotAt(t) => GqlViewOp::SnapshotAt(time(*t)),
            ViewOp::SnapshotLatest => GqlViewOp::Kind(ViewKind::SnapshotLatest),
            ViewOp::Layers(names) => GqlViewOp::Layers(names.clone()),
            ViewOp::DefaultLayer => GqlViewOp::Kind(ViewKind::DefaultLayer),
            ViewOp::ExcludeLayers(names) => GqlViewOp::ExcludeLayers(names.clone()),
            ViewOp::ShrinkStart(t) => GqlViewOp::ShrinkStart(time(*t)),
            ViewOp::ShrinkEnd(t) => GqlViewOp::ShrinkEnd(time(*t)),
            ViewOp::ExcludeNodes(ids) => GqlViewOp::ExcludeNodes(node_ids(ids)),
            ViewOp::Subgraph(ids) => GqlViewOp::Subgraph(node_ids(ids)),
            ViewOp::SubgraphNodeTypes(types) => GqlViewOp::SubgraphNodeTypes(types.clone()),
            ViewOp::Valid => GqlViewOp::Kind(ViewKind::Valid),
        }
    }
}

/// The views a read carries, as the wire spells them: absent when there are none.
fn read_views(views: &[ViewOp]) -> Option<Vec<GqlViewOp>> {
    (!views.is_empty()).then(|| views.iter().map(GqlViewOp::from).collect())
}

// ── the reads: what a term names on each entity ──────────────────────────────

/// What a read names on one entity: how `NodeRead`, `EdgeRead` or
/// `ExplodedEdgeRead` becomes the tree's leaf and back. The one place the
/// entities differ in more than their type names.
pub trait Term: Sized {
    /// The tree's leaf for this entity.
    type Leaf: Leaf;

    /// The leaf this read names, with its views; refused unless exactly one
    /// term is set.
    fn into_leaf(self) -> Result<Self::Leaf, GraphError>;

    /// The read that names `leaf`.
    fn from_leaf(leaf: &Self::Leaf) -> Result<Self, GraphError>;
}

/// The one term a read names, scoped by its views. A read that names none or
/// several is refused, naming the fields given.
fn one_term<L: Leaf, const N: usize>(
    terms: [(&str, Option<L>); N],
    views: Option<Vec<GqlViewOp>>,
) -> Result<L, GraphError> {
    let mut given: Vec<(&str, L)> = terms
        .into_iter()
        .filter_map(|(name, term)| term.map(|term| (name, term)))
        .collect();
    if given.len() != 1 {
        let names: Vec<String> = given.iter().map(|(name, _)| format!("`{name}`")).collect();
        let got = match names.as_slice() {
            [] => "none".to_string(),
            [init @ .., last] => format!("{} and {last}", init.join(", ")),
        };
        return Err(invalid(format!("a read names one term, got {got}")));
    }
    let (_, mut leaf) = given.remove(0);
    for op in views.into_iter().flatten() {
        leaf.push_view(op.into());
    }
    Ok(leaf)
}

/// One node term, read through optional views. Name exactly one of
/// `property`, `temporalProperty`, `metadata` or `field`.
#[derive(InputObject, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct NodeRead {
    /// The latest value of a property.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
    /// The history of a property, as a list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal_property: Option<String>,
    /// A metadata entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    /// A built-in node term.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<NodeField>,
    /// Views that scope the term, applied in list order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<Vec<GqlViewOp>>,
}

impl Term for NodeRead {
    type Leaf = NodeLeaf;

    fn into_leaf(self) -> Result<NodeLeaf, GraphError> {
        one_term(
            [
                (
                    "property",
                    self.property
                        .map(|name| NodeLeaf::property(Vec::new(), name, false)),
                ),
                (
                    "temporalProperty",
                    self.temporal_property
                        .map(|name| NodeLeaf::property(Vec::new(), name, true)),
                ),
                (
                    "metadata",
                    self.metadata
                        .map(|name| NodeLeaf::metadata(Vec::new(), name)),
                ),
                ("field", self.field.map(NodeLeaf::from)),
            ],
            self.views,
        )
    }

    fn from_leaf(leaf: &NodeLeaf) -> Result<Self, GraphError> {
        let (views, read) = match leaf {
            NodeLeaf::Field { views, field } => (
                views,
                NodeRead {
                    field: Some((*field).into()),
                    ..Default::default()
                },
            ),
            NodeLeaf::Degree { views, direction } => (
                views,
                NodeRead {
                    field: Some((*direction).into()),
                    ..Default::default()
                },
            ),
            NodeLeaf::Property {
                views,
                name,
                temporal: false,
            } => (
                views,
                NodeRead {
                    property: Some(name.clone()),
                    ..Default::default()
                },
            ),
            NodeLeaf::Property {
                views,
                name,
                temporal: true,
            } => (
                views,
                NodeRead {
                    temporal_property: Some(name.clone()),
                    ..Default::default()
                },
            ),
            NodeLeaf::Metadata { views, name } => (
                views,
                NodeRead {
                    metadata: Some(name.clone()),
                    ..Default::default()
                },
            ),
            NodeLeaf::IsActive { views } => (
                views,
                NodeRead {
                    field: Some(NodeField::IsActive),
                    ..Default::default()
                },
            ),
        };
        Ok(NodeRead {
            views: read_views(views),
            ..read
        })
    }
}

/// One edge term, read through optional views. Name exactly one of
/// `property`, `temporalProperty`, `metadata`, `field`, `src` or `dst`.
#[derive(InputObject, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EdgeRead {
    /// The latest value of a property.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
    /// The history of a property, as a list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal_property: Option<String>,
    /// A metadata entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    /// A built-in edge term.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<EdgeField>,
    /// A node expression evaluated on the edge's source node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub src: Option<Wrapped<NodeExpr>>,
    /// A node expression evaluated on the edge's destination node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dst: Option<Wrapped<NodeExpr>>,
    /// Views that scope the term, applied in list order; on `src` or `dst`
    /// they scope every node term inside.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<Vec<GqlViewOp>>,
}

/// A node expression on one end of an edge, as a tree.
fn endpoint(e: Option<Wrapped<NodeExpr>>) -> Result<Option<Box<expr::NodeExpr>>, GraphError> {
    e.map(|e| Ok(Box::new(e.into_inner().into_tree()?)))
        .transpose()
}

impl Term for EdgeRead {
    type Leaf = EdgeLeaf;

    fn into_leaf(self) -> Result<EdgeLeaf, GraphError> {
        one_term(
            [
                (
                    "property",
                    self.property
                        .map(|name| EdgeLeaf::property(Vec::new(), name, false)),
                ),
                (
                    "temporalProperty",
                    self.temporal_property
                        .map(|name| EdgeLeaf::property(Vec::new(), name, true)),
                ),
                (
                    "metadata",
                    self.metadata
                        .map(|name| EdgeLeaf::metadata(Vec::new(), name)),
                ),
                ("field", self.field.map(EdgeLeaf::from)),
                ("src", endpoint(self.src)?.map(EdgeLeaf::Src)),
                ("dst", endpoint(self.dst)?.map(EdgeLeaf::Dst)),
            ],
            self.views,
        )
    }

    fn from_leaf(leaf: &EdgeLeaf) -> Result<Self, GraphError> {
        let node = |e: &expr::NodeExpr| -> Result<_, GraphError> {
            Ok(Some(Wrapped::from(NodeExpr::from_tree(e)?)))
        };
        let (views, read): (&[ViewOp], _) = match leaf {
            EdgeLeaf::Property {
                views,
                name,
                temporal: false,
            } => (
                views,
                EdgeRead {
                    property: Some(name.clone()),
                    ..Default::default()
                },
            ),
            EdgeLeaf::Property {
                views,
                name,
                temporal: true,
            } => (
                views,
                EdgeRead {
                    temporal_property: Some(name.clone()),
                    ..Default::default()
                },
            ),
            EdgeLeaf::Metadata { views, name } => (
                views,
                EdgeRead {
                    metadata: Some(name.clone()),
                    ..Default::default()
                },
            ),
            EdgeLeaf::IsActive { views } => (views, EdgeRead::field(EdgeField::IsActive)),
            EdgeLeaf::IsValid { views } => (views, EdgeRead::field(EdgeField::IsValid)),
            EdgeLeaf::IsDeleted { views } => (views, EdgeRead::field(EdgeField::IsDeleted)),
            EdgeLeaf::IsSelfLoop { views } => (views, EdgeRead::field(EdgeField::IsSelfLoop)),
            // The endpoint's own terms carry their views; there are none here.
            EdgeLeaf::Src(inner) => (
                &[],
                EdgeRead {
                    src: node(inner)?,
                    ..Default::default()
                },
            ),
            EdgeLeaf::Dst(inner) => (
                &[],
                EdgeRead {
                    dst: node(inner)?,
                    ..Default::default()
                },
            ),
        };
        Ok(EdgeRead {
            views: read_views(views),
            ..read
        })
    }
}

impl EdgeRead {
    fn field(field: EdgeField) -> Self {
        EdgeRead {
            field: Some(field),
            ..Default::default()
        }
    }
}

/// One exploded-edge term (one update of an edge), read through optional
/// views. Name exactly one of `property`, `temporalProperty`, `metadata` or
/// `field`.
#[derive(InputObject, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ExplodedEdgeRead {
    /// The latest value of a property.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
    /// The history of a property, as a list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal_property: Option<String>,
    /// A metadata entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    /// A built-in edge term.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<EdgeField>,
    /// Views that scope the term, applied in list order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<Vec<GqlViewOp>>,
}

impl Term for ExplodedEdgeRead {
    type Leaf = ExplodedEdgeLeaf;

    fn into_leaf(self) -> Result<ExplodedEdgeLeaf, GraphError> {
        one_term(
            [
                (
                    "property",
                    self.property
                        .map(|name| ExplodedEdgeLeaf::property(Vec::new(), name, false)),
                ),
                (
                    "temporalProperty",
                    self.temporal_property
                        .map(|name| ExplodedEdgeLeaf::property(Vec::new(), name, true)),
                ),
                (
                    "metadata",
                    self.metadata
                        .map(|name| ExplodedEdgeLeaf::metadata(Vec::new(), name)),
                ),
                ("field", self.field.map(ExplodedEdgeLeaf::from)),
            ],
            self.views,
        )
    }

    fn from_leaf(leaf: &ExplodedEdgeLeaf) -> Result<Self, GraphError> {
        let field = |field| ExplodedEdgeRead {
            field: Some(field),
            ..Default::default()
        };
        let (views, read) = match leaf {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal: false,
            } => (
                views,
                ExplodedEdgeRead {
                    property: Some(name.clone()),
                    ..Default::default()
                },
            ),
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal: true,
            } => (
                views,
                ExplodedEdgeRead {
                    temporal_property: Some(name.clone()),
                    ..Default::default()
                },
            ),
            ExplodedEdgeLeaf::Metadata { views, name } => (
                views,
                ExplodedEdgeRead {
                    metadata: Some(name.clone()),
                    ..Default::default()
                },
            ),
            ExplodedEdgeLeaf::IsActive { views } => (views, field(EdgeField::IsActive)),
            ExplodedEdgeLeaf::IsValid { views } => (views, field(EdgeField::IsValid)),
            ExplodedEdgeLeaf::IsDeleted { views } => (views, field(EdgeField::IsDeleted)),
            ExplodedEdgeLeaf::IsSelfLoop { views } => (views, field(EdgeField::IsSelfLoop)),
        };
        Ok(ExplodedEdgeRead {
            views: read_views(views),
            ..read
        })
    }
}

// ── node expressions ─────────────────────────────────────────────────────────

/// A value or a yes/no on a node: a literal, a read of a term, an aggregate, a comparison or test, or a combination of yes/nos.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeExpr {
    /// A literal.
    Const(Value),
    /// One term of the node.
    Read(NodeRead),
    /// A list reduced to one value.
    Agg(NodeAggregate),
    /// Two values compared.
    Cmp(NodeComparison),
    /// One string tested against another.
    Str(NodeStringTest),
    /// `expr` is one of `values`.
    IsIn(NodeMembership),
    /// `expr` is none of `values`.
    IsNotIn(NodeMembership),
    /// Whether a value is there.
    Presence(NodePresence),
    /// An element-wise yes/no over a list, made one.
    Quantified(NodeQuantified),
    /// Every yes/no inside holds.
    And(Vec<NodeExpr>),
    /// Any yes/no inside holds.
    Or(Vec<NodeExpr>),
    /// The yes/no inside does not hold.
    Not(Wrapped<NodeExpr>),
}

/// Two values compared.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct NodeComparison {
    /// How they compare.
    pub op: CmpOp,
    /// The left side.
    pub lhs: Wrapped<NodeExpr>,
    /// The right side.
    pub rhs: Wrapped<NodeExpr>,
}

/// One string tested against another. `levenshteinDistance` and `prefixMatch` are required with `FUZZY` and refused with every other test.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct NodeStringTest {
    /// The test.
    pub op: StrOp,
    /// The string tested.
    pub lhs: Wrapped<NodeExpr>,
    /// The string it is tested against.
    pub rhs: Wrapped<NodeExpr>,
    /// `FUZZY` only: the largest edit distance that still matches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub levenshtein_distance: Option<usize>,
    /// `FUZZY` only: whether a match on a prefix counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_match: Option<bool>,
}

/// A membership test. `values` is a list; a policy may also leave a single placeholder here (`{"var": …}`) that resolves to the list per caller.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct NodeMembership {
    /// The value to look for.
    pub expr: Wrapped<NodeExpr>,
    /// The values it may be one of.
    pub values: Value,
}

/// Whether a value is there.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct NodePresence {
    /// Present or absent.
    pub op: PresenceOp,
    /// The value.
    pub expr: Wrapped<NodeExpr>,
}

/// An element-wise yes/no over a list, made one yes/no.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct NodeQuantified {
    /// Any element or every element.
    pub op: Quantifier,
    /// The element-wise yes/no.
    pub expr: Wrapped<NodeExpr>,
}

/// A list reduced to one value.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct NodeAggregate {
    /// The reduction.
    pub op: AggOp,
    /// The list; the innermost list when lists nest.
    pub expr: Wrapped<NodeExpr>,
}

impl EntityInput for NodeExpr {
    type Leaf = NodeLeaf;
    type Read = NodeRead;

    fn into_shape(self) -> ExprShape<Self> {
        match self {
            NodeExpr::Const(v) => ExprShape::Const(v),
            NodeExpr::Read(read) => ExprShape::Read(read),
            NodeExpr::Agg(NodeAggregate { op, expr }) => ExprShape::Agg { op, expr },
            NodeExpr::Cmp(NodeComparison { op, lhs, rhs }) => ExprShape::Cmp { op, lhs, rhs },
            NodeExpr::Str(NodeStringTest {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            }) => ExprShape::Str {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            },
            NodeExpr::IsIn(NodeMembership { expr, values }) => ExprShape::IsIn { expr, values },
            NodeExpr::IsNotIn(NodeMembership { expr, values }) => {
                ExprShape::IsNotIn { expr, values }
            }
            NodeExpr::Presence(NodePresence { op, expr }) => ExprShape::Presence { op, expr },
            NodeExpr::Quantified(NodeQuantified { op, expr }) => ExprShape::Quantified { op, expr },
            NodeExpr::And(items) => ExprShape::And(items),
            NodeExpr::Or(items) => ExprShape::Or(items),
            NodeExpr::Not(e) => ExprShape::Not(e),
        }
    }

    fn from_shape(shape: ExprShape<Self>) -> Self {
        match shape {
            ExprShape::Const(v) => NodeExpr::Const(v),
            ExprShape::Read(read) => NodeExpr::Read(read),
            ExprShape::Agg { op, expr } => NodeExpr::Agg(NodeAggregate { op, expr }),
            ExprShape::Cmp { op, lhs, rhs } => NodeExpr::Cmp(NodeComparison { op, lhs, rhs }),
            ExprShape::Str {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            } => NodeExpr::Str(NodeStringTest {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            }),
            ExprShape::IsIn { expr, values } => NodeExpr::IsIn(NodeMembership { expr, values }),
            ExprShape::IsNotIn { expr, values } => {
                NodeExpr::IsNotIn(NodeMembership { expr, values })
            }
            ExprShape::Presence { op, expr } => NodeExpr::Presence(NodePresence { op, expr }),
            ExprShape::Quantified { op, expr } => NodeExpr::Quantified(NodeQuantified { op, expr }),
            ExprShape::And(items) => NodeExpr::And(items),
            ExprShape::Or(items) => NodeExpr::Or(items),
            ExprShape::Not(e) => NodeExpr::Not(e),
        }
    }
}

// ── edge expressions ─────────────────────────────────────────────────────────

/// A value or a yes/no on an edge: a literal, a read of a term, an aggregate, a comparison or test, or a combination of yes/nos.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EdgeExpr {
    /// A literal.
    Const(Value),
    /// One term of the edge.
    Read(EdgeRead),
    /// A list reduced to one value.
    Agg(EdgeAggregate),
    /// Two values compared.
    Cmp(EdgeComparison),
    /// One string tested against another.
    Str(EdgeStringTest),
    /// `expr` is one of `values`.
    IsIn(EdgeMembership),
    /// `expr` is none of `values`.
    IsNotIn(EdgeMembership),
    /// Whether a value is there.
    Presence(EdgePresence),
    /// An element-wise yes/no over a list, made one.
    Quantified(EdgeQuantified),
    /// Every yes/no inside holds.
    And(Vec<EdgeExpr>),
    /// Any yes/no inside holds.
    Or(Vec<EdgeExpr>),
    /// The yes/no inside does not hold.
    Not(Wrapped<EdgeExpr>),
}

/// Two values compared.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EdgeComparison {
    /// How they compare.
    pub op: CmpOp,
    /// The left side.
    pub lhs: Wrapped<EdgeExpr>,
    /// The right side.
    pub rhs: Wrapped<EdgeExpr>,
}

/// One string tested against another. `levenshteinDistance` and `prefixMatch` are required with `FUZZY` and refused with every other test.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EdgeStringTest {
    /// The test.
    pub op: StrOp,
    /// The string tested.
    pub lhs: Wrapped<EdgeExpr>,
    /// The string it is tested against.
    pub rhs: Wrapped<EdgeExpr>,
    /// `FUZZY` only: the largest edit distance that still matches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub levenshtein_distance: Option<usize>,
    /// `FUZZY` only: whether a match on a prefix counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_match: Option<bool>,
}

/// A membership test. `values` is a list; a policy may also leave a single placeholder here (`{"var": …}`) that resolves to the list per caller.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EdgeMembership {
    /// The value to look for.
    pub expr: Wrapped<EdgeExpr>,
    /// The values it may be one of.
    pub values: Value,
}

/// Whether a value is there.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EdgePresence {
    /// Present or absent.
    pub op: PresenceOp,
    /// The value.
    pub expr: Wrapped<EdgeExpr>,
}

/// An element-wise yes/no over a list, made one yes/no.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EdgeQuantified {
    /// Any element or every element.
    pub op: Quantifier,
    /// The element-wise yes/no.
    pub expr: Wrapped<EdgeExpr>,
}

/// A list reduced to one value.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EdgeAggregate {
    /// The reduction.
    pub op: AggOp,
    /// The list; the innermost list when lists nest.
    pub expr: Wrapped<EdgeExpr>,
}

impl EntityInput for EdgeExpr {
    type Leaf = EdgeLeaf;
    type Read = EdgeRead;

    fn into_shape(self) -> ExprShape<Self> {
        match self {
            EdgeExpr::Const(v) => ExprShape::Const(v),
            EdgeExpr::Read(read) => ExprShape::Read(read),
            EdgeExpr::Agg(EdgeAggregate { op, expr }) => ExprShape::Agg { op, expr },
            EdgeExpr::Cmp(EdgeComparison { op, lhs, rhs }) => ExprShape::Cmp { op, lhs, rhs },
            EdgeExpr::Str(EdgeStringTest {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            }) => ExprShape::Str {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            },
            EdgeExpr::IsIn(EdgeMembership { expr, values }) => ExprShape::IsIn { expr, values },
            EdgeExpr::IsNotIn(EdgeMembership { expr, values }) => {
                ExprShape::IsNotIn { expr, values }
            }
            EdgeExpr::Presence(EdgePresence { op, expr }) => ExprShape::Presence { op, expr },
            EdgeExpr::Quantified(EdgeQuantified { op, expr }) => ExprShape::Quantified { op, expr },
            EdgeExpr::And(items) => ExprShape::And(items),
            EdgeExpr::Or(items) => ExprShape::Or(items),
            EdgeExpr::Not(e) => ExprShape::Not(e),
        }
    }

    fn from_shape(shape: ExprShape<Self>) -> Self {
        match shape {
            ExprShape::Const(v) => EdgeExpr::Const(v),
            ExprShape::Read(read) => EdgeExpr::Read(read),
            ExprShape::Agg { op, expr } => EdgeExpr::Agg(EdgeAggregate { op, expr }),
            ExprShape::Cmp { op, lhs, rhs } => EdgeExpr::Cmp(EdgeComparison { op, lhs, rhs }),
            ExprShape::Str {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            } => EdgeExpr::Str(EdgeStringTest {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            }),
            ExprShape::IsIn { expr, values } => EdgeExpr::IsIn(EdgeMembership { expr, values }),
            ExprShape::IsNotIn { expr, values } => {
                EdgeExpr::IsNotIn(EdgeMembership { expr, values })
            }
            ExprShape::Presence { op, expr } => EdgeExpr::Presence(EdgePresence { op, expr }),
            ExprShape::Quantified { op, expr } => EdgeExpr::Quantified(EdgeQuantified { op, expr }),
            ExprShape::And(items) => EdgeExpr::And(items),
            ExprShape::Or(items) => EdgeExpr::Or(items),
            ExprShape::Not(e) => EdgeExpr::Not(e),
        }
    }
}

// ── exploded-edge expressions ────────────────────────────────────────────────

/// A value or a yes/no on an exploded edge: a literal, a read of a term, an aggregate, a comparison or test, or a combination of yes/nos.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExplodedEdgeExpr {
    /// A literal.
    Const(Value),
    /// One term of the exploded edge.
    Read(ExplodedEdgeRead),
    /// A list reduced to one value.
    Agg(ExplodedEdgeAggregate),
    /// Two values compared.
    Cmp(ExplodedEdgeComparison),
    /// One string tested against another.
    Str(ExplodedEdgeStringTest),
    /// `expr` is one of `values`.
    IsIn(ExplodedEdgeMembership),
    /// `expr` is none of `values`.
    IsNotIn(ExplodedEdgeMembership),
    /// Whether a value is there.
    Presence(ExplodedEdgePresence),
    /// An element-wise yes/no over a list, made one.
    Quantified(ExplodedEdgeQuantified),
    /// Every yes/no inside holds.
    And(Vec<ExplodedEdgeExpr>),
    /// Any yes/no inside holds.
    Or(Vec<ExplodedEdgeExpr>),
    /// The yes/no inside does not hold.
    Not(Wrapped<ExplodedEdgeExpr>),
}

/// Two values compared.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ExplodedEdgeComparison {
    /// How they compare.
    pub op: CmpOp,
    /// The left side.
    pub lhs: Wrapped<ExplodedEdgeExpr>,
    /// The right side.
    pub rhs: Wrapped<ExplodedEdgeExpr>,
}

/// One string tested against another. `levenshteinDistance` and `prefixMatch` are required with `FUZZY` and refused with every other test.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ExplodedEdgeStringTest {
    /// The test.
    pub op: StrOp,
    /// The string tested.
    pub lhs: Wrapped<ExplodedEdgeExpr>,
    /// The string it is tested against.
    pub rhs: Wrapped<ExplodedEdgeExpr>,
    /// `FUZZY` only: the largest edit distance that still matches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub levenshtein_distance: Option<usize>,
    /// `FUZZY` only: whether a match on a prefix counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_match: Option<bool>,
}

/// A membership test. `values` is a list; a policy may also leave a single placeholder here (`{"var": …}`) that resolves to the list per caller.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ExplodedEdgeMembership {
    /// The value to look for.
    pub expr: Wrapped<ExplodedEdgeExpr>,
    /// The values it may be one of.
    pub values: Value,
}

/// Whether a value is there.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ExplodedEdgePresence {
    /// Present or absent.
    pub op: PresenceOp,
    /// The value.
    pub expr: Wrapped<ExplodedEdgeExpr>,
}

/// An element-wise yes/no over a list, made one yes/no.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ExplodedEdgeQuantified {
    /// Any element or every element.
    pub op: Quantifier,
    /// The element-wise yes/no.
    pub expr: Wrapped<ExplodedEdgeExpr>,
}

/// A list reduced to one value.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ExplodedEdgeAggregate {
    /// The reduction.
    pub op: AggOp,
    /// The list; the innermost list when lists nest.
    pub expr: Wrapped<ExplodedEdgeExpr>,
}

impl EntityInput for ExplodedEdgeExpr {
    type Leaf = ExplodedEdgeLeaf;
    type Read = ExplodedEdgeRead;

    fn into_shape(self) -> ExprShape<Self> {
        match self {
            ExplodedEdgeExpr::Const(v) => ExprShape::Const(v),
            ExplodedEdgeExpr::Read(read) => ExprShape::Read(read),
            ExplodedEdgeExpr::Agg(ExplodedEdgeAggregate { op, expr }) => {
                ExprShape::Agg { op, expr }
            }
            ExplodedEdgeExpr::Cmp(ExplodedEdgeComparison { op, lhs, rhs }) => {
                ExprShape::Cmp { op, lhs, rhs }
            }
            ExplodedEdgeExpr::Str(ExplodedEdgeStringTest {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            }) => ExprShape::Str {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            },
            ExplodedEdgeExpr::IsIn(ExplodedEdgeMembership { expr, values }) => {
                ExprShape::IsIn { expr, values }
            }
            ExplodedEdgeExpr::IsNotIn(ExplodedEdgeMembership { expr, values }) => {
                ExprShape::IsNotIn { expr, values }
            }
            ExplodedEdgeExpr::Presence(ExplodedEdgePresence { op, expr }) => {
                ExprShape::Presence { op, expr }
            }
            ExplodedEdgeExpr::Quantified(ExplodedEdgeQuantified { op, expr }) => {
                ExprShape::Quantified { op, expr }
            }
            ExplodedEdgeExpr::And(items) => ExprShape::And(items),
            ExplodedEdgeExpr::Or(items) => ExprShape::Or(items),
            ExplodedEdgeExpr::Not(e) => ExprShape::Not(e),
        }
    }

    fn from_shape(shape: ExprShape<Self>) -> Self {
        match shape {
            ExprShape::Const(v) => ExplodedEdgeExpr::Const(v),
            ExprShape::Read(read) => ExplodedEdgeExpr::Read(read),
            ExprShape::Agg { op, expr } => {
                ExplodedEdgeExpr::Agg(ExplodedEdgeAggregate { op, expr })
            }
            ExprShape::Cmp { op, lhs, rhs } => {
                ExplodedEdgeExpr::Cmp(ExplodedEdgeComparison { op, lhs, rhs })
            }
            ExprShape::Str {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            } => ExplodedEdgeExpr::Str(ExplodedEdgeStringTest {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            }),
            ExprShape::IsIn { expr, values } => {
                ExplodedEdgeExpr::IsIn(ExplodedEdgeMembership { expr, values })
            }
            ExprShape::IsNotIn { expr, values } => {
                ExplodedEdgeExpr::IsNotIn(ExplodedEdgeMembership { expr, values })
            }
            ExprShape::Presence { op, expr } => {
                ExplodedEdgeExpr::Presence(ExplodedEdgePresence { op, expr })
            }
            ExprShape::Quantified { op, expr } => {
                ExplodedEdgeExpr::Quantified(ExplodedEdgeQuantified { op, expr })
            }
            ExprShape::And(items) => ExplodedEdgeExpr::And(items),
            ExprShape::Or(items) => ExplodedEdgeExpr::Or(items),
            ExprShape::Not(e) => ExplodedEdgeExpr::Not(e),
        }
    }
}

// ── the conversion, written once for every entity ────────────────────────────

/// An entity's expression one level deep, in the shape every entity shares:
/// the variant and its operands, the operands still the entity's own type.
/// [`EntityInput`] takes [`NodeExpr`], [`EdgeExpr`] and [`ExplodedEdgeExpr`]
/// apart into it and puts them back together, so the conversion to and from
/// the tree is written once, over this shape.
pub enum ExprShape<E: EntityInput> {
    Const(Value),
    Read(E::Read),
    Agg {
        op: AggOp,
        expr: Wrapped<E>,
    },
    Cmp {
        op: CmpOp,
        lhs: Wrapped<E>,
        rhs: Wrapped<E>,
    },
    Str {
        op: StrOp,
        lhs: Wrapped<E>,
        rhs: Wrapped<E>,
        levenshtein_distance: Option<usize>,
        prefix_match: Option<bool>,
    },
    IsIn {
        expr: Wrapped<E>,
        values: Value,
    },
    IsNotIn {
        expr: Wrapped<E>,
        values: Value,
    },
    Presence {
        op: PresenceOp,
        expr: Wrapped<E>,
    },
    Quantified {
        op: Quantifier,
        expr: Wrapped<E>,
    },
    And(Vec<E>),
    Or(Vec<E>),
    Not(Wrapped<E>),
}

/// One entity's expression type: what its reads name, how it is taken apart
/// into an [`ExprShape`] and put back together, and with those, its
/// conversion to and from the tree.
pub trait EntityInput: Sized {
    /// The tree's leaf for this entity.
    type Leaf: Leaf;

    /// What a read names on this entity.
    type Read: Term<Leaf = Self::Leaf>;

    /// The expression one level deep.
    fn into_shape(self) -> ExprShape<Self>;

    /// The expression from its parts.
    fn from_shape(shape: ExprShape<Self>) -> Self;

    /// The expression as a tree.
    fn into_tree(self) -> Result<expr::Expr<Self::Leaf>, GraphError> {
        let all = |items: Vec<Self>| -> Result<Vec<_>, GraphError> {
            items.into_iter().map(Self::into_tree).collect()
        };
        Ok(match self.into_shape() {
            ExprShape::Const(v) => expr::Expr::Const(prop(v)?),
            ExprShape::Read(read) => expr::Expr::Term(read.into_leaf()?),
            ExprShape::Agg { op, expr } => expr::Expr::Agg(op.into(), subtree(expr)?),
            ExprShape::Cmp { op, lhs, rhs } => {
                expr::Expr::Cmp(op.into(), subtree(lhs)?, subtree(rhs)?)
            }
            ExprShape::Str {
                op,
                lhs,
                rhs,
                levenshtein_distance,
                prefix_match,
            } => expr::Expr::Str(
                string_op(op, levenshtein_distance, prefix_match)?,
                subtree(lhs)?,
                subtree(rhs)?,
            ),
            ExprShape::IsIn { expr, values } => expr::Expr::In {
                expr: subtree(expr)?,
                values: member_values(values, false)?,
                negated: false,
            },
            ExprShape::IsNotIn { expr, values } => expr::Expr::In {
                expr: subtree(expr)?,
                values: member_values(values, true)?,
                negated: true,
            },
            ExprShape::Presence { op, expr } => match op {
                PresenceOp::IsSome => expr::Expr::IsSome(subtree(expr)?),
                PresenceOp::IsNone => expr::Expr::IsNone(subtree(expr)?),
            },
            ExprShape::Quantified { op, expr } => match op {
                Quantifier::Any => expr::Expr::Any(subtree(expr)?),
                Quantifier::All => expr::Expr::All(subtree(expr)?),
            },
            ExprShape::And(items) => expr::Expr::And(all(items)?),
            ExprShape::Or(items) => expr::Expr::Or(all(items)?),
            ExprShape::Not(e) => expr::Expr::Not(subtree(e)?),
        })
    }

    /// The expression a tree is spelled as.
    fn from_tree(e: &expr::Expr<Self::Leaf>) -> Result<Self, GraphError> {
        let all = |items: &[expr::Expr<Self::Leaf>]| -> Result<Vec<Self>, GraphError> {
            items.iter().map(Self::from_tree).collect()
        };
        let shape = match e {
            expr::Expr::Const(p) => ExprShape::Const(value(p)?),
            expr::Expr::Opaque(_) => return Err(invalid(OPAQUE_FILTER_ERROR)),
            expr::Expr::Term(leaf) => ExprShape::Read(<Self::Read as Term>::from_leaf(leaf)?),
            expr::Expr::Agg(op, e) => ExprShape::Agg {
                op: (*op).into(),
                expr: wire(e)?,
            },
            expr::Expr::Cmp(op, l, r) => ExprShape::Cmp {
                op: (*op).into(),
                lhs: wire(l)?,
                rhs: wire(r)?,
            },
            expr::Expr::Str(op, l, r) => {
                let (op, levenshtein_distance, prefix_match) = match *op {
                    StringOp::StartsWith => (StrOp::StartsWith, None, None),
                    StringOp::EndsWith => (StrOp::EndsWith, None, None),
                    StringOp::Contains => (StrOp::Contains, None, None),
                    StringOp::NotContains => (StrOp::NotContains, None, None),
                    StringOp::FuzzySearch {
                        levenshtein_distance,
                        prefix_match,
                    } => (StrOp::Fuzzy, Some(levenshtein_distance), Some(prefix_match)),
                };
                ExprShape::Str {
                    op,
                    lhs: wire(l)?,
                    rhs: wire(r)?,
                    levenshtein_distance,
                    prefix_match,
                }
            }
            expr::Expr::In {
                expr,
                values,
                negated,
            } => {
                let expr = wire(expr)?;
                let values = Value::List(values.iter().map(value).collect::<Result<_, _>>()?);
                if *negated {
                    ExprShape::IsNotIn { expr, values }
                } else {
                    ExprShape::IsIn { expr, values }
                }
            }
            expr::Expr::IsSome(e) => ExprShape::Presence {
                op: PresenceOp::IsSome,
                expr: wire(e)?,
            },
            expr::Expr::IsNone(e) => ExprShape::Presence {
                op: PresenceOp::IsNone,
                expr: wire(e)?,
            },
            expr::Expr::Any(e) => ExprShape::Quantified {
                op: Quantifier::Any,
                expr: wire(e)?,
            },
            expr::Expr::All(e) => ExprShape::Quantified {
                op: Quantifier::All,
                expr: wire(e)?,
            },
            expr::Expr::And(items) => ExprShape::And(all(items)?),
            expr::Expr::Or(items) => ExprShape::Or(all(items)?),
            expr::Expr::Not(e) => ExprShape::Not(wire(e)?),
        };
        Ok(Self::from_shape(shape))
    }
}

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidGqlFilter(msg.into())
}

fn prop(value: Value) -> Result<Prop, GraphError> {
    Prop::try_from(value).map_err(|e| invalid(format!("invalid constant: {e}")))
}

fn value(p: &Prop) -> Result<Value, GraphError> {
    Value::try_from(p).map_err(|e| invalid(format!("constant has no wire form: {e}")))
}

/// The members of a set: a list of constants. Anything else, a placeholder a
/// policy failed to resolve included, is refused.
fn member_values(values: Value, negated: bool) -> Result<Vec<Prop>, GraphError> {
    let op = if negated { "isNotIn" } else { "isIn" };
    match values {
        Value::List(items) => items.into_iter().map(prop).collect(),
        other => Err(invalid(format!("{op} requires a list value, got {other}"))),
    }
}

/// A subexpression as a tree.
fn subtree<E: EntityInput>(e: Wrapped<E>) -> Result<Box<expr::Expr<E::Leaf>>, GraphError> {
    Ok(Box::new(e.into_inner().into_tree()?))
}

/// A subexpression in the wire form.
fn wire<E: EntityInput>(e: &expr::Expr<E::Leaf>) -> Result<Wrapped<E>, GraphError> {
    Ok(Wrapped::from(E::from_tree(e)?))
}

/// The tree's string operator; `levenshteinDistance` and `prefixMatch`
/// belong to `FUZZY` alone.
fn string_op(
    op: StrOp,
    levenshtein_distance: Option<usize>,
    prefix_match: Option<bool>,
) -> Result<StringOp, GraphError> {
    let plain = |op| match (levenshtein_distance, prefix_match) {
        (None, None) => Ok(op),
        _ => Err(invalid(
            "only FUZZY takes `levenshteinDistance` and `prefixMatch`",
        )),
    };
    match op {
        StrOp::StartsWith => plain(StringOp::StartsWith),
        StrOp::EndsWith => plain(StringOp::EndsWith),
        StrOp::Contains => plain(StringOp::Contains),
        StrOp::NotContains => plain(StringOp::NotContains),
        StrOp::Fuzzy => match (levenshtein_distance, prefix_match) {
            (Some(levenshtein_distance), Some(prefix_match)) => Ok(StringOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            }),
            _ => Err(invalid(
                "FUZZY requires `levenshteinDistance` and `prefixMatch`",
            )),
        },
    }
}

// ── the filter ───────────────────────────────────────────────────────────────

/// The filter itself: a yes/no over one kind of entity, a view, or a combination.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[graphql(name = "FilterExpr")]
pub enum GqlFilter {
    /// A yes/no over nodes.
    Node(NodeExpr),
    /// A yes/no over edges.
    Edge(EdgeExpr),
    /// A yes/no over exploded edges, one per update of an edge.
    ExplodedEdge(ExplodedEdgeExpr),
    /// A graph-level view with no predicate: the result is the view.
    View(Vec<GqlViewOp>),
    /// Every leg holds. A view leg applies first and the others run inside it.
    And(Vec<GqlFilter>),
    /// Any leg holds. Node legs combine on nodes and edge legs on edges; a leg
    /// of the other kind leaves that side unconstrained. No view legs.
    Or(Vec<GqlFilter>),
    /// The filter that keeps what the inner one drops: a negated node filter
    /// keeps the nodes that fail it and the edges between them. No views.
    Not(Wrapped<GqlFilter>),
}

impl TryFrom<GqlFilter> for expr::FilterExpr {
    type Error = GraphError;

    fn try_from(filter: GqlFilter) -> Result<Self, Self::Error> {
        use expr::FilterExpr as F;
        Ok(match filter {
            GqlFilter::Node(e) => F::Node(e.into_tree()?),
            GqlFilter::Edge(e) => F::Edge(e.into_tree()?),
            GqlFilter::ExplodedEdge(e) => F::ExplodedEdge(e.into_tree()?),
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
            GqlFilter::Not(inner) => F::Not(Box::new(inner.into_inner().try_into()?)),
        })
    }
}

// Clients build trees and send them; this is the spelling they send.
impl TryFrom<&expr::FilterExpr> for GqlFilter {
    type Error = GraphError;

    fn try_from(filter: &expr::FilterExpr) -> Result<Self, Self::Error> {
        use expr::FilterExpr as F;
        Ok(match filter {
            F::Node(e) => GqlFilter::Node(NodeExpr::from_tree(e)?),
            F::Edge(e) => GqlFilter::Edge(EdgeExpr::from_tree(e)?),
            F::ExplodedEdge(e) => GqlFilter::ExplodedEdge(ExplodedEdgeExpr::from_tree(e)?),
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

impl TryFrom<expr::FilterExpr> for GqlFilter {
    type Error = GraphError;

    fn try_from(filter: expr::FilterExpr) -> Result<Self, Self::Error> {
        GqlFilter::try_from(&filter)
    }
}

impl CreateFilter for GqlFilter {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        expr::FilterExpr::try_from(self)?.create_graph_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        expr::FilterExpr::try_from(self)?.create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        expr::FilterExpr::try_from(self)?.create_edge_filter(graph)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql::Variables;
    use dynamic_graphql::{App, Request, ResolvedObject, ResolvedObjectFields};
    use expr::FilterExpr as F;
    use itertools::Itertools;
    use raphtory::db::graph::views::filter::model::{
        node_filter::NodeFilter, EntityExprFilterOps, PropertyExprFactory,
    };
    use serde_json::json;

    /// A schema with one field that parses a filter the way every resolver
    /// does and answers the tree it became, as JSON.
    #[derive(ResolvedObject)]
    #[graphql(root)]
    struct Echo;

    #[ResolvedObjectFields]
    impl Echo {
        /// The tree `expr` parses to, as JSON.
        async fn tree(expr: GqlFilter) -> dynamic_graphql::Result<String> {
            Ok(serde_json::to_string(&F::try_from(expr)?)?)
        }
    }

    #[derive(App)]
    struct EchoApp(Echo);

    async fn run(request: Request) -> Result<String, String> {
        let schema = EchoApp::create_schema().finish().unwrap();
        let res = schema.execute(request).await;
        if !res.errors.is_empty() {
            return Err(res.errors.iter().map(|e| e.message.as_str()).join("; "));
        }
        let data = res.data.into_json().unwrap();
        Ok(data["tree"].as_str().unwrap().to_string())
    }

    /// The tree a filter sent as a GraphQL variable becomes, as JSON.
    async fn from_variable(filter: serde_json::Value) -> Result<String, String> {
        run(Request::new("query($f: FilterExpr!) { tree(expr: $f) }")
            .variables(Variables::from_json(json!({ "f": filter }))))
        .await
    }

    /// The tree a filter written as a GraphQL literal becomes, as JSON.
    async fn from_literal(filter: &str) -> Result<String, String> {
        run(Request::new(format!("{{ tree(expr: {filter}) }}"))).await
    }

    /// The tree a filter stored as JSON (a grant) becomes.
    fn from_json(filter: serde_json::Value) -> Result<F, String> {
        let wire: GqlFilter = serde_json::from_value(filter).map_err(|e| e.to_string())?;
        F::try_from(wire).map_err(|e| e.to_string())
    }

    fn tree_json(tree: &F) -> String {
        serde_json::to_string(tree).unwrap()
    }

    /// The tree goes to the wire type, to JSON, and back by serde and by
    /// GraphQL, and is the same tree every way.
    async fn assert_round_trip(tree: F) {
        let wire = GqlFilter::try_from(&tree).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(from_json(json.clone()).unwrap(), tree);
        assert_eq!(from_variable(json).await.unwrap(), tree_json(&tree));
    }

    fn t(at: i64) -> EventTime {
        EventTime::from(at)
    }

    fn every_view() -> Vec<ViewOp> {
        vec![
            ViewOp::Window {
                start: t(0),
                end: t(5),
            },
            ViewOp::At(t(1)),
            ViewOp::After(t(2)),
            ViewOp::Before(t(9)),
            ViewOp::Latest,
            ViewOp::SnapshotAt(t(4)),
            ViewOp::SnapshotLatest,
            ViewOp::Layers(vec!["work".into()]),
            ViewOp::DefaultLayer,
            ViewOp::ExcludeLayers(vec!["a".into(), "b".into()]),
            ViewOp::ShrinkStart(t(3)),
            ViewOp::ShrinkEnd(t(8)),
            ViewOp::ExcludeNodes(vec![GID::Str("a".into()), GID::U64(7)]),
            ViewOp::Subgraph(vec![GID::U64(1), GID::Str("b".into())]),
            ViewOp::SubgraphNodeTypes(vec!["person".into()]),
            ViewOp::Valid,
        ]
    }

    /// Every variant of the tree over one entity: each leaf given (as a
    /// presence test), and every operator around the first one.
    fn every_variant<L: Leaf>(leaves: Vec<L>) -> expr::Expr<L> {
        let term = || Box::new(expr::Expr::Term(leaves[0].clone()));
        let one = || Box::new(expr::Expr::Const(Prop::I64(1)));
        let text = || Box::new(expr::Expr::Const(Prop::str("al")));
        let mut all: Vec<expr::Expr<L>> = leaves
            .iter()
            .map(|leaf| expr::Expr::IsSome(Box::new(expr::Expr::Term(leaf.clone()))))
            .collect();
        all.push(expr::Expr::Const(Prop::Bool(true)));
        for agg in [
            Agg::Sum,
            Agg::Avg,
            Agg::Min,
            Agg::Max,
            Agg::First,
            Agg::Last,
            Agg::Len,
            Agg::Earliest,
            Agg::Latest,
        ] {
            all.push(expr::Expr::Cmp(
                BinaryOp::Ge,
                Box::new(expr::Expr::Agg(agg, term())),
                one(),
            ));
        }
        for op in [
            BinaryOp::Eq,
            BinaryOp::Ne,
            BinaryOp::Lt,
            BinaryOp::Le,
            BinaryOp::Gt,
            BinaryOp::Ge,
        ] {
            all.push(expr::Expr::Cmp(op, term(), one()));
        }
        for op in [
            StringOp::StartsWith,
            StringOp::EndsWith,
            StringOp::Contains,
            StringOp::NotContains,
            StringOp::FuzzySearch {
                levenshtein_distance: 2,
                prefix_match: true,
            },
        ] {
            all.push(expr::Expr::Str(op, term(), text()));
        }
        for negated in [false, true] {
            all.push(expr::Expr::In {
                expr: term(),
                values: vec![Prop::str("alice"), Prop::I64(3)],
                negated,
            });
        }
        all.push(expr::Expr::IsNone(term()));
        let gt = || Box::new(expr::Expr::Cmp(BinaryOp::Gt, term(), one()));
        all.push(expr::Expr::Any(gt()));
        all.push(expr::Expr::All(gt()));
        all.push(expr::Expr::Or(vec![*gt(), expr::Expr::Not(gt())]));
        expr::Expr::And(all)
    }

    fn node_leaves() -> Vec<NodeLeaf> {
        let field = |field| NodeLeaf::Field {
            views: Vec::new(),
            field,
        };
        let degree = |direction| NodeLeaf::Degree {
            views: Vec::new(),
            direction,
        };
        vec![
            NodeLeaf::Property {
                views: every_view(),
                name: "score".into(),
                temporal: false,
            },
            NodeLeaf::Property {
                views: Vec::new(),
                name: "score".into(),
                temporal: true,
            },
            NodeLeaf::Metadata {
                views: vec![ViewOp::Latest],
                name: "region".into(),
            },
            field(Field::Name),
            field(Field::Id),
            field(Field::NodeType),
            degree(Direction::BOTH),
            degree(Direction::IN),
            degree(Direction::OUT),
            NodeLeaf::IsActive {
                views: vec![ViewOp::Layers(vec!["work".into()])],
            },
        ]
    }

    #[tokio::test]
    async fn every_node_variant_round_trips() {
        assert_round_trip(F::Node(every_variant(node_leaves()))).await;
    }

    #[tokio::test]
    async fn every_edge_variant_round_trips() {
        let views = || vec![ViewOp::At(t(2)), ViewOp::DefaultLayer];
        let endpoint = || Box::new(every_variant(node_leaves()));
        let leaves = vec![
            EdgeLeaf::Property {
                views: every_view(),
                name: "w".into(),
                temporal: false,
            },
            EdgeLeaf::Property {
                views: Vec::new(),
                name: "w".into(),
                temporal: true,
            },
            EdgeLeaf::Metadata {
                views: Vec::new(),
                name: "kind".into(),
            },
            EdgeLeaf::IsActive { views: views() },
            EdgeLeaf::IsValid { views: Vec::new() },
            EdgeLeaf::IsDeleted { views: views() },
            EdgeLeaf::IsSelfLoop { views: Vec::new() },
            EdgeLeaf::Src(endpoint()),
            EdgeLeaf::Dst(endpoint()),
        ];
        assert_round_trip(F::Edge(every_variant(leaves))).await;
    }

    #[tokio::test]
    async fn every_exploded_edge_variant_round_trips() {
        let leaves = vec![
            ExplodedEdgeLeaf::Property {
                views: every_view(),
                name: "w".into(),
                temporal: false,
            },
            ExplodedEdgeLeaf::Property {
                views: Vec::new(),
                name: "w".into(),
                temporal: true,
            },
            ExplodedEdgeLeaf::Metadata {
                views: Vec::new(),
                name: "kind".into(),
            },
            ExplodedEdgeLeaf::IsActive {
                views: vec![ViewOp::Valid],
            },
            ExplodedEdgeLeaf::IsValid { views: Vec::new() },
            ExplodedEdgeLeaf::IsDeleted { views: Vec::new() },
            ExplodedEdgeLeaf::IsSelfLoop { views: Vec::new() },
        ];
        assert_round_trip(F::ExplodedEdge(every_variant(leaves))).await;
    }

    #[tokio::test]
    async fn every_filter_variant_round_trips() {
        let score = || {
            F::Node(expr::Expr::IsSome(Box::new(expr::Expr::Term(
                NodeLeaf::Property {
                    views: Vec::new(),
                    name: "score".into(),
                    temporal: false,
                },
            ))))
        };
        assert_round_trip(F::And(vec![
            F::View(every_view()),
            F::Or(vec![score(), score()]),
            F::Not(Box::new(score())),
        ]))
        .await;
    }

    #[tokio::test]
    async fn views_on_an_endpoint_read_scope_the_node_terms_inside() {
        let outside = json!({ "edge": { "read": {
            "src": { "read": { "property": "score" } },
            "views": [{ "at": 2 }]
        } } });
        let inside = json!({ "edge": { "read": {
            "src": { "read": { "property": "score", "views": [{ "at": 2 }] } }
        } } });
        assert_eq!(
            from_variable(outside).await.unwrap(),
            from_variable(inside).await.unwrap()
        );
    }

    #[tokio::test]
    async fn the_wire_json_is_the_graphql_spelling() {
        let tree = F::Edge(expr::Expr::Cmp(
            BinaryOp::Eq,
            Box::new(expr::Expr::Term(EdgeLeaf::Src(Box::new(expr::Expr::Term(
                NodeLeaf::Field {
                    views: Vec::new(),
                    field: Field::Name,
                },
            ))))),
            Box::new(expr::Expr::Const(Prop::str("alice"))),
        ));
        let wire = GqlFilter::try_from(&tree).unwrap();
        assert_eq!(
            serde_json::to_string(&wire).unwrap(),
            r#"{"edge":{"cmp":{"op":"EQ","lhs":{"read":{"src":{"read":{"field":"NAME"}}}},"rhs":{"const":{"str":"alice"}}}}}"#
        );
        let literal = r#"{ edge: { cmp: { op: EQ, lhs: { read: { src: { read: { field: NAME } } } }, rhs: { const: { str: "alice" } } } } }"#;
        assert_eq!(from_literal(literal).await.unwrap(), tree_json(&tree));
    }

    #[tokio::test]
    async fn the_documented_examples_parse() {
        for example in [
            r#"{ node: { cmp: { op: GT, lhs: { read: { property: "score" } }, rhs: { const: { i64: 4 } } } } }"#,
            r#"{ node: { cmp: { op: EQ, lhs: { read: { metadata: "region" } }, rhs: { const: { str: "eu" } } } } }"#,
            r#"{ node: { cmp: { op: EQ, lhs: { read: { field: NODE_TYPE } }, rhs: { const: { str: "user" } } } } }"#,
            r#"{ node: { cmp: { op: GT, lhs: { read: { field: DEGREE } }, rhs: { read: { field: IN_DEGREE } } } } }"#,
            r#"{ node: { read: { field: IS_ACTIVE } } }"#,
            r#"{ node: { str: { op: STARTS_WITH, lhs: { read: { field: NAME } }, rhs: { const: { str: "al" } } } } }"#,
            r#"{ node: { str: { op: FUZZY, lhs: { read: { field: NAME } }, rhs: { const: { str: "alise" } }, levenshteinDistance: 1, prefixMatch: false } } }"#,
            r#"{ node: { isIn: { expr: { read: { field: NAME } }, values: { list: [ { str: "alice" }, { str: "carol" } ] } } } }"#,
            r#"{ node: { presence: { op: IS_NONE, expr: { read: { property: "score" } } } } }"#,
            r#"{ node: { cmp: { op: GE, lhs: { agg: { op: SUM, expr: { read: { temporalProperty: "score" } } } }, rhs: { const: { i64: 19 } } } } }"#,
            r#"{ node: { quantified: { op: ANY, expr: { cmp: { op: GT, lhs: { read: { temporalProperty: "score" } }, rhs: { const: { i64: 8 } } } } } } }"#,
            r#"{ node: { cmp: { op: GT, lhs: { read: { property: "score", views: [ { window: { start: 0, end: 2 } } ] } }, rhs: { const: { i64: 4 } } } } }"#,
            r#"{ node: { cmp: { op: EQ, lhs: { read: { property: "score", views: [ { window: { start: 0, end: 4 } }, { kind: LATEST } ] } }, rhs: { const: { i64: 2 } } } } }"#,
            r#"{ node: { read: { field: IS_ACTIVE, views: [ { excludeNodes: ["bob"] }, { kind: LATEST } ] } } }"#,
            r#"{ edge: { cmp: { op: EQ, lhs: { read: { src: { read: { field: NAME } } } }, rhs: { const: { str: "alice" } } } } }"#,
            r#"{ edge: { cmp: { op: GT, lhs: { read: { src: { read: { property: "score", views: [ { at: 2 } ] } } } }, rhs: { read: { dst: { read: { property: "score", views: [ { at: 1 } ] } } } } } } }"#,
            r#"{ edge: { read: { field: IS_VALID, views: [ { layers: ["work"] } ] } } }"#,
            r#"{ explodedEdge: { cmp: { op: GT, lhs: { read: { property: "w" } }, rhs: { const: { f64: 5.0 } } } } }"#,
            r#"{ view: [ { window: { start: 1, end: 5 } }, { excludeLayers: ["friends"] } ] }"#,
            r#"{ view: [ { subgraph: ["alice", "bob"] }, { kind: VALID } ] }"#,
            r#"{ and: [ { view: [ { window: { start: 1, end: 5 } } ] }, { node: { cmp: { op: GT, lhs: { read: { property: "score" } }, rhs: { const: { i64: 4 } } } } } ] }"#,
            r#"{ node: { and: [ { presence: { op: IS_SOME, expr: { read: { property: "score" } } } }, { cmp: { op: EQ, lhs: { read: { field: NODE_TYPE } }, rhs: { const: { str: "user" } } } } ] } }"#,
        ] {
            if let Err(err) = from_literal(example).await {
                panic!("{example}: {err}");
            }
        }
    }

    #[tokio::test]
    async fn a_read_names_exactly_one_term() {
        for (read, got) in [
            (
                json!({ "property": "a", "metadata": "b" }),
                "got `property` and `metadata`",
            ),
            (
                json!({ "property": "a", "metadata": "b", "field": "NAME" }),
                "got `property`, `metadata` and `field`",
            ),
            (json!({}), "got none"),
            (json!({ "views": [{ "kind": "LATEST" }] }), "got none"),
        ] {
            let filter = json!({ "node": { "read": read } });
            let message = format!("a read names one term, {got}");
            let err = from_variable(filter.clone()).await.unwrap_err();
            assert!(err.contains(&message), "{err}");
            let err = from_json(filter).unwrap_err();
            assert!(err.contains(&message), "{err}");
        }
        let err = from_literal(
            r#"{ edge: { read: { field: IS_VALID, src: { read: { field: NAME } } } } }"#,
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("a read names one term, got `field` and `src`"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn only_fuzzy_takes_its_arguments() {
        let test = |extra: serde_json::Value, op: &str| {
            let mut s = json!({
                "op": op,
                "lhs": { "read": { "field": "NAME" } },
                "rhs": { "const": { "str": "al" } },
            });
            s.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            json!({ "node": { "str": s } })
        };
        for (filter, message) in [
            (
                test(json!({}), "FUZZY"),
                "FUZZY requires `levenshteinDistance` and `prefixMatch`",
            ),
            (
                test(json!({ "levenshteinDistance": 1 }), "FUZZY"),
                "FUZZY requires `levenshteinDistance` and `prefixMatch`",
            ),
            (
                test(json!({ "levenshteinDistance": 1 }), "CONTAINS"),
                "only FUZZY takes `levenshteinDistance` and `prefixMatch`",
            ),
            (
                test(
                    json!({ "levenshteinDistance": 1, "prefixMatch": true }),
                    "CONTAINS",
                ),
                "only FUZZY takes `levenshteinDistance` and `prefixMatch`",
            ),
        ] {
            let err = from_variable(filter.clone()).await.unwrap_err();
            assert!(err.contains(message), "{err}");
            let err = from_json(filter).unwrap_err();
            assert!(err.contains(message), "{err}");
        }
        let fuzzy = test(
            json!({ "levenshteinDistance": 1, "prefixMatch": false }),
            "FUZZY",
        );
        assert!(from_variable(fuzzy).await.is_ok());
    }

    #[tokio::test]
    async fn a_view_without_argument_is_a_kind() {
        for (kind, op, name) in [
            (ViewKind::Latest, ViewOp::Latest, "LATEST"),
            (
                ViewKind::SnapshotLatest,
                ViewOp::SnapshotLatest,
                "SNAPSHOT_LATEST",
            ),
            (ViewKind::Valid, ViewOp::Valid, "VALID"),
            (
                ViewKind::DefaultLayer,
                ViewOp::DefaultLayer,
                "DEFAULT_LAYER",
            ),
        ] {
            assert_eq!(ViewOp::from(GqlViewOp::Kind(kind)), op);
            let json = serde_json::to_value(GqlViewOp::from(&op)).unwrap();
            assert_eq!(json, json!({ "kind": name }));
            let back: GqlViewOp = serde_json::from_value(json).unwrap();
            assert_eq!(ViewOp::from(back), op);
        }
        for old in ["latest", "snapshotLatest", "valid", "defaultLayer"] {
            let view = json!([{ old: true }]);
            assert!(serde_json::from_value::<Vec<GqlViewOp>>(view.clone()).is_err());
            let err = from_variable(json!({ "view": view })).await.unwrap_err();
            assert!(err.contains(old), "{err}");
        }
    }

    #[test]
    fn exclude_layer_is_exclude_layers_with_one_name() {
        let one = ViewOp::from(GqlViewOp::ExcludeLayer("a".into()));
        let list = ViewOp::from(GqlViewOp::ExcludeLayers(vec!["a".into()]));
        assert_eq!(one, ViewOp::ExcludeLayers(vec!["a".into()]));
        assert_eq!(one, list);
        let json = serde_json::to_value(GqlViewOp::from(&one)).unwrap();
        assert_eq!(json, json!({ "excludeLayers": ["a"] }));
    }

    #[test]
    fn node_ids_take_the_scalar_spelling_and_keep_their_type() {
        // The wire form is the variables a client sends, so a node id is the
        // `NodeId` scalar: a JSON string or a JSON integer, never the tagged
        // serde form of `GID`.
        let op = ViewOp::ExcludeNodes(vec![GID::Str("7".into()), GID::U64(7)]);
        let json = serde_json::to_value(GqlViewOp::from(&op)).unwrap();
        assert_eq!(json, json!({ "excludeNodes": ["7", 7] }));
        let back: GqlViewOp = serde_json::from_value(json).unwrap();
        assert_eq!(ViewOp::from(back), op);
        assert!(serde_json::from_value::<GqlViewOp>(json!({ "subgraph": [-1] })).is_err());
        assert!(
            serde_json::from_value::<GqlViewOp>(json!({ "subgraph": [{ "U64": 1 }] })).is_err()
        );
    }

    #[test]
    fn an_opaque_filter_has_no_wire_form() {
        let opaque = expr::OpaqueFilter::new(
            NodeFilter
                .property("score")
                .is_some()
                .compile_value()
                .unwrap(),
        )
        .into_filter();
        let err = GqlFilter::try_from(&opaque).unwrap_err();
        assert!(err.to_string().contains(OPAQUE_FILTER_ERROR), "{err}");
    }
}
