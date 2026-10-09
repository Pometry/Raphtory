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
//! filter(expr: { node: { cmp: { op: GT, lhs: { read: { property: { name: "score", views: [{ window: { start: 0, end: 5 } }] } } }, rhs: { const: { i64: 4 } } } } })
//! filter(expr: { node: { cmp: { op: GT, lhs: { read: { field: { name: DEGREE } } }, rhs: { read: { field: { name: IN_DEGREE } } } } } })
//! filter(expr: { node: { str: { op: FUZZY, lhs: { read: { field: { name: NAME } } }, rhs: { const: { str: "alise" } }, levenshteinDistance: 1, prefixMatch: false } } })
//! filter(expr: { node: { quantified: { op: ANY, expr: { cmp: { op: GT, lhs: { read: { temporalProperty: { name: "score" } } }, rhs: { const: { i64: 8 } } } } } } })
//! filter(expr: { edge: { read: { src: { expr: { cmp: { op: EQ, lhs: { read: { field: { name: NAME } } }, rhs: { const: { str: "alice" } } } } } } } })
//! ```
//!
//! A read names exactly one of `property`, `temporalProperty`, `metadata` or
//! `field` (on an edge also `src` or `dst`). Each carries what it reads
//! (`name`, or for `src`/`dst` a node `expr`) and optional `views`, which
//! scope that term, applied in order:
//! `read: { property: { name: "score", views: [{ window: { start: 0, end: 5 } }] } }`,
//! `read: { field: { name: NAME } }`,
//! `read: { src: { expr: { cmp: { op: EQ, lhs: { read: { field: { name: NAME } } }, rhs: { const: { str: "alice" } } } } } }`.
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
        match f {
            NodeField::Name => NodeLeaf::Field {
                views,
                field: Field::Name,
            },
            NodeField::Id => NodeLeaf::Field {
                views,
                field: Field::Id,
            },
            NodeField::NodeType => NodeLeaf::Field {
                views,
                field: Field::NodeType,
            },
            NodeField::Degree => NodeLeaf::Degree {
                views,
                direction: Direction::BOTH,
            },
            NodeField::InDegree => NodeLeaf::Degree {
                views,
                direction: Direction::IN,
            },
            NodeField::OutDegree => NodeLeaf::Degree {
                views,
                direction: Direction::OUT,
            },
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

/// A term's views as a read spells them: absent when there are none.
///
/// The `Option` on a read's `views` is only the GraphQL surface's way of
/// making the field optional: an input field may be left out only when it is
/// an `Option`. The tree has one encoding, the empty list, so a read takes an
/// absent `views` as the empty list. This is used once per read payload and
/// is the only place an empty list becomes absent.
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

    /// The leaf this read names, with its views.
    fn into_leaf(self) -> Result<Self::Leaf, GraphError>;

    /// The read that names `leaf`.
    fn from_leaf(leaf: &Self::Leaf) -> Result<Self, GraphError>;
}

/// `leaf` scoped by a read's views, applied in list order after any it carries.
fn scoped<L: Leaf>(mut leaf: L, views: Option<Vec<GqlViewOp>>) -> L {
    for op in views.unwrap_or_default() {
        leaf.push_view(op.into());
    }
    leaf
}

/// A term named by a string: a property or a metadata entry.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NamedRead {
    /// The property or metadata name.
    pub name: String,
    /// Views the term is read through, in order; none when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<Vec<GqlViewOp>>,
}

/// A built-in node term.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NodeFieldRead {
    /// The built-in term.
    pub name: NodeField,
    /// Views the term is read through, in order; none when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<Vec<GqlViewOp>>,
}

/// A built-in edge or exploded-edge term.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EdgeFieldRead {
    /// The built-in term.
    pub name: EdgeField,
    /// Views the term is read through, in order; none when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<Vec<GqlViewOp>>,
}

/// A node expression evaluated on one endpoint of an edge.
#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EndpointRead {
    /// The node expression.
    pub expr: Wrapped<NodeExpr>,
    /// Views the term is read through, in order; none when absent. They scope
    /// every node term inside `expr`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<Vec<GqlViewOp>>,
}

/// One node term, read through optional views. Name exactly one of
/// `property`, `temporalProperty`, `metadata` or `field`.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeRead {
    /// The latest value of a property.
    Property(NamedRead),
    /// The history of a property, as a list.
    TemporalProperty(NamedRead),
    /// A metadata entry.
    Metadata(NamedRead),
    /// A built-in node term.
    Field(NodeFieldRead),
}

impl Term for NodeRead {
    type Leaf = NodeLeaf;

    fn into_leaf(self) -> Result<NodeLeaf, GraphError> {
        Ok(match self {
            NodeRead::Property(r) => scoped(NodeLeaf::property(Vec::new(), r.name, false), r.views),
            NodeRead::TemporalProperty(r) => {
                scoped(NodeLeaf::property(Vec::new(), r.name, true), r.views)
            }
            NodeRead::Metadata(r) => scoped(NodeLeaf::metadata(Vec::new(), r.name), r.views),
            NodeRead::Field(r) => scoped(r.name.into(), r.views),
        })
    }

    fn from_leaf(leaf: &NodeLeaf) -> Result<Self, GraphError> {
        Ok(match leaf {
            NodeLeaf::Field { views, field } => NodeRead::Field(NodeFieldRead {
                name: (*field).into(),
                views: read_views(views),
            }),
            NodeLeaf::Degree { views, direction } => NodeRead::Field(NodeFieldRead {
                name: (*direction).into(),
                views: read_views(views),
            }),
            NodeLeaf::Property {
                views,
                name,
                temporal: false,
            } => NodeRead::Property(NamedRead {
                name: name.clone(),
                views: read_views(views),
            }),
            NodeLeaf::Property {
                views,
                name,
                temporal: true,
            } => NodeRead::TemporalProperty(NamedRead {
                name: name.clone(),
                views: read_views(views),
            }),
            NodeLeaf::Metadata { views, name } => NodeRead::Metadata(NamedRead {
                name: name.clone(),
                views: read_views(views),
            }),
            NodeLeaf::IsActive { views } => NodeRead::Field(NodeFieldRead {
                name: NodeField::IsActive,
                views: read_views(views),
            }),
        })
    }
}

/// One edge term, read through optional views. Name exactly one of
/// `property`, `temporalProperty`, `metadata`, `field`, `src` or `dst`.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EdgeRead {
    /// The latest value of a property.
    Property(NamedRead),
    /// The history of a property, as a list.
    TemporalProperty(NamedRead),
    /// A metadata entry.
    Metadata(NamedRead),
    /// A built-in edge term.
    Field(EdgeFieldRead),
    /// A node expression evaluated on the edge's source node.
    Src(EndpointRead),
    /// A node expression evaluated on the edge's destination node.
    Dst(EndpointRead),
}

impl Term for EdgeRead {
    type Leaf = EdgeLeaf;

    fn into_leaf(self) -> Result<EdgeLeaf, GraphError> {
        Ok(match self {
            EdgeRead::Property(r) => scoped(EdgeLeaf::property(Vec::new(), r.name, false), r.views),
            EdgeRead::TemporalProperty(r) => {
                scoped(EdgeLeaf::property(Vec::new(), r.name, true), r.views)
            }
            EdgeRead::Metadata(r) => scoped(EdgeLeaf::metadata(Vec::new(), r.name), r.views),
            EdgeRead::Field(r) => scoped(r.name.into(), r.views),
            EdgeRead::Src(r) => scoped(
                EdgeLeaf::Src(Box::new(r.expr.into_inner().into_tree()?)),
                r.views,
            ),
            EdgeRead::Dst(r) => scoped(
                EdgeLeaf::Dst(Box::new(r.expr.into_inner().into_tree()?)),
                r.views,
            ),
        })
    }

    fn from_leaf(leaf: &EdgeLeaf) -> Result<Self, GraphError> {
        Ok(match leaf {
            EdgeLeaf::Property {
                views,
                name,
                temporal: false,
            } => EdgeRead::Property(NamedRead {
                name: name.clone(),
                views: read_views(views),
            }),
            EdgeLeaf::Property {
                views,
                name,
                temporal: true,
            } => EdgeRead::TemporalProperty(NamedRead {
                name: name.clone(),
                views: read_views(views),
            }),
            EdgeLeaf::Metadata { views, name } => EdgeRead::Metadata(NamedRead {
                name: name.clone(),
                views: read_views(views),
            }),
            EdgeLeaf::IsActive { views } => EdgeRead::Field(EdgeFieldRead {
                name: EdgeField::IsActive,
                views: read_views(views),
            }),
            EdgeLeaf::IsValid { views } => EdgeRead::Field(EdgeFieldRead {
                name: EdgeField::IsValid,
                views: read_views(views),
            }),
            EdgeLeaf::IsDeleted { views } => EdgeRead::Field(EdgeFieldRead {
                name: EdgeField::IsDeleted,
                views: read_views(views),
            }),
            EdgeLeaf::IsSelfLoop { views } => EdgeRead::Field(EdgeFieldRead {
                name: EdgeField::IsSelfLoop,
                views: read_views(views),
            }),
            // The endpoint's own terms carry their views; there are none here.
            EdgeLeaf::Src(inner) => EdgeRead::Src(EndpointRead {
                expr: Wrapped::from(NodeExpr::from_tree(inner)?),
                views: None,
            }),
            EdgeLeaf::Dst(inner) => EdgeRead::Dst(EndpointRead {
                expr: Wrapped::from(NodeExpr::from_tree(inner)?),
                views: None,
            }),
        })
    }
}

/// One exploded-edge term (one update of an edge), read through optional
/// views. Name exactly one of `property`, `temporalProperty`, `metadata` or
/// `field`.
#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExplodedEdgeRead {
    /// The latest value of a property.
    Property(NamedRead),
    /// The history of a property, as a list.
    TemporalProperty(NamedRead),
    /// A metadata entry.
    Metadata(NamedRead),
    /// A built-in edge term.
    Field(EdgeFieldRead),
}

impl Term for ExplodedEdgeRead {
    type Leaf = ExplodedEdgeLeaf;

    fn into_leaf(self) -> Result<ExplodedEdgeLeaf, GraphError> {
        Ok(match self {
            ExplodedEdgeRead::Property(r) => scoped(
                ExplodedEdgeLeaf::property(Vec::new(), r.name, false),
                r.views,
            ),
            ExplodedEdgeRead::TemporalProperty(r) => scoped(
                ExplodedEdgeLeaf::property(Vec::new(), r.name, true),
                r.views,
            ),
            ExplodedEdgeRead::Metadata(r) => {
                scoped(ExplodedEdgeLeaf::metadata(Vec::new(), r.name), r.views)
            }
            ExplodedEdgeRead::Field(r) => scoped(r.name.into(), r.views),
        })
    }

    fn from_leaf(leaf: &ExplodedEdgeLeaf) -> Result<Self, GraphError> {
        Ok(match leaf {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal: false,
            } => ExplodedEdgeRead::Property(NamedRead {
                name: name.clone(),
                views: read_views(views),
            }),
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal: true,
            } => ExplodedEdgeRead::TemporalProperty(NamedRead {
                name: name.clone(),
                views: read_views(views),
            }),
            ExplodedEdgeLeaf::Metadata { views, name } => ExplodedEdgeRead::Metadata(NamedRead {
                name: name.clone(),
                views: read_views(views),
            }),
            ExplodedEdgeLeaf::IsActive { views } => ExplodedEdgeRead::Field(EdgeFieldRead {
                name: EdgeField::IsActive,
                views: read_views(views),
            }),
            ExplodedEdgeLeaf::IsValid { views } => ExplodedEdgeRead::Field(EdgeFieldRead {
                name: EdgeField::IsValid,
                views: read_views(views),
            }),
            ExplodedEdgeLeaf::IsDeleted { views } => ExplodedEdgeRead::Field(EdgeFieldRead {
                name: EdgeField::IsDeleted,
                views: read_views(views),
            }),
            ExplodedEdgeLeaf::IsSelfLoop { views } => ExplodedEdgeRead::Field(EdgeFieldRead {
                name: EdgeField::IsSelfLoop,
                views: read_views(views),
            }),
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

/// Stamps `EntityInput` for one entity: the field moves between an entity's
/// expression enum and the shape every entity shares. The conversion itself is
/// written once, in `EntityInput::into_tree` / `from_tree`.
macro_rules! entity_input {
    (
        $expr:ident,
        leaf: $leaf:ident,
        read: $read:ident,
        agg: $agg:ident,
        cmp: $cmp:ident,
        str: $str:ident,
        members: $members:ident,
        presence: $presence:ident,
        quantified: $quantified:ident $(,)?
    ) => {
        impl EntityInput for $expr {
            type Leaf = $leaf;
            type Read = $read;

            fn into_shape(self) -> ExprShape<Self> {
                match self {
                    $expr::Const(v) => ExprShape::Const(v),
                    $expr::Read(read) => ExprShape::Read(read),
                    $expr::Agg($agg { op, expr }) => ExprShape::Agg { op, expr },
                    $expr::Cmp($cmp { op, lhs, rhs }) => ExprShape::Cmp { op, lhs, rhs },
                    $expr::Str($str {
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
                    $expr::IsIn($members { expr, values }) => ExprShape::IsIn { expr, values },
                    $expr::IsNotIn($members { expr, values }) => {
                        ExprShape::IsNotIn { expr, values }
                    }
                    $expr::Presence($presence { op, expr }) => ExprShape::Presence { op, expr },
                    $expr::Quantified($quantified { op, expr }) => {
                        ExprShape::Quantified { op, expr }
                    }
                    $expr::And(items) => ExprShape::And(items),
                    $expr::Or(items) => ExprShape::Or(items),
                    $expr::Not(e) => ExprShape::Not(e),
                }
            }

            fn from_shape(shape: ExprShape<Self>) -> Self {
                match shape {
                    ExprShape::Const(v) => $expr::Const(v),
                    ExprShape::Read(read) => $expr::Read(read),
                    ExprShape::Agg { op, expr } => $expr::Agg($agg { op, expr }),
                    ExprShape::Cmp { op, lhs, rhs } => $expr::Cmp($cmp { op, lhs, rhs }),
                    ExprShape::Str {
                        op,
                        lhs,
                        rhs,
                        levenshtein_distance,
                        prefix_match,
                    } => $expr::Str($str {
                        op,
                        lhs,
                        rhs,
                        levenshtein_distance,
                        prefix_match,
                    }),
                    ExprShape::IsIn { expr, values } => $expr::IsIn($members { expr, values }),
                    ExprShape::IsNotIn { expr, values } => {
                        $expr::IsNotIn($members { expr, values })
                    }
                    ExprShape::Presence { op, expr } => $expr::Presence($presence { op, expr }),
                    ExprShape::Quantified { op, expr } => {
                        $expr::Quantified($quantified { op, expr })
                    }
                    ExprShape::And(items) => $expr::And(items),
                    ExprShape::Or(items) => $expr::Or(items),
                    ExprShape::Not(e) => $expr::Not(e),
                }
            }
        }
    };
}

entity_input!(
    NodeExpr,
    leaf: NodeLeaf,
    read: NodeRead,
    agg: NodeAggregate,
    cmp: NodeComparison,
    str: NodeStringTest,
    members: NodeMembership,
    presence: NodePresence,
    quantified: NodeQuantified,
);

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

entity_input!(
    EdgeExpr,
    leaf: EdgeLeaf,
    read: EdgeRead,
    agg: EdgeAggregate,
    cmp: EdgeComparison,
    str: EdgeStringTest,
    members: EdgeMembership,
    presence: EdgePresence,
    quantified: EdgeQuantified,
);

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

entity_input!(
    ExplodedEdgeExpr,
    leaf: ExplodedEdgeLeaf,
    read: ExplodedEdgeRead,
    agg: ExplodedEdgeAggregate,
    cmp: ExplodedEdgeComparison,
    str: ExplodedEdgeStringTest,
    members: ExplodedEdgeMembership,
    presence: ExplodedEdgePresence,
    quantified: ExplodedEdgeQuantified,
);

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
            expr::Expr::Const(p) => ExprShape::Const(p.into()),
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
                let values = Value::List(values.iter().map(Value::from).collect());
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
        let outside = json!({ "edge": { "read": { "src": {
            "expr": { "read": { "property": { "name": "score" } } },
            "views": [{ "at": 2 }]
        } } } });
        let inside = json!({ "edge": { "read": { "src": {
            "expr": { "read": { "property": { "name": "score", "views": [{ "at": 2 }] } } }
        } } } });
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
            r#"{"edge":{"cmp":{"op":"EQ","lhs":{"read":{"src":{"expr":{"read":{"field":{"name":"NAME"}}}}}},"rhs":{"const":{"str":"alice"}}}}}"#
        );
        let literal = r#"{ edge: { cmp: { op: EQ, lhs: { read: { src: { expr: { read: { field: { name: NAME } } } } } }, rhs: { const: { str: "alice" } } } } }"#;
        assert_eq!(from_literal(literal).await.unwrap(), tree_json(&tree));
    }

    #[tokio::test]
    async fn the_documented_examples_parse() {
        for example in [
            r#"{ node: { cmp: { op: GT, lhs: { read: { property: { name: "score" } } }, rhs: { const: { i64: 4 } } } } }"#,
            r#"{ node: { cmp: { op: EQ, lhs: { read: { metadata: { name: "region" } } }, rhs: { const: { str: "eu" } } } } }"#,
            r#"{ node: { cmp: { op: EQ, lhs: { read: { field: { name: NODE_TYPE } } }, rhs: { const: { str: "user" } } } } }"#,
            r#"{ node: { cmp: { op: GT, lhs: { read: { field: { name: DEGREE } } }, rhs: { read: { field: { name: IN_DEGREE } } } } } }"#,
            r#"{ node: { read: { field: { name: IS_ACTIVE } } } }"#,
            r#"{ node: { str: { op: STARTS_WITH, lhs: { read: { field: { name: NAME } } }, rhs: { const: { str: "al" } } } } }"#,
            r#"{ node: { str: { op: FUZZY, lhs: { read: { field: { name: NAME } } }, rhs: { const: { str: "alise" } }, levenshteinDistance: 1, prefixMatch: false } } }"#,
            r#"{ node: { isIn: { expr: { read: { field: { name: NAME } } }, values: { list: [ { str: "alice" }, { str: "carol" } ] } } } }"#,
            r#"{ node: { presence: { op: IS_NONE, expr: { read: { property: { name: "score" } } } } } }"#,
            r#"{ node: { cmp: { op: GE, lhs: { agg: { op: SUM, expr: { read: { temporalProperty: { name: "score" } } } } }, rhs: { const: { i64: 19 } } } } }"#,
            r#"{ node: { quantified: { op: ANY, expr: { cmp: { op: GT, lhs: { read: { temporalProperty: { name: "score" } } }, rhs: { const: { i64: 8 } } } } } } }"#,
            r#"{ node: { cmp: { op: GT, lhs: { read: { property: { name: "score", views: [ { window: { start: 0, end: 2 } } ] } } }, rhs: { const: { i64: 4 } } } } }"#,
            r#"{ node: { cmp: { op: EQ, lhs: { read: { property: { name: "score", views: [ { window: { start: 0, end: 4 } }, { kind: LATEST } ] } } }, rhs: { const: { i64: 2 } } } } }"#,
            r#"{ node: { read: { field: { name: IS_ACTIVE, views: [ { excludeNodes: ["bob"] }, { kind: LATEST } ] } } } }"#,
            r#"{ edge: { cmp: { op: EQ, lhs: { read: { src: { expr: { read: { field: { name: NAME } } } } } }, rhs: { const: { str: "alice" } } } } }"#,
            r#"{ edge: { cmp: { op: GT, lhs: { read: { src: { expr: { read: { property: { name: "score", views: [ { at: 2 } ] } } } } } }, rhs: { read: { dst: { expr: { read: { property: { name: "score", views: [ { at: 1 } ] } } } } } } } } }"#,
            r#"{ edge: { read: { field: { name: IS_VALID, views: [ { layers: ["work"] } ] } } } }"#,
            r#"{ explodedEdge: { cmp: { op: GT, lhs: { read: { property: { name: "w" } } }, rhs: { const: { f64: 5.0 } } } } }"#,
            r#"{ node: { cmp: { op: GT, lhs: { read: { property: { name: "score", views: [{ window: { start: 0, end: 5 } }] } } }, rhs: { const: { i64: 4 } } } } }"#,
            r#"{ edge: { read: { src: { expr: { cmp: { op: EQ, lhs: { read: { field: { name: NAME } } }, rhs: { const: { str: "alice" } } } } } } } }"#,
            r#"{ view: [ { window: { start: 1, end: 5 } }, { excludeLayers: ["friends"] } ] }"#,
            r#"{ view: [ { subgraph: ["alice", "bob"] }, { kind: VALID } ] }"#,
            r#"{ and: [ { view: [ { window: { start: 1, end: 5 } } ] }, { node: { cmp: { op: GT, lhs: { read: { property: { name: "score" } } }, rhs: { const: { i64: 4 } } } } } ] }"#,
            r#"{ node: { and: [ { presence: { op: IS_SOME, expr: { read: { property: { name: "score" } } } } }, { cmp: { op: EQ, lhs: { read: { field: { name: NODE_TYPE } } }, rhs: { const: { str: "user" } } } } ] } }"#,
        ] {
            if let Err(err) = from_literal(example).await {
                panic!("{example}: {err}");
            }
        }
    }

    #[tokio::test]
    async fn a_read_names_exactly_one_term() {
        for read in [
            // Two terms.
            json!({ "property": { "name": "a" }, "metadata": { "name": "b" } }),
            json!({ "property": { "name": "a" }, "metadata": { "name": "b" }, "field": { "name": "NAME" } }),
            // No term.
            json!({}),
            json!({ "views": [{ "kind": "LATEST" }] }),
            // The flat string form.
            json!({ "property": "a" }),
            // The `term` wrapper.
            json!({ "term": { "property": "a" } }),
            json!({ "term": { "property": { "name": "a" } } }),
        ] {
            let filter = json!({ "node": { "read": read } });
            assert!(from_variable(filter.clone()).await.is_err(), "{filter}");
            assert!(from_json(filter.clone()).is_err(), "{filter}");
        }
        assert!(from_literal(
            r#"{ edge: { read: { field: { name: IS_VALID }, src: { expr: { read: { field: { name: NAME } } } } } } }"#,
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn only_fuzzy_takes_its_arguments() {
        let test = |extra: serde_json::Value, op: &str| {
            let mut s = json!({
                "op": op,
                "lhs": { "read": { "field": { "name": "NAME" } } },
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
