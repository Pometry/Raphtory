//! A filter as data, one expression type per entity.
//!
//! The rust builders (`NodeFilter.property("score").gt(4)`), the python
//! bindings, a GraphQL request and a stored permission grant all write the
//! same filter as plain data. These enums are that data.
//!
//! [`Expr`] holds what every entity can do: constants, aggregates,
//! comparisons, string and set tests, presence tests, `any`/`all` over an
//! element-wise result, and `and`/`or`/`not`. The *terms* an entity offers
//! make up its leaf type: [`NodeLeaf`] mirrors `NodeFilter`, [`EdgeLeaf`]
//! mirrors `EdgeFilter` (with `src`/`dst` holding a node expression), and
//! [`ExplodedEdgeLeaf`] mirrors `ExplodedEdgeFilter`. A combination the API
//! does not have cannot be written down, so it need not be rejected.
//!
//! Every expression has a result type. A comparison of two comparable values
//! is a `Bool`; a comparison of a list-valued side against a value its
//! elements are comparable with is a `List<Bool>`, one answer per element,
//! which `Any` or `All` turn into a `Bool`. A [`FilterExpr`] accepts an
//! expression only when it is a `Bool`. Property types are known only once a
//! graph is at hand, so that check runs when the filter is built against a
//! graph.
//!
//! Applying a tree compiles it straight to the ops that evaluate it; see
//! [`compile`].

pub mod builder;
mod compile;
mod display;
#[cfg(test)]
mod tests;

pub use builder::{Chain, EdgeEndpoint, EdgeKind, GraphLeaf, PropertyTerm};
pub use compile::Leaf;

use super::{
    filter_operator::{BinaryOp, StringOp},
    node_expr::typing::static_comparison_type,
};
use crate::{
    db::api::{state::NodeOp, view::internal::DynGraphArc},
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropType},
        Layer, GID,
    },
    storage::timeindex::EventTime,
    Direction,
};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

/// One view restriction, in the order it was applied. Each one is the graph
/// view method of the same name applied to the view built so far.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewOp {
    /// `window(start, end)`.
    Window { start: EventTime, end: EventTime },
    /// `at(t)`.
    At(EventTime),
    /// `after(t)`.
    After(EventTime),
    /// `before(t)`.
    Before(EventTime),
    /// `latest()`.
    Latest,
    /// `snapshot_at(t)`.
    SnapshotAt(EventTime),
    /// `snapshot_latest()`.
    SnapshotLatest,
    /// `layers(layer)`.
    Layers(Layer),
    /// `default_layer()`.
    DefaultLayer,
    /// `exclude_layers(layer)`.
    ExcludeLayers(Layer),
    /// `valid_layers(layer)`: the named layers; a name the graph does not
    /// have is ignored, where `layers` errors on it.
    ValidLayers(Layer),
    /// `exclude_valid_layers(layer)`: every layer but the named ones; a name
    /// the graph does not have is ignored, where `exclude_layers` errors on it.
    ExcludeValidLayers(Layer),
    /// `shrink_start(t)`: the later of `t` and the current start.
    ShrinkStart(EventTime),
    /// `shrink_end(t)`: the earlier of `t` and the current end.
    ShrinkEnd(EventTime),
    /// `exclude_nodes(ids)`: every node but the named ones; an id the view
    /// does not hold changes nothing.
    ExcludeNodes(Vec<GID>),
    /// `subgraph(ids)`: the named nodes alone; an id the view does not hold
    /// is skipped.
    Subgraph(Vec<GID>),
    /// `subgraph_node_types(types)`: the nodes of the named types alone.
    SubgraphNodeTypes(Vec<String>),
    /// `valid()`: only the edges that are valid in the view (on a persistent
    /// graph, whose last update is an addition); nodes are untouched.
    Valid,
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
///
/// `Sum`, `Avg`, `Min`, `Max`, `First`, `Last` and `Len` reduce the innermost
/// lists: on a history of list values they answer per update. `Earliest` and
/// `Latest` pick one update of a temporal history, whatever its type.
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
    Earliest,
    Latest,
}

/// Which elements of an element-wise yes/no result must hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Quantifier {
    /// At least one.
    Any,
    /// Every one.
    All,
}

/// A node value built in this process, from in-process state (a node-state
/// column) rather than from data: what an [`OpaqueFilter`] carries.
pub trait OpaqueOp: Send + Sync + 'static {
    /// The op that reads the value per node against `graph`.
    fn node_value_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError>;

    /// The type of the value, when it is fixed whatever the graph.
    fn prop_type(&self) -> PropType;

    /// Whether the value can be missing.
    fn nullable(&self) -> bool;
}

/// A node value already built in this process, carried inside a tree as a
/// node term: a value over in-process state (a node-state column). It runs
/// but does not serialise: asking for its wire form is an error, not a guess.
#[derive(Clone)]
pub struct OpaqueFilter(pub Arc<dyn OpaqueOp>);

pub const OPAQUE_FILTER_ERROR: &str =
    "this filter has no server-side form; it was built from in-process state";

impl OpaqueFilter {
    pub fn new<E: OpaqueOp>(expr: E) -> Self {
        OpaqueFilter(Arc::new(expr))
    }

    /// The filter this value is: a predicate on nodes.
    pub fn into_filter(self) -> FilterExpr {
        FilterExpr::Node(Expr::Term(NodeLeaf::Opaque(self)))
    }
}

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

/// What every entity can do with a value, whatever terms the entity offers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expr<L> {
    Const(Prop),
    Agg(Agg, Box<Expr<L>>),
    Cmp(BinaryOp, Box<Expr<L>>, Box<Expr<L>>),
    Str(StringOp, Box<Expr<L>>, Box<Expr<L>>),
    In {
        expr: Box<Expr<L>>,
        values: Vec<Prop>,
        negated: bool,
    },
    IsSome(Box<Expr<L>>),
    IsNone(Box<Expr<L>>),
    /// Holds when the element-wise result inside holds for any element.
    Any(Box<Expr<L>>),
    /// Holds when the element-wise result inside holds for every element.
    All(Box<Expr<L>>),
    And(Vec<Expr<L>>),
    Or(Vec<Expr<L>>),
    Not(Box<Expr<L>>),
    /// A term the entity offers; see [`NodeLeaf`], [`EdgeLeaf`], [`ExplodedEdgeLeaf`].
    /// Serialised as the leaf itself, so the term's name is the key.
    #[serde(untagged)]
    Term(L),
}

/// What a node offers: the surface of `NodeFilter`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeLeaf {
    Field {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        field: Field,
    },
    Degree {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        direction: Direction,
    },
    Property {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
        /// The property's history as a list instead of its latest value.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        temporal: bool,
    },
    Metadata {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
    },
    IsActive {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    /// A node value built in this process (a node-state column) that has no
    /// wire form: it runs where it was built and cannot be sent anywhere.
    Opaque(OpaqueFilter),
}

/// What an edge offers: the surface of `EdgeFilter`, plus a look at either
/// endpoint node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeLeaf {
    Property {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        temporal: bool,
    },
    Metadata {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
    },
    IsActive {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsValid {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsDeleted {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsSelfLoop {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    /// A node expression evaluated on the edge's source node.
    Src(Box<NodeExpr>),
    /// A node expression evaluated on the edge's destination node.
    Dst(Box<NodeExpr>),
}

/// What an exploded edge offers: the surface of `ExplodedEdgeFilter`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplodedEdgeLeaf {
    Property {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        temporal: bool,
    },
    Metadata {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
        name: String,
    },
    IsActive {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsValid {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsDeleted {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
    IsSelfLoop {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        views: Vec<ViewOp>,
    },
}

pub type NodeExpr = Expr<NodeLeaf>;
pub type EdgeExpr = Expr<EdgeLeaf>;
pub type ExplodedEdgeExpr = Expr<ExplodedEdgeLeaf>;

/// What a tree knows about itself before any graph is at hand: its result
/// type and whether it can be missing.
/// Each answer is read off the tree; a term answers for itself through its
/// [`Leaf`].
impl<L: Leaf> Expr<L> {
    /// The result type, when it is known before the filter meets a graph: a
    /// constant's, a term's, or one yes/no (or one per element) for a
    /// comparison of known types. Anything else is `Empty`.
    pub(crate) fn prop_type(&self) -> PropType {
        match self {
            Expr::Const(value) => value.dtype(),
            Expr::Term(leaf) => leaf.prop_type(),
            Expr::Cmp(op, lhs, rhs) => {
                static_comparison_type(op, &lhs.prop_type(), &rhs.prop_type())
            }
            Expr::Agg(..)
            | Expr::Str(..)
            | Expr::In { .. }
            | Expr::IsSome(_)
            | Expr::IsNone(_)
            | Expr::Any(_)
            | Expr::All(_)
            | Expr::And(_)
            | Expr::Or(_)
            | Expr::Not(_) => PropType::Empty,
        }
    }

    /// Whether the value can be missing. A test and a constant always
    /// answer; an aggregate, `any()`/`all()` and most terms may not have a
    /// value.
    pub(crate) fn nullable(&self) -> bool {
        match self {
            Expr::Term(leaf) => leaf.nullable(),
            Expr::Agg(..) | Expr::Any(_) | Expr::All(_) => true,
            Expr::Const(_)
            | Expr::Cmp(..)
            | Expr::Str(..)
            | Expr::In { .. }
            | Expr::IsSome(_)
            | Expr::IsNone(_)
            | Expr::And(_)
            | Expr::Or(_)
            | Expr::Not(_) => false,
        }
    }
}

impl<L> Expr<L> {
    /// Whether this tree is built only from constants. Such a tree answers
    /// the same for every entity, so it is not a filter.
    fn is_constant(&self) -> bool {
        match self {
            Expr::Const(_) => true,
            Expr::Not(inner)
            | Expr::Agg(_, inner)
            | Expr::Any(inner)
            | Expr::All(inner)
            | Expr::IsSome(inner)
            | Expr::IsNone(inner) => inner.is_constant(),
            Expr::Cmp(_, lhs, rhs) | Expr::Str(_, lhs, rhs) => {
                lhs.is_constant() && rhs.is_constant()
            }
            Expr::In { expr, .. } => expr.is_constant(),
            Expr::And(items) | Expr::Or(items) => {
                !items.is_empty() && items.iter().all(Self::is_constant)
            }
            Expr::Term(_) => false,
        }
    }

    pub(crate) fn refuse_constant(&self) -> Result<(), GraphError> {
        if self.is_constant() {
            return Err(GraphError::invalid_filter("a constant is not a filter"));
        }
        Ok(())
    }
}

/// The filter itself: a yes/no on one kind of entity, a view, or a
/// combination of filters.
///
/// A filter is a graph: a node predicate keeps the nodes that pass it, an edge
/// predicate the edges, and a view is the graph seen through it. The legs of
/// `And` and `Or` are each read on the same base graph and their graphs
/// combine; `Not` keeps what its leg drops. A node or an edge stays in a
/// collection when it exists in that graph.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterExpr {
    Node(NodeExpr),
    Edge(EdgeExpr),
    ExplodedEdge(ExplodedEdgeExpr),
    /// A graph-level view with no predicate: the result *is* the view.
    View(Vec<ViewOp>),
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    /// The filter that keeps what the inner one drops, one question at a time.
    Not(Box<FilterExpr>),
}

impl FilterExpr {
    /// Whether a graph-level view appears anywhere in this filter.
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
        match self {
            FilterExpr::Edge(_) | FilterExpr::ExplodedEdge(_) => true,
            FilterExpr::Node(_) | FilterExpr::View(_) => false,
            FilterExpr::And(items) | FilterExpr::Or(items) => items.iter().any(Self::tests_edges),
            FilterExpr::Not(inner) => inner.tests_edges(),
        }
    }
}
