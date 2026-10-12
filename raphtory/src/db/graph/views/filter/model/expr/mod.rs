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
mod split;
pub(crate) mod stream;
#[cfg(test)]
mod tests;

pub use builder::{Chain, EdgeEndpoint, EdgeKind, GraphLeaf, PropertyTerm};
pub use compile::Leaf;
pub(crate) use split::{EdgePredicate, Question, SplitFilter};
pub use stream::{EdgeHistory, NodeHistory, ValueTest};

use super::{
    filter_operator::{BinaryOp, StringOp},
    node_expr::{
        pushdown::{IndexQuery, IndexTerm, Pushdown},
        typing::static_comparison_type,
    },
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
/// type, whether it can be missing, the constant it is, the side a history
/// walk would read, and how a node filter on it can start from an index
/// instead of every node.
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

    /// The value, when this expression is a constant.
    pub(crate) fn constant(&self) -> Option<Prop> {
        match self {
            Expr::Const(value) => Some(value.clone()),
            _ => None,
        }
    }

    /// The term a node index covers, when this expression is one. The latest
    /// update of a history is the property's latest value, so `latest()` of
    /// an indexed history is indexed too.
    pub(crate) fn index_term(&self) -> Option<IndexTerm> {
        match self {
            Expr::Term(leaf) => leaf.index_term(),
            Expr::Agg(Agg::Latest, inner) => inner.index_term()?.latest_value(),
            _ => None,
        }
    }

    /// The test a node index can answer with a candidate set: a comparison,
    /// string test or membership test of an indexed term against a constant.
    pub(crate) fn index_query(&self) -> Option<IndexQuery> {
        match self {
            Expr::Cmp(op, lhs, rhs) => {
                let (term, op, value) = term_and_constant(*op, lhs, rhs)?;
                IndexQuery::cmp(term, op, value)
            }
            Expr::Str(op, lhs, rhs) => IndexQuery::string(lhs.index_term()?, *op, rhs.constant()?),
            Expr::In {
                expr,
                values,
                negated: false,
            } => IndexQuery::members(expr.index_term()?, values),
            _ => None,
        }
    }

    /// Where a node filter on this yes/no expression can start instead of at
    /// every node: the nodes it names by id, or the index candidates for its
    /// test. A history test narrows only under `any()`, a latest-value test
    /// only outside it.
    pub(crate) fn pushdown(&self) -> Option<Pushdown> {
        match self {
            // Equality on the id, or on the name (the node's external id),
            // names the node outright.
            Expr::Cmp(op, lhs, rhs) => {
                if let Some((IndexTerm::Id | IndexTerm::Name, BinaryOp::Eq, value)) =
                    term_and_constant(*op, lhs, rhs)
                {
                    return Some(Pushdown::Ids(vec![value]));
                }
                self.latest_value_pushdown()
            }
            Expr::In {
                expr,
                values,
                negated: false,
            } if matches!(expr.index_term(), Some(IndexTerm::Id | IndexTerm::Name)) => {
                Some(Pushdown::Ids(values.clone()))
            }
            Expr::Str(..) | Expr::In { .. } => self.latest_value_pushdown(),
            // Under `any()` the history test narrows: any value ever held may match.
            Expr::Any(inner) => inner
                .index_query()
                .filter(|query| query.is_history())
                .map(Pushdown::Index),
            // Any leg's candidates are a superset of the matches, so the first
            // leg that can narrow does.
            Expr::And(items) => items.iter().find_map(Self::pushdown),
            _ => None,
        }
    }

    /// For `any()` / `all()` over this expression: the side tested against a
    /// constant and the test to apply to each of its values. The side is
    /// walked when it reads a history (see `Expr::history_op`); otherwise the
    /// test collapses its list as usual.
    pub(crate) fn value_test(&self) -> Option<(&Expr<L>, ValueTest)> {
        match self {
            Expr::Cmp(op, lhs, rhs) => {
                if let Some(value) = rhs.constant() {
                    return Some((lhs, ValueTest::Cmp(*op, value)));
                }
                Some((rhs, ValueTest::Cmp(op.flipped(), lhs.constant()?)))
            }
            Expr::Str(op, lhs, rhs) => Some((lhs, ValueTest::Str(*op, rhs.constant()?))),
            Expr::In {
                expr,
                values,
                negated,
            } => Some((expr, ValueTest::members(values.iter().cloned(), *negated))),
            _ => None,
        }
    }

    /// The index candidates for a test of a term's latest value.
    fn latest_value_pushdown(&self) -> Option<Pushdown> {
        self.index_query()
            .filter(|query| !query.is_history())
            .map(Pushdown::Index)
    }
}

/// The indexed term and the constant it is compared with, the operator turned
/// round when the constant stands on the left.
fn term_and_constant<L: Leaf>(
    op: BinaryOp,
    lhs: &Expr<L>,
    rhs: &Expr<L>,
) -> Option<(IndexTerm, BinaryOp, Prop)> {
    if let (Some(term), Some(value)) = (lhs.index_term(), rhs.constant()) {
        return Some((term, op, value));
    }
    if let (Some(term), Some(value)) = (rhs.index_term(), lhs.constant()) {
        return Some((term, op.flipped(), value));
    }
    None
}

/// The filter itself: a yes/no on one kind of entity, a view, or a
/// combination of filters.
///
/// A filtered graph answers two questions, which nodes stay and which edges
/// stay. A node predicate answers the first directly and the second by "both
/// ends stayed"; an edge predicate the reverse. `And`, `Or` and `Not` combine
/// the direct answers question by question: `name == "b" | name == "c"` keeps
/// the edge b→c, `Not` of a node predicate keeps the nodes that fail it and the
/// edges between them, `Not(And(node, edge))` negates each answer, and an `Or`
/// between a node predicate and an edge predicate keeps every node and the
/// edges either leg keeps: the union of the two.
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
