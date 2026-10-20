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
pub mod typing;

pub use builder::{Chain, EdgeEndpoint, EdgeKind, GraphLeaf, PropertyTerm};
pub(super) use compile::EdgeOps;
pub use compile::Leaf;

use super::{
    filter_operator::{BinaryOp, StringOp, UnaryOp},
    validate_const_comparable,
};
use crate::{db::api::state::NodeOp, errors::GraphError};
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropType},
        Layer, GID,
    },
    storage::timeindex::EventTime,
    Direction,
};
use std::{fmt, sync::Arc};
use typing::{
    comparison_shape, is_known, qualified_type, static_aggregate_type, static_comparison_type,
    static_qualified_type, static_set_type, static_string_type, string_shape,
};

/// One view restriction, in the order it was applied. Each one is the graph
/// view method of the same name applied to the view built so far.
#[derive(Clone, Debug, PartialEq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

impl Agg {
    /// The aggregate as its method is called: `sum`, `avg` and so on.
    pub fn name(&self) -> &'static str {
        match self {
            Agg::Sum => "sum",
            Agg::Avg => "avg",
            Agg::Min => "min",
            Agg::Max => "max",
            Agg::First => "first",
            Agg::Last => "last",
            Agg::Len => "len",
            Agg::Earliest => "earliest",
            Agg::Latest => "latest",
        }
    }
}

/// Which elements of an element-wise boolean result must hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Qualifier {
    /// At least one.
    Any,
    /// Every one.
    All,
}

/// A node value already built in this process, carried inside a tree as a
/// node term: a value over in-process state (a node-state column). It runs
/// but has no wire form: asking for one is an error, not a guess.
#[derive(Clone)]
pub struct OpaqueFilter {
    op: Arc<dyn NodeOp<Output = Option<Prop>>>,
    nullable: bool,
}

pub const OPAQUE_FILTER_ERROR: &str =
    "this filter has no server-side form; it was built from in-process state";

impl OpaqueFilter {
    /// The compiled `op`, and whether its value can be missing.
    pub fn new(op: impl NodeOp<Output = Option<Prop>> + 'static, nullable: bool) -> Self {
        OpaqueFilter {
            op: Arc::new(op),
            nullable,
        }
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
        Arc::ptr_eq(&self.op, &other.op)
    }
}

/// What every entity can do with a value, whatever terms the entity offers.
#[derive(Clone, Debug, PartialEq)]
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
    Term(L),
}

/// What a node offers: the surface of `NodeFilter`.
#[derive(Clone, Debug, PartialEq)]
pub enum NodeLeaf {
    Field {
        views: Vec<ViewOp>,
        field: Field,
    },
    Degree {
        views: Vec<ViewOp>,
        direction: Direction,
    },
    Property {
        views: Vec<ViewOp>,
        name: String,
        /// The property's history as a list instead of its latest value.
        temporal: bool,
    },
    Metadata {
        views: Vec<ViewOp>,
        name: String,
    },
    IsActive {
        views: Vec<ViewOp>,
    },
    Exists {
        views: Vec<ViewOp>,
    },
    /// A node value built in this process (a node-state column) that has no
    /// wire form: it runs where it was built and cannot be sent anywhere.
    Opaque(OpaqueFilter),
}

/// What an edge offers: the surface of `EdgeFilter`, plus a look at either
/// endpoint node.
#[derive(Clone, Debug, PartialEq)]
pub enum EdgeLeaf {
    Property {
        views: Vec<ViewOp>,
        name: String,
        temporal: bool,
    },
    Metadata {
        views: Vec<ViewOp>,
        name: String,
    },
    IsActive {
        views: Vec<ViewOp>,
    },
    Exists {
        views: Vec<ViewOp>,
    },
    IsValid {
        views: Vec<ViewOp>,
    },
    IsDeleted {
        views: Vec<ViewOp>,
    },
    IsSelfLoop {
        views: Vec<ViewOp>,
    },
    /// A node expression evaluated on the edge's source node.
    Src(Box<NodeExpr>),
    /// A node expression evaluated on the edge's destination node.
    Dst(Box<NodeExpr>),
}

/// What an exploded edge offers: the surface of `ExplodedEdgeFilter`.
#[derive(Clone, Debug, PartialEq)]
pub enum ExplodedEdgeLeaf {
    Property {
        views: Vec<ViewOp>,
        name: String,
        temporal: bool,
    },
    Metadata {
        views: Vec<ViewOp>,
        name: String,
    },
    IsActive {
        views: Vec<ViewOp>,
    },
    Exists {
        views: Vec<ViewOp>,
    },
    IsValid {
        views: Vec<ViewOp>,
    },
    IsDeleted {
        views: Vec<ViewOp>,
    },
    IsSelfLoop {
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
    /// The result type, as far as it is known before the filter meets a
    /// graph. A test (`is_some()`, `is_none()`, `and`, `or`, `not`) is always
    /// one boolean. A comparison, a string or membership test, an aggregate
    /// and `any()`/`all()` have the type their rule gives once their operands
    /// are fully known; until then they are `Empty`. A term answers for
    /// itself: a temporal property is a list whose elements are not yet known
    /// (`List<Empty>`).
    pub(crate) fn prop_type(&self) -> PropType {
        match self {
            Expr::Const(value) => value.dtype(),
            Expr::Term(leaf) => leaf.prop_type(),
            Expr::Cmp(op, lhs, rhs) => {
                static_comparison_type(op, &lhs.prop_type(), &rhs.prop_type())
            }
            Expr::Str(_, lhs, rhs) => static_string_type(&lhs.prop_type(), &rhs.prop_type()),
            Expr::In { expr, values, .. } => static_set_type(&expr.prop_type(), values),
            Expr::Agg(agg, inner) => static_aggregate_type(*agg, &inner.prop_type()),
            Expr::Any(inner) | Expr::All(inner) => static_qualified_type(&inner.prop_type()),
            Expr::IsSome(_) | Expr::IsNone(_) | Expr::And(_) | Expr::Or(_) | Expr::Not(_) => {
                PropType::Bool
            }
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

    /// Refuses `value` against this expression under `op` when the expression's type is
    /// already known and can never be compared with it, element by element for a list; an
    /// expression whose type only a graph knows defers to filter time.
    pub fn check_value(&self, op: BinaryOp, value: &Prop) -> Result<(), GraphError> {
        let pt = self.prop_type();
        if is_known(&pt) {
            let rhs = value.dtype();
            comparison_shape(&op, &pt, &rhs).map_err(|e| e.into_error(&rhs, Some(value)))?;
        }
        Ok(())
    }

    /// Refuses `value` as the operand of a string operator on this expression by the
    /// string rule, which asks for a string operand whatever the left side and, when
    /// the expression's type is already known, for a string on the left as well.
    pub fn check_string(&self, value: &Prop) -> Result<(), GraphError> {
        let rhs = value.dtype();
        string_shape(&self.prop_type(), &rhs).map_err(|e| e.into_error(&rhs, Some(value)))?;
        Ok(())
    }

    /// Refuses the presence test `op` on an expression that can never be missing.
    pub fn check_nullable(&self, op: UnaryOp) -> Result<(), GraphError> {
        if !self.nullable() {
            return Err(GraphError::invalid_filter(format!(
                "{op}() is not valid on an expression that always has a value"
            )));
        }
        Ok(())
    }

    /// Refuses `any()`/`all()` after this expression when its type is already known and is
    /// not one boolean per element; an expression whose type only a graph knows defers.
    pub fn check_qualifiable(&self) -> Result<(), GraphError> {
        let pt = self.prop_type();
        if is_known(&pt) {
            qualified_type(&pt)?;
        }
        Ok(())
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

/// The filter itself: a boolean on one kind of entity, a view, or a
/// combination of filters.
///
/// A filter is a graph: a node predicate keeps the nodes that pass it, an edge
/// predicate the edges, and a view is the graph seen through it. The expressions of
/// `And` and `Or` are each read on the same base graph and their graphs
/// combine; `Not` keeps what its expression drops. A node or an edge stays in a
/// collection when it exists in that graph.
#[derive(Clone, Debug, PartialEq)]
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
    /// Refuses a view expression with no view in it: it is not "no restriction".
    pub fn needs_view(views: &[ViewOp]) -> Result<(), GraphError> {
        if views.is_empty() {
            return Err(GraphError::invalid_filter(
                "a view filter needs at least one view",
            ));
        }
        Ok(())
    }

    /// The first expression of `and` or `or` (`name`) and the rest, of a filter or
    /// of an expression. An `and` or `or` with no expressions is refused.
    pub fn needs_operand<'a, T>(
        name: &str,
        expressions: &'a [T],
    ) -> Result<(&'a T, &'a [T]), GraphError> {
        expressions.split_first().ok_or_else(|| {
            GraphError::invalid_filter(format!("`{name}` needs at least one operand"))
        })
    }

    /// Refuses a view in this expression of `or` or `not`: the union with a view, or
    /// the complement of one, waits until views compose with time semantics.
    pub(crate) fn no_view_below_top_level(&self) -> Result<(), GraphError> {
        if self.has_view() {
            return Err(GraphError::invalid_filter(
                "a view applies to the whole filter: use it alone or as an expression of the top-level `and`, \
                 not under `or` or `not`; to test that an entity does not exist in a view, negate `.exists()`",
            ));
        }
        Ok(())
    }

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
