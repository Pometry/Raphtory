//! How a filter is written in Rust.
//!
//! Every builder here produces the filter tree ([`Expr`], [`FilterExpr`]):
//! the same data the python bindings build and the GraphQL wire type carries,
//! compiled by the one compiler in `compile.rs`. Nothing a user writes
//! constructs a typed expression directly; the tree does, once, from data.
//!
//! ```rust,ignore
//! NodeFilter.property("score").gt(4)                       // Expr<NodeLeaf>
//! NodeFilter.window(1, 5).property("score").temporal().any()
//! EdgeFilter::src().name().eq("alice")                     // Expr<EdgeLeaf>
//! NodeFilter.name().eq("b").and(EdgeFilter.is_valid())     // FilterExpr
//! GraphFilter.window(1, 5).layer("a")                      // a view, FilterExpr::View
//! ```
//!
//! Each method set exists once, as a trait with default bodies over
//! `Into<…>`, and is implemented by the types that offer it: a root and the
//! chain it becomes after a view, or an expression and a property term.

use crate::{
    db::{
        api::state::{NodeStateValue, TypedNodeState},
        graph::views::filter::model::{
            edge_filter::{EdgeFilter, Endpoint},
            exploded_edge_filter::ExplodedEdgeFilter,
            expr::{
                Agg, EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, FilterExpr, Leaf, NodeLeaf,
                OpaqueFilter, ViewOp,
            },
            filter_operator::{BinaryOp, StringOp},
            graph_filter::GraphFilter,
            node_filter::NodeFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, Layer, GID},
    utils::time::IntoTime,
    Direction,
};
use std::marker::PhantomData;

// ── chains ───────────────────────────────────────────────────────────────────

/// A root with views applied: `NodeFilter.window(1, 5)`. The views scope every
/// term built from it. `L` is the leaf type of the entity, or `()` for a view
/// on the graph itself.
#[derive(Clone, Debug, PartialEq)]
pub struct Chain<L> {
    views: Vec<ViewOp>,
    leaf: PhantomData<L>,
}

impl<L> Chain<L> {
    fn new(views: Vec<ViewOp>) -> Self {
        Chain {
            views,
            leaf: PhantomData,
        }
    }

    /// The views applied so far, innermost first.
    pub fn views(&self) -> &[ViewOp] {
        &self.views
    }

    fn push(mut self, op: Option<ViewOp>) -> Self {
        self.views.extend(op);
        self
    }
}

impl From<NodeFilter> for Chain<NodeLeaf> {
    fn from(_: NodeFilter) -> Self {
        Chain::new(Vec::new())
    }
}

impl From<EdgeFilter> for Chain<EdgeLeaf> {
    fn from(_: EdgeFilter) -> Self {
        Chain::new(Vec::new())
    }
}

impl From<ExplodedEdgeFilter> for Chain<ExplodedEdgeLeaf> {
    fn from(_: ExplodedEdgeFilter) -> Self {
        Chain::new(Vec::new())
    }
}

impl From<GraphFilter> for Chain<()> {
    fn from(_: GraphFilter) -> Self {
        Chain::new(Vec::new())
    }
}

/// A view on the graph itself is a filter: the graph seen through it.
impl From<Chain<()>> for FilterExpr {
    fn from(chain: Chain<()>) -> Self {
        FilterExpr::View(chain.views)
    }
}

/// The view op a layer restriction is; `Layer::All` restricts nothing.
fn layer_view(layer: Layer) -> Option<ViewOp> {
    let names = match layer {
        Layer::All => return None,
        Layer::None => Vec::new(),
        Layer::Default => vec!["_default".to_string()],
        Layer::One(name) => vec![name.to_string()],
        Layer::Multiple(names) => names.iter().map(ToString::to_string).collect(),
    };
    Some(ViewOp::Layers(names))
}

/// The views: each one scopes what is built after it. Implemented for the four
/// roots and for the chain they become.
pub trait ViewWrapOps<L>: Into<Chain<L>> + Sized {
    fn window<S: IntoTime, E: IntoTime>(self, start: S, end: E) -> Chain<L> {
        self.into().push(Some(ViewOp::Window {
            start: start.into_time(),
            end: end.into_time(),
        }))
    }

    fn at<T: IntoTime>(self, time: T) -> Chain<L> {
        self.into().push(Some(ViewOp::At(time.into_time())))
    }

    fn after<T: IntoTime>(self, time: T) -> Chain<L> {
        self.into().push(Some(ViewOp::After(time.into_time())))
    }

    fn before<T: IntoTime>(self, time: T) -> Chain<L> {
        self.into().push(Some(ViewOp::Before(time.into_time())))
    }

    fn latest(self) -> Chain<L> {
        self.into().push(Some(ViewOp::Latest))
    }

    fn snapshot_at<T: IntoTime>(self, time: T) -> Chain<L> {
        self.into().push(Some(ViewOp::SnapshotAt(time.into_time())))
    }

    fn snapshot_latest(self) -> Chain<L> {
        self.into().push(Some(ViewOp::SnapshotLatest))
    }

    fn layer<Ly: Into<Layer>>(self, layer: Ly) -> Chain<L> {
        self.into().push(layer_view(layer.into()))
    }

    fn default_layer(self) -> Chain<L> {
        self.into().push(Some(ViewOp::DefaultLayer))
    }

    fn exclude_layer(self, layer: impl Into<String>) -> Chain<L> {
        self.into()
            .push(Some(ViewOp::ExcludeLayers(vec![layer.into()])))
    }

    fn exclude_layers<I, S>(self, layers: I) -> Chain<L>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let names = layers.into_iter().map(Into::into).collect();
        self.into().push(Some(ViewOp::ExcludeLayers(names)))
    }

    fn shrink_start<T: IntoTime>(self, start: T) -> Chain<L> {
        self.into()
            .push(Some(ViewOp::ShrinkStart(start.into_time())))
    }

    fn shrink_end<T: IntoTime>(self, end: T) -> Chain<L> {
        self.into().push(Some(ViewOp::ShrinkEnd(end.into_time())))
    }

    fn exclude_nodes<I, V>(self, nodes: I) -> Chain<L>
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        let ids = nodes.into_iter().map(Into::into).collect();
        self.into().push(Some(ViewOp::ExcludeNodes(ids)))
    }

    fn subgraph<I, V>(self, nodes: I) -> Chain<L>
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        let ids = nodes.into_iter().map(Into::into).collect();
        self.into().push(Some(ViewOp::Subgraph(ids)))
    }

    fn subgraph_node_types<I, S>(self, node_types: I) -> Chain<L>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let types = node_types.into_iter().map(Into::into).collect();
        self.into().push(Some(ViewOp::SubgraphNodeTypes(types)))
    }

    fn valid(self) -> Chain<L> {
        self.into().push(Some(ViewOp::Valid))
    }
}

impl<L> ViewWrapOps<L> for Chain<L> {}
impl ViewWrapOps<NodeLeaf> for NodeFilter {}
impl ViewWrapOps<EdgeLeaf> for EdgeFilter {}
impl ViewWrapOps<ExplodedEdgeLeaf> for ExplodedEdgeFilter {}
impl ViewWrapOps<()> for GraphFilter {}

// ── terms ────────────────────────────────────────────────────────────────────

/// A property term: its latest value, or its history after `temporal()`.
/// Both forms are built up front, the way the python bindings do, so the
/// term can switch without rebuilding its views.
#[derive(Clone, Debug, PartialEq)]
pub struct PropertyTerm<L: Leaf> {
    latest: Expr<L>,
    history: Expr<L>,
}

impl<L: Leaf> PropertyTerm<L> {
    fn new(term: impl Fn(bool) -> Expr<L>) -> Self {
        PropertyTerm {
            latest: term(false),
            history: term(true),
        }
    }

    /// The property's history: one value per update, inside the views.
    pub fn temporal(&self) -> Expr<L> {
        self.history.clone()
    }
}

impl<L: Leaf> From<PropertyTerm<L>> for Expr<L> {
    fn from(term: PropertyTerm<L>) -> Self {
        term.latest
    }
}

/// Property and metadata terms on an entity. Implemented for the three entity
/// roots and their chains.
pub trait PropertyExprFactory<L: Leaf>: Into<Chain<L>> + Clone {
    fn property(&self, name: impl Into<String>) -> PropertyTerm<L> {
        let views = self.clone().into().views;
        let name = name.into();
        PropertyTerm::new(|temporal| Expr::Term(L::property(views.clone(), name.clone(), temporal)))
    }

    fn metadata(&self, name: impl Into<String>) -> Expr<L> {
        Expr::Term(L::metadata(self.clone().into().views, name.into()))
    }
}

impl<L: Leaf> PropertyExprFactory<L> for Chain<L> {}
impl PropertyExprFactory<NodeLeaf> for NodeFilter {}
impl PropertyExprFactory<EdgeLeaf> for EdgeFilter {}
impl PropertyExprFactory<ExplodedEdgeLeaf> for ExplodedEdgeFilter {}

/// The terms only a node offers. Implemented for `NodeFilter` and its chain.
pub trait NodeFilterFactory: Into<Chain<NodeLeaf>> + Clone {
    fn id(&self) -> Expr<NodeLeaf> {
        self.field(Field::Id)
    }

    fn name(&self) -> Expr<NodeLeaf> {
        self.field(Field::Name)
    }

    fn node_type(&self) -> Expr<NodeLeaf> {
        self.field(Field::NodeType)
    }

    fn field(&self, field: Field) -> Expr<NodeLeaf> {
        Expr::Term(NodeLeaf::Field {
            views: self.clone().into().views,
            field,
        })
    }

    fn degree(&self) -> Expr<NodeLeaf> {
        self.degree_in(Direction::BOTH)
    }

    fn in_degree(&self) -> Expr<NodeLeaf> {
        self.degree_in(Direction::IN)
    }

    fn out_degree(&self) -> Expr<NodeLeaf> {
        self.degree_in(Direction::OUT)
    }

    fn degree_in(&self, direction: Direction) -> Expr<NodeLeaf> {
        Expr::Term(NodeLeaf::Degree {
            views: self.clone().into().views,
            direction,
        })
    }

    /// Whether the node has an event inside the views.
    fn is_active(&self) -> Expr<NodeLeaf> {
        Expr::Term(NodeLeaf::is_active(self.clone().into().views))
    }

    /// The nodes a boolean column of a node state marks. Built from
    /// in-process state, so it has no wire form and travels opaque.
    fn by_column<'graph, V, G, T>(
        state: &TypedNodeState<'graph, V, G, T>,
        col: &str,
    ) -> Result<FilterExpr, GraphError>
    where
        V: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
    {
        Ok(FilterExpr::Opaque(OpaqueFilter::new(
            state.bool_col_filter(col)?,
        )))
    }
}

impl NodeFilterFactory for NodeFilter {}
impl NodeFilterFactory for Chain<NodeLeaf> {}

/// The unit tests an edge offers, beyond `is_active`. Implemented by the two
/// edge leaf types.
pub trait EdgeKind: Leaf {
    fn is_valid(views: Vec<ViewOp>) -> Self;
    fn is_deleted(views: Vec<ViewOp>) -> Self;
    fn is_self_loop(views: Vec<ViewOp>) -> Self;
}

impl EdgeKind for EdgeLeaf {
    fn is_valid(views: Vec<ViewOp>) -> Self {
        EdgeLeaf::IsValid { views }
    }

    fn is_deleted(views: Vec<ViewOp>) -> Self {
        EdgeLeaf::IsDeleted { views }
    }

    fn is_self_loop(views: Vec<ViewOp>) -> Self {
        EdgeLeaf::IsSelfLoop { views }
    }
}

impl EdgeKind for ExplodedEdgeLeaf {
    fn is_valid(views: Vec<ViewOp>) -> Self {
        ExplodedEdgeLeaf::IsValid { views }
    }

    fn is_deleted(views: Vec<ViewOp>) -> Self {
        ExplodedEdgeLeaf::IsDeleted { views }
    }

    fn is_self_loop(views: Vec<ViewOp>) -> Self {
        ExplodedEdgeLeaf::IsSelfLoop { views }
    }
}

/// The unit tests of an edge or exploded edge, inside the chain's views.
/// Implemented for the two edge roots and their chains.
pub trait EdgeViewFilterOps<L: EdgeKind>: Into<Chain<L>> + Clone {
    fn is_active(&self) -> Expr<L> {
        Expr::Term(L::is_active(self.clone().into().views))
    }

    fn is_valid(&self) -> Expr<L> {
        Expr::Term(L::is_valid(self.clone().into().views))
    }

    fn is_deleted(&self) -> Expr<L> {
        Expr::Term(L::is_deleted(self.clone().into().views))
    }

    fn is_self_loop(&self) -> Expr<L> {
        Expr::Term(L::is_self_loop(self.clone().into().views))
    }
}

impl<L: EdgeKind> EdgeViewFilterOps<L> for Chain<L> {}
impl EdgeViewFilterOps<EdgeLeaf> for EdgeFilter {}
impl EdgeViewFilterOps<ExplodedEdgeLeaf> for ExplodedEdgeFilter {}

// ── endpoints ────────────────────────────────────────────────────────────────

/// One end of an edge: node terms read on the node at that end, so a filter on
/// edges can ask about their source or destination.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeEndpoint {
    views: Vec<ViewOp>,
    endpoint: Endpoint,
}

impl EdgeFilter {
    pub fn src() -> EdgeEndpoint {
        EdgeEndpoint {
            views: Vec::new(),
            endpoint: Endpoint::Src,
        }
    }

    pub fn dst() -> EdgeEndpoint {
        EdgeEndpoint {
            views: Vec::new(),
            endpoint: Endpoint::Dst,
        }
    }
}

impl Chain<EdgeLeaf> {
    /// The source node, read inside this chain's views.
    pub fn src(&self) -> EdgeEndpoint {
        EdgeEndpoint {
            views: self.views.clone(),
            endpoint: Endpoint::Src,
        }
    }

    /// The destination node, read inside this chain's views.
    pub fn dst(&self) -> EdgeEndpoint {
        EdgeEndpoint {
            views: self.views.clone(),
            endpoint: Endpoint::Dst,
        }
    }
}

impl EdgeEndpoint {
    /// A node term, evaluated on the node at this end of the edge.
    fn through(&self, inner: Expr<NodeLeaf>) -> Expr<EdgeLeaf> {
        let inner = Box::new(inner);
        Expr::Term(match self.endpoint {
            Endpoint::Src => EdgeLeaf::Src(inner),
            Endpoint::Dst => EdgeLeaf::Dst(inner),
        })
    }

    fn node(&self) -> Chain<NodeLeaf> {
        Chain::new(self.views.clone())
    }

    pub fn id(&self) -> Expr<EdgeLeaf> {
        self.through(self.node().id())
    }

    pub fn name(&self) -> Expr<EdgeLeaf> {
        self.through(self.node().name())
    }

    pub fn node_type(&self) -> Expr<EdgeLeaf> {
        self.through(self.node().node_type())
    }

    pub fn property(&self, name: impl Into<String>) -> PropertyTerm<EdgeLeaf> {
        let node = self.node();
        let name = name.into();
        PropertyTerm::new(|temporal| {
            self.through(Expr::Term(NodeLeaf::property(
                node.views.clone(),
                name.clone(),
                temporal,
            )))
        })
    }

    pub fn metadata(&self, name: impl Into<String>) -> Expr<EdgeLeaf> {
        self.through(self.node().metadata(name))
    }
}

// ── values ───────────────────────────────────────────────────────────────────

/// The right-hand side of a comparison: another expression on the same
/// entity, a property term, or a constant.
pub trait IntoExpr<L: Leaf> {
    fn into_expr(self) -> Expr<L>;
}

impl<L: Leaf> IntoExpr<L> for Expr<L> {
    fn into_expr(self) -> Expr<L> {
        self
    }
}

impl<L: Leaf> IntoExpr<L> for PropertyTerm<L> {
    fn into_expr(self) -> Expr<L> {
        self.latest
    }
}

impl<L: Leaf, T: Into<Prop>> IntoExpr<L> for T {
    fn into_expr(self) -> Expr<L> {
        Expr::Const(self.into())
    }
}

/// Comparison, string, membership and presence tests, and the `any`/`all`
/// qualifiers over an element-wise result. Implemented for an expression and
/// for a property term (its latest value).
pub trait EntityExprFilterOps<L: Leaf>: Into<Expr<L>> + Sized {
    fn cmp(self, op: BinaryOp, rhs: impl IntoExpr<L>) -> Expr<L> {
        Expr::Cmp(op, Box::new(self.into()), Box::new(rhs.into_expr()))
    }

    fn gt(self, rhs: impl IntoExpr<L>) -> Expr<L> {
        self.cmp(BinaryOp::Gt, rhs)
    }

    fn ge(self, rhs: impl IntoExpr<L>) -> Expr<L> {
        self.cmp(BinaryOp::Ge, rhs)
    }

    fn lt(self, rhs: impl IntoExpr<L>) -> Expr<L> {
        self.cmp(BinaryOp::Lt, rhs)
    }

    fn le(self, rhs: impl IntoExpr<L>) -> Expr<L> {
        self.cmp(BinaryOp::Le, rhs)
    }

    fn eq(self, rhs: impl IntoExpr<L>) -> Expr<L> {
        self.cmp(BinaryOp::Eq, rhs)
    }

    fn ne(self, rhs: impl IntoExpr<L>) -> Expr<L> {
        self.cmp(BinaryOp::Ne, rhs)
    }

    fn string(self, op: StringOp, rhs: impl IntoExpr<L>) -> Expr<L> {
        Expr::Str(op, Box::new(self.into()), Box::new(rhs.into_expr()))
    }

    fn starts_with(self, rhs: impl IntoExpr<L>) -> Expr<L> {
        self.string(StringOp::StartsWith, rhs)
    }

    fn ends_with(self, rhs: impl IntoExpr<L>) -> Expr<L> {
        self.string(StringOp::EndsWith, rhs)
    }

    fn contains(self, rhs: impl IntoExpr<L>) -> Expr<L> {
        self.string(StringOp::Contains, rhs)
    }

    fn not_contains(self, rhs: impl IntoExpr<L>) -> Expr<L> {
        self.string(StringOp::NotContains, rhs)
    }

    fn fuzzy_search(
        self,
        rhs: impl IntoExpr<L>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Expr<L> {
        self.string(
            StringOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            rhs,
        )
    }

    fn is_in<V: Into<Prop>>(self, values: impl IntoIterator<Item = V>) -> Expr<L> {
        self.members(values, false)
    }

    fn is_not_in<V: Into<Prop>>(self, values: impl IntoIterator<Item = V>) -> Expr<L> {
        self.members(values, true)
    }

    fn members<V: Into<Prop>>(self, values: impl IntoIterator<Item = V>, negated: bool) -> Expr<L> {
        Expr::In {
            expr: Box::new(self.into()),
            values: values.into_iter().map(Into::into).collect(),
            negated,
        }
    }

    fn is_some(self) -> Expr<L> {
        Expr::IsSome(Box::new(self.into()))
    }

    fn is_none(self) -> Expr<L> {
        Expr::IsNone(Box::new(self.into()))
    }

    /// Some element of an element-wise yes/no holds.
    fn any(self) -> Expr<L> {
        Expr::Any(Box::new(self.into()))
    }

    /// Every element of an element-wise yes/no holds.
    fn all(self) -> Expr<L> {
        Expr::All(Box::new(self.into()))
    }
}

impl<L: Leaf> EntityExprFilterOps<L> for Expr<L> {}
impl<L: Leaf> EntityExprFilterOps<L> for PropertyTerm<L> {}

/// Aggregates over a list-valued expression or a property history.
/// Implemented for an expression and for a property term.
pub trait EntityAggOps<L: Leaf>: Into<Expr<L>> + Sized {
    fn agg(self, agg: Agg) -> Expr<L> {
        Expr::Agg(agg, Box::new(self.into()))
    }

    fn sum(self) -> Expr<L> {
        self.agg(Agg::Sum)
    }

    fn avg(self) -> Expr<L> {
        self.agg(Agg::Avg)
    }

    fn min(self) -> Expr<L> {
        self.agg(Agg::Min)
    }

    fn max(self) -> Expr<L> {
        self.agg(Agg::Max)
    }

    fn first(self) -> Expr<L> {
        self.agg(Agg::First)
    }

    fn last(self) -> Expr<L> {
        self.agg(Agg::Last)
    }

    fn len(self) -> Expr<L> {
        self.agg(Agg::Len)
    }

    fn earliest(self) -> Expr<L> {
        self.agg(Agg::Earliest)
    }

    fn latest(self) -> Expr<L> {
        self.agg(Agg::Latest)
    }
}

impl<L: Leaf> EntityAggOps<L> for Expr<L> {}
impl<L: Leaf> EntityAggOps<L> for PropertyTerm<L> {}

// ── filters ──────────────────────────────────────────────────────────────────

/// A yes/no expression is a filter on its entity.
impl<L: Leaf> From<Expr<L>> for FilterExpr {
    fn from(expr: Expr<L>) -> Self {
        L::filter(expr)
    }
}

/// `and`, `or` and `not` of filters, across entities and views. Implemented
/// for expressions, filters and graph views.
pub trait ComposableFilter: Into<FilterExpr> + Sized {
    fn and(self, other: impl Into<FilterExpr>) -> FilterExpr {
        FilterExpr::And(vec![self.into(), other.into()])
    }

    fn or(self, other: impl Into<FilterExpr>) -> FilterExpr {
        FilterExpr::Or(vec![self.into(), other.into()])
    }

    fn not(self) -> FilterExpr {
        FilterExpr::Not(Box::new(self.into()))
    }
}

impl<L: Leaf> ComposableFilter for Expr<L> {}
impl ComposableFilter for FilterExpr {}
impl ComposableFilter for Chain<()> {}
