//! Filter types — bridge from expressions to a filtered graph.
//!
//! A filter is a pure data structure that pairs two expressions with an operator.
//! Calling `create_filter(graph)` compiles both sides into [`NodeOp`]s and wraps the
//! graph in a [`NodeFilteredGraph`] that skips non-matching nodes during iteration.
//!
//! # Three-phase pipeline
//!
//! ```text
//! Phase 1 — Build (pure Rust data, no graph):
//!   NodeFilter.property("age").gt(30i64)
//!   ──► BinaryCmpNodeFilter { left: Property("age"), op: Gt, right: 30i64 }
//!
//! Phase 2 — Compile (bind to graph, resolve names):
//!   BinaryCmpNodeFilter::create_node_filter(graph)?
//!   ──► Arc<dyn NodeOp<Output = bool>>
//!         = BinaryCmpNodeOp { left: NodePropOp(id=3), right: Const(Some(I64(30))), op: Gt }
//!
//! Phase 3 — Runtime (per-node, O(1)):
//!   filter.apply(storage, vid)  →  age_value = NodePropOp.apply(...)
//!                                   Prop::binary_cmp(Gt, age_value, Some(I64(30)))  →  true/false
//! ```
//!
//! # Temporal quantification
//!
//! Filter types also implement `NodeExpr` (producing list-aware ops), enabling chaining
//! before `.any()`/`.all()`:
//!
//! ```rust,ignore
//! // "pass if any temporal value of 'score' > 10"
//! NodeFilter.property("score").temporal().gt(10i64).any()
//! ──► BinaryCmpNodeFilter<AnyExpr<BinaryCmpNodeFilter<TemporalPropertyExpr, i64>>, Prop>
//!   create_node_filter(graph)?
//!   ──► BinaryCmpNodeOp { left: AnyNodeOp { inner: ListAwareCmpNodeOp { TemporalNodePropOp,
//!                                                                         Const(I64(10)), Gt } },
//!                          right: Const(Bool(true)), op: Eq }
//!
//! // "pass if sum of 'score' > 100"
//! NodeFilter.property("score").temporal().sum().gt(100i64)
//! ──► BinaryCmpNodeFilter<SumExpr<TemporalPropertyExpr>, i64>
//! ```

use super::{
    ops::{
        BinaryCmpNodeOp, ListAwareCmpNodeOp, ListAwareSetNodeOp,
        ListAwareStringNodeOp, PropValueSetNodeOp, StringNodeOp, UnaryNodeOp,
    },
    AllExpr, AnyExpr, EntityExpr, NodeExpr,
};
use crate::{
    db::{
        api::{
            state::ops::NodeOp,
            view::internal::GraphView,
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                filter_operator::{
                    BinaryOp, SetOp, StringOp, UnaryOp,
                },
                node_filter::NodeFilterFactory
                ,
                ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter, CreateFilter,
                CreateView, MetadataExpr, PropertyExpr, TryAsCompositeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::{marker::PhantomData, sync::Arc};

// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpNodeFilter<L, R> — binary expression filter
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that compares two [`NodeExpr`] values using a [`BinaryOp`].
///
/// Both sides produce `Option<Prop>` at runtime. Created by [`NodeExprFilterOps`] methods
/// (`.gt`, `.lt`, `.eq`, `.ne`, `.ge`, `.le`).
///
/// As a **terminal filter** (`CreateFilter`): compiles to `BinaryCmpNodeOp` → bool.
/// As a **mid-chain expression** (`NodeExpr`): compiles to `ListAwareCmpNodeOp` → `Option<Prop::List([Bool]...)>`.
///
/// ```rust,ignore
/// NodeFilter.degree().gt(2usize)
///   → BinaryCmpNodeFilter<DegreeExpr<..>, usize>
///   → BinaryCmpNodeOp { left: Degree(..).map(Prop::U64), right: Const(Some(U64(2))), op: Gt }
///
/// NodeFilter.property("age").eq(30i64)
///   → BinaryCmpNodeFilter<Property, i64>
///   → BinaryCmpNodeOp { left: NodePropOp(prop_id=N), right: Const(Some(I64(30))), op: Eq }
/// ```
pub struct BinaryCmpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
    pub left: L,
    pub op: BinaryOp,
    pub right: R,
}

impl<L, R> BinaryCmpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
    pub fn new(left: L, op: BinaryOp, right: R) -> Self {
        Self { left, op, right }
    }
}

impl<L, R> Clone for BinaryCmpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
    fn clone(&self) -> Self {
        Self {
            left: self.left.clone(),
            op: self.op,
            right: self.right.clone(),
        }
    }
}

impl<L, R> ComposableFilter for BinaryCmpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
}

/// Reject ordering operators on boolean properties.
fn validate_binary_op(op: &BinaryOp, prop_type: &PropType) -> Result<(), GraphError> {
    if *prop_type != PropType::Empty
        && matches!(
            op,
            BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
        )
        && *prop_type == PropType::Bool
    {
        return Err(GraphError::InvalidFilter(format!(
            "operator {:?} is not valid for boolean properties",
            op
        )));
    }
    Ok(())
}

/// Reject string operators on non-string properties.
///
/// Only fires when the type is known (`!= PropType::Empty`).
fn validate_string_op(prop_type: &PropType) -> Result<(), GraphError> {
    if *prop_type != PropType::Empty && *prop_type != PropType::Str {
        return Err(GraphError::InvalidFilter(format!(
            "string operator requires a Str property, but the property type is {}",
            prop_type
        )));
    }
    Ok(())
}

impl<L, R> CreateFilter for BinaryCmpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, Self::NodeFilter<'graph, G>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        validate_binary_op(&self.op, &left.prop_type())?;
        Ok(Arc::new(BinaryCmpNodeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<L, R> TryAsCompositeFilter for BinaryCmpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnaryNodeFilter<E> — is_some / is_none on nullable expressions
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that tests the presence of an `Option`-valued expression.
///
/// Created by `.is_some()` / `.is_none()` on any `NodeExpr<Output = Option<I>>`.
/// Compiles to a `UnaryNodeOp { inner, op }`.
///
/// ```rust,ignore
/// NodeFilter.property("age").is_some::<Prop>()
///   → UnaryNodeFilter<Property, Prop>
///   → UnaryNodeOp { inner: NodePropOp(prop_id=N), op: IsSome }
/// ```
pub struct UnaryNodeFilter<E, I>
where
    E: NodeExpr,
    I: Clone + Send + Sync + 'static,
{
    pub expr: E,
    pub op: UnaryOp,
    pub(crate) _phantom: PhantomData<I>,
}

impl<E, I> Clone for UnaryNodeFilter<E, I>
where
    E: NodeExpr,
    I: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            op: self.op,
            _phantom: PhantomData,
        }
    }
}

impl<E, I> ComposableFilter for UnaryNodeFilter<E, I>
where
    E: NodeExpr,
    I: Clone + Send + Sync + 'static,
{
}

impl<E> CreateFilter for UnaryNodeFilter<E, Prop>
where
    E: NodeExpr,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, UnaryNodeOp<'graph, Prop>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = UnaryNodeOp<'graph, Prop>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let inner = self.expr.create_node_op(graph)?;
        Ok(UnaryNodeOp { inner, op: self.op })
    }
}

impl<E, I> TryAsCompositeFilter for UnaryNodeFilter<E, I>
where
    E: NodeExpr,
    I: Clone + Send + Sync + 'static,
{
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// StringNodeFilter<L, R> — string expression filter
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that applies a [`StringOp`] to two [`NodeExpr`] values.
///
/// Both sides must produce the same string-comparable type (`L::Output: StringComparable`).
/// Created by the string methods on [`NodeExprFilterOps`] (`.starts_with`, `.ends_with`,
/// `.contains`, `.not_contains`, `.fuzzy_search`).
/// Compiles to a `StringNodeOp` wrapped in `Arc<dyn NodeOp<Output = bool>>`.
///
/// ```rust,ignore
/// NodeFilter.name().starts_with("Al")
///   → StringNodeFilter<Name, &str>
///   → StringNodeOp { left: Name.map(...), right: Const(Some(Str("Al"))), op: StartsWith }
///
/// NodeFilter.property("tag").contains(Prop::Str("foo".into()))
///   → StringNodeFilter<Property, Prop>
///   → StringNodeOp { left: NodePropOp(prop_id=N), right: Const(Some(Str("foo"))), op: Contains }
/// ```
pub struct StringNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
    pub left: L,
    pub op: StringOp,
    pub right: R,
}

impl<L, R> StringNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
    pub fn new(left: L, op: StringOp, right: R) -> Self {
        Self { left, op, right }
    }
}

impl<L, R> Clone for StringNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
    fn clone(&self) -> Self {
        Self {
            left: self.left.clone(),
            op: self.op,
            right: self.right.clone(),
        }
    }
}

impl<L, R> ComposableFilter for StringNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
}

impl<L, R> CreateFilter for StringNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, Self::NodeFilter<'graph, G>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        validate_string_op(&left.prop_type())?;
        Ok(Arc::new(StringNodeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<L, R> TryAsCompositeFilter for StringNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr,
{
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropValueSetFilter<E> — is_in / is_not_in for aggregated Option<Prop> values
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that checks whether an aggregated scalar property value is in
/// (or not in) a fixed set of `Prop` values.  Uses linear scan because `Prop`
/// may contain floats that don't implement `Hash`.
pub struct PropValueSetFilter<E: NodeExpr> {
    pub(crate) expr: E,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
}

impl<E: NodeExpr> Clone for PropValueSetFilter<E> {
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            values: self.values.clone(),
            op: self.op,
        }
    }
}

impl<E: NodeExpr> ComposableFilter for PropValueSetFilter<E> {}

impl<E: NodeExpr> CreateFilter for PropValueSetFilter<E> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, PropValueSetNodeOp<'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = PropValueSetNodeOp<'graph>;
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(PropValueSetNodeOp {
            inner: self.expr.create_node_op(graph)?,
            values: self.values,
            op: self.op,
        })
    }
}

impl<E: NodeExpr> TryAsCompositeFilter for PropValueSetFilter<E> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalProp<E> — entry point returned from `.property(name).temporal()`
// ─────────────────────────────────────────────────────────────────────────────

/// Entry point returned by `PropertyExpr::temporal()`.
///
/// `E` is the view expression (e.g. `NodeFilter`, `Windowed<NodeFilter>`, `Layered<NodeFilter>`)
/// that scopes which temporal property values are visible.
///
/// Calling a method produces the next step in the chain:
/// ```rust,ignore
/// NodeFilter.property("score").temporal()        // → TemporalProp<NodeFilter>
///     .gt(10i64)                                 // → BinaryCmpNodeFilter<TemporalPropertyExpr, i64>
///     .any()                                     // → BinaryCmpNodeFilter<AnyExpr<..>, Prop>
///
/// NodeFilter.property("price").temporal()        // → TemporalProp<NodeFilter>
///     .sum()                                     // → SumExpr<TemporalPropertyExpr<NodeFilter>>
///     .gt(100i64)                                // → BinaryCmpNodeFilter<SumExpr<..>, i64>
///
/// NodeFilter.window(0, 100).property("score")
///     .temporal()                                // → TemporalProp<Windowed<NodeFilter>>
///     .gt(10i64).any()
/// ```
pub struct TemporalProp<E: CreateView + Clone> {
    pub(crate) view_expr: E,
    pub(crate) name: String,
}

impl<E: CreateView + Clone + Send + Sync + 'static> TemporalProp<E> {
    pub(crate) fn new(view_expr: E, name: impl Into<String>) -> Self {
        Self {
            view_expr,
            name: name.into(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NodePropertyExprOps — fluent comparison API for node-side property expressions
// ─────────────────────────────────────────────────────────────────────────────

pub trait NodePropertyExprOps: NodeExpr + Sized {
    fn is_in(self, values: impl IntoIterator<Item = Prop>) -> PropValueSetFilter<Self> {
        PropValueSetFilter { expr: self, values: values.into_iter().collect(), op: SetOp::IsIn }
    }
    fn is_not_in(self, values: impl IntoIterator<Item = Prop>) -> PropValueSetFilter<Self> {
        PropValueSetFilter { expr: self, values: values.into_iter().collect(), op: SetOp::IsNotIn }
    }
}

impl<E: CreateView + NodeFilterFactory + Clone + Send + Sync + 'static> NodePropertyExprOps
    for PropertyExpr<E>
{
}

impl<E: CreateView + NodeFilterFactory + Clone + Send + Sync + 'static> NodePropertyExprOps
    for MetadataExpr<E>
{
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeExprFilterOps — comparison and set operators on NodeExpr
// ─────────────────────────────────────────────────────────────────────────────

/// Comparison, string, set, and presence operators on any [`NodeExpr`].
///
/// `.any()` / `.all()` are terminal: they wrap `self` in `AnyExpr`/`AllExpr` and compare the
/// result to `Bool(true)`. For element-wise comparison before reduction, chain in order:
/// `.gt(10i64).any()` not `.any().gt(10i64)`.
///
/// ```rust,ignore
/// NodeFilter.degree().gt(2usize)
/// NodeFilter.out_degree().gt(NodeFilter.in_degree())
/// NodeFilter.property("age").gt(30i64)
/// NodeFilter.property("score").temporal().gt(10i64).any()
/// ```
pub trait NodeExprFilterOps: NodeExpr + Sized {
    fn gt<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Gt, rhs)
    }

    fn ge<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Ge, rhs)
    }

    fn lt<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Lt, rhs)
    }

    fn le<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Le, rhs)
    }

    fn eq<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Eq, rhs)
    }

    fn ne<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Ne, rhs)
    }

    fn starts_with<R: NodeExpr>(self, rhs: R) -> StringNodeFilter<Self, R>
    {
        StringNodeFilter::new(self, StringOp::StartsWith, rhs)
    }

    fn ends_with<R: NodeExpr>(self, rhs: R) -> StringNodeFilter<Self, R>
    where
    {
        StringNodeFilter::new(self, StringOp::EndsWith, rhs)
    }

    fn contains<R: NodeExpr>(self, rhs: R) -> StringNodeFilter<Self, R>
    {
        StringNodeFilter::new(self, StringOp::Contains, rhs)
    }

    fn not_contains<R: NodeExpr>(self, rhs: R) -> StringNodeFilter<Self, R>
    where
    {
        StringNodeFilter::new(self, StringOp::NotContains, rhs)
    }

    fn fuzzy_search<R: NodeExpr>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringNodeFilter<Self, R>
    {
        StringNodeFilter::new(
            self,
            StringOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            rhs,
        )
    }

    fn is_some<Inner>(self) -> UnaryNodeFilter<Self, Inner>
    where
        Self: NodeExpr,
        Inner: Clone + Send + Sync + 'static,
    {
        UnaryNodeFilter {
            expr: self,
            op: UnaryOp::IsSome,
            _phantom: PhantomData,
        }
    }

    fn is_none<Inner>(self) -> UnaryNodeFilter<Self, Inner>
    where
        Self: NodeExpr,
        Inner: Clone + Send + Sync + 'static,
    {
        UnaryNodeFilter {
            expr: self,
            op: UnaryOp::IsNone,
            _phantom: PhantomData,
        }
    }

    fn is_true(self) -> BinaryCmpNodeFilter<Self, Prop>
    where
        Self: NodeExpr,
    {
        self.eq(Prop::Bool(true))
    }

    fn is_false(self) -> BinaryCmpNodeFilter<Self, Prop>
    where
        Self: NodeExpr,
    {
        self.eq(Prop::Bool(false))
    }

    fn any(self) -> BinaryCmpNodeFilter<AnyExpr<Self>, Prop> {
        BinaryCmpNodeFilter::new(AnyExpr(self), BinaryOp::Eq, Prop::Bool(true))
    }

    fn all(self) -> BinaryCmpNodeFilter<AllExpr<Self>, Prop> {
        BinaryCmpNodeFilter::new(AllExpr(self), BinaryOp::Eq, Prop::Bool(true))
    }
}

impl<E: NodeExpr> NodeExprFilterOps for E {}

// ─────────────────────────────────────────────────────────────────────────────
// NodeExpr impls for filter types — enables mid-chain use before .any()/.all()
//
// e.g. temporal().sum().gt(5).any()
//      temporal().contains("rock").all()
//      temporal().is_in([...]).any()
// ─────────────────────────────────────────────────────────────────────────────

impl<L: NodeExpr, R: NodeExpr> EntityExpr for BinaryCmpNodeFilter<L, R> {}

impl<L: NodeExpr, R: NodeExpr> NodeExpr for BinaryCmpNodeFilter<L, R> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        Ok(Arc::new(ListAwareCmpNodeOp { left, right, op: self.op }))
    }
}

impl<L: NodeExpr, R: NodeExpr> EntityExpr for StringNodeFilter<L, R> {}

impl<L: NodeExpr, R: NodeExpr> NodeExpr for StringNodeFilter<L, R> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        Ok(Arc::new(ListAwareStringNodeOp { left, right, op: self.op }))
    }
}

impl<E: NodeExpr> EntityExpr for PropValueSetFilter<E> {}

impl<E: NodeExpr> NodeExpr for PropValueSetFilter<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.expr.create_node_op(graph)?;
        Ok(Arc::new(ListAwareSetNodeOp {
            inner,
            values: self.values.clone(),
            op: self.op,
        }))
    }
}
