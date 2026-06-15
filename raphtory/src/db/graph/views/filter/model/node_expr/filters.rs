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
//!   NodeFilter::property("age").gt(30i64)
//!   ──► BinaryCmpNodeFilter { left: Property("age"), op: Gt, right: ConstExpr(30i64) }
//!
//! Phase 2 — Compile (bind to graph, resolve names):
//!   BinaryCmpNodeFilter::create_node_filter(graph)?
//!   ──► Arc<dyn NodeOp<Output = bool>>
//!         = BinaryCmpNodeOp { left: NodePropOp(id=3), right: ConstNodeOp(30), op: Gt }
//!
//! Phase 3 — Runtime (per-node, O(1)):
//!   filter.apply(storage, vid)  →  age_value = NodePropOp.apply(...)
//!                                   Prop::binary_cmp(Gt, age_value, 30)  →  true/false
//! ```
//!
//! # Temporal quantification
//!
//! ```rust,ignore
//! // "pass if any temporal value of 'score' > 10"
//! NodeFilter::temporal_property("score").any().gt(10i64)
//! ──► QuantifiedNodeFilter<TemporalPropertyExpr, AnyMode, ConstExpr<i64>>
//!   create_node_filter(graph)?
//!   ──► AnyNodeOp { inner: PropListCompareOp { temporal_op, rhs: ConstNodeOp(10), op: Gt } }
//!
//! // "pass if sum of 'score' > 100"
//! NodeFilter::temporal_property("score").sum().gt(100i64)
//! ──► BinaryCmpNodeFilter<SumExpr<TemporalPropertyExpr>, ConstExpr<i64>>
//! ```

use super::{
    ops::{
        AllNodeOp, AnyNodeOp, BinaryCmpNodeOp, ListAwareCmpNodeOp, ListAwareSetNodeOp,
        ListAwareStringNodeOp, PropValueSetNodeOp, SetNodeOp, StringNodeOp, UnaryNodeOp,
    },
    EntityExpr, NodeExpr, TemporalPropertyExpr,
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
                    BinaryOp, Comparable, SetOp, StringComparable, StringOp, UnaryOp,
                },
                node_filter::NodeFilterFactory,
                property_filter::Op,
                ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter, CreateFilter,
                CreateView, MetadataExpr, PropertyExpr, TryAsCompositeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropType},
        VID,
    },
    storage::arc_str::ArcStr,
};
use std::{collections::HashSet, hash::Hash, marker::PhantomData, sync::Arc};
use crate::db::graph::views::filter::model::{FirstExpr, LastExpr, LenExpr, MaxExpr, MinExpr, SumExpr};

// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpNodeFilter<L, R> — binary expression filter
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that compares two [`NodeExpr`] values using a [`BinaryOp`].
///
/// The output type is determined by the left expression (`L::Output`);
/// the right expression must produce the same type.
///
/// Created by [`NodeExprFilterOps`] methods (`.gt`, `.lt`, `.eq`, `.ne`, `.ge`, `.le`).
/// Compiles to a `BinaryCmpNodeOp` wrapped in `Arc<dyn NodeOp<Output = bool>>`.
///
/// ```rust,ignore
/// NodeFilter::degree().gt(2usize)
///   → BinaryCmpNodeFilter<DegreeExpr<..>, usize>
///   → BinaryCmpNodeOp { left: Degree(..), right: ConstNodeOp(2), op: Gt }
///
/// NodeFilter::property("age").eq(30i64)
///   → BinaryCmpNodeFilter<Property, i64>
///   → BinaryCmpNodeOp { left: NodePropOp(prop_id=N), right: ConstNodeOp(30), op: Eq }
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
/// NodeFilter::property("age").is_some()
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

impl<E, I> CreateFilter for UnaryNodeFilter<E, I>
where
    E: NodeExpr,
    I: Clone + Send + Sync + 'static,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, UnaryNodeOp<'graph, I>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = UnaryNodeOp<'graph, I>;

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
// SetNodeFilter<E> — is_in / is_not_in on nullable expressions
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that checks whether an `Option`-valued expression is contained
/// in (or absent from) a fixed set of values.
///
/// Created by `.is_in(values)` / `.is_not_in(values)`.
/// Compiles to a `SetNodeOp { inner, op, values }`.
///
/// ```rust,ignore
/// NodeFilter::node_type().is_in(["Person", "Account"])
///   → SetNodeFilter<Type, ArcStr>
///   → SetNodeOp { inner: TypeOp, op: IsIn, values: {"Person", "Account"} }
/// ```
pub struct SetNodeFilter<E, I>
where
    E: NodeExpr,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    pub expr: E,
    pub op: SetOp,
    pub values: Arc<HashSet<I>>,
    pub(crate) _phantom: PhantomData<I>,
}

impl<E, I> Clone for SetNodeFilter<E, I>
where
    E: NodeExpr,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            op: self.op,
            values: self.values.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<E, I> ComposableFilter for SetNodeFilter<E, I>
where
    E: NodeExpr,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
}

impl<E, I> CreateFilter for SetNodeFilter<E, I>
where
    E: NodeExpr,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, SetNodeOp<'graph, I>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = SetNodeOp<'graph, I>;

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
        Ok(SetNodeOp {
            inner,
            op: self.op,
            values: self.values,
        })
    }
}

impl<E, I> TryAsCompositeFilter for SetNodeFilter<E, I>
where
    E: NodeExpr,
    I: Eq + Hash + Clone + Send + Sync + 'static,
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
/// NodeFilter::name().starts_with("Al")
///   → StringNodeFilter<Name, &str>
///   → StringNodeOp { left: NameOp, right: ConstNodeOp("Al"), op: StartsWith }
///
/// NodeFilter::property("tag").contains(Prop::Str("foo".into()))
///   → StringNodeFilter<Property, Prop>
///   → StringNodeOp { left: NodePropOp(prop_id=N), right: ConstNodeOp(Str("foo")), op: Contains }
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
// TemporalProp<E> — entry point returned from `.temporal_property(name)`
// ─────────────────────────────────────────────────────────────────────────────

/// Entry point returned by `NodeFilter::temporal_property(name)`.
///
/// `E` is the view expression (e.g. `NodeFilter`, `Windowed<NodeFilter>`, `Layered<NodeFilter>`)
/// that scopes which temporal property values are visible.
///
/// Calling a method produces the next step in the chain:
/// ```rust,ignore
/// NodeFilter::temporal_property("score")         // → TemporalProp<NodeFilter>
///     .any()                                      // → Quantified<TemporalPropertyExpr<..>, AnyMode>
///     .gt(10i64)                                  // → QuantifiedNodeFilter<.., AnyMode, i64>
///
/// NodeFilter::temporal_property("price")         // → TemporalProp<NodeFilter>
///     .sum()                                      // → Aggregated<SumExpr<TemporalPropertyExpr<..>>>
///     .gt(100i64)                                 // → BinaryCmpNodeFilter<SumExpr<..>, i64>
///
/// NodeFilter.window(0, 100)
///     .temporal_property("score")                // → TemporalProp<Windowed<NodeFilter>>
///     .any().gt(10i64)
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
/// ```rust,ignore
/// DegreeExpr(Direction::BOTH).gt(2usize)
/// DegreeExpr(Direction::OUT).gt(DegreeExpr(Direction::IN))
/// NodeFilter::property("age").gt(30i64)
/// DegreeExpr(Direction::BOTH).is_in([2usize, 3usize])
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
