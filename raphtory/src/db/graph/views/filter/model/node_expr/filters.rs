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
        AllNodeOp, AnyNodeOp, BinaryCmpNodeOp, PropListCompareOp, SetNodeOp, StringNodeOp,
        UnaryNodeOp,
    },
    AvgExpr, FirstExpr, LastExpr, LenExpr, MaxExpr, MinExpr, NodeExpr, SumExpr,
    TemporalPropertyExpr,
};
use crate::{
    db::{
        api::{state::ops::NodeOp, view::internal::GraphView},
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                filter_operator::{
                    BinaryOp, Comparable, SetOp, StringComparable, StringOp, UnaryOp,
                },
                ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter, CreateFilter,
                CreateView, TryAsCompositeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::{
    properties::prop::{Prop, PropType},
    VID,
};
use std::{collections::HashSet, hash::Hash, marker::PhantomData, sync::Arc};

// ─────────────────────────────────────────────────────────────────────────────
// Sealed trait for QuantifierMode
// ─────────────────────────────────────────────────────────────────────────────

mod sealed {
    pub trait Sealed {}
}

// ─────────────────────────────────────────────────────────────────────────────
// QuantifierMode — AnyMode / AllMode
// ─────────────────────────────────────────────────────────────────────────────

/// Sealed marker trait used as a type parameter on [`QuantifiedNodeFilter`] and
/// [`QuantifiedContextBuilder`] to distinguish `any` vs `all` semantics at compile time.
/// Never instantiated — only used as `<AnyMode>` / `<AllMode>` in type positions.
pub trait QuantifierMode: sealed::Sealed + Clone + Copy + Send + Sync + 'static {}

/// Marker for "pass if *any* temporal value matches" — used as `Q` in
/// `QuantifiedNodeFilter<E, AnyMode, R>`. Selects [`AnyNodeOp`] at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnyMode;

/// Marker for "pass if *all* temporal values match" — used as `Q` in
/// `QuantifiedNodeFilter<E, AllMode, R>`. Selects [`AllNodeOp`] at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllMode;

impl sealed::Sealed for AnyMode {}
impl sealed::Sealed for AllMode {}
impl QuantifierMode for AnyMode {}
impl QuantifierMode for AllMode {}

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
    R: NodeExpr<Output = L::Output>,
{
    pub left: L,
    pub op: BinaryOp,
    pub right: R,
}

impl<L, R> BinaryCmpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
{
    pub fn new(left: L, op: BinaryOp, right: R) -> Self {
        Self { left, op, right }
    }
}

impl<L, R> Clone for BinaryCmpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
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
    R: NodeExpr<Output = L::Output>,
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
    R: NodeExpr<Output = L::Output>,
    L::Output: Comparable,
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
    R: NodeExpr<Output = L::Output>,
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
    E: NodeExpr<Output = Option<I>>,
    I: Clone + Send + Sync + 'static,
{
    pub expr: E,
    pub op: UnaryOp,
    pub(crate) _phantom: PhantomData<I>,
}

impl<E, I> Clone for UnaryNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
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
    E: NodeExpr<Output = Option<I>>,
    I: Clone + Send + Sync + 'static,
{
}

impl<E, I> CreateFilter for UnaryNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
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
    E: NodeExpr<Output = Option<I>>,
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
    E: NodeExpr<Output = Option<I>>,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    pub expr: E,
    pub op: SetOp,
    pub values: Arc<HashSet<I>>,
    pub(crate) _phantom: PhantomData<I>,
}

impl<E, I> Clone for SetNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
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
    E: NodeExpr<Output = Option<I>>,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
}

impl<E, I> CreateFilter for SetNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
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
    E: NodeExpr<Output = Option<I>>,
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
    R: NodeExpr<Output = L::Output>,
    L::Output: StringComparable,
{
    pub left: L,
    pub op: StringOp,
    pub right: R,
}

impl<L, R> StringNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
    L::Output: StringComparable,
{
    pub fn new(left: L, op: StringOp, right: R) -> Self {
        Self { left, op, right }
    }
}

impl<L, R> Clone for StringNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
    L::Output: StringComparable,
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
    R: NodeExpr<Output = L::Output>,
    L::Output: StringComparable,
{
}

impl<L, R> CreateFilter for StringNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
    L::Output: StringComparable,
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
    R: NodeExpr<Output = L::Output>,
    L::Output: StringComparable,
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
// QuantifiedNodeFilter<E, Q, R> — leaf filter wrapping a quantified comparison
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that applies a [`BinaryOp`] to every temporal value and reduces
/// the results using `Q` ([`AnyMode`] or [`AllMode`]).
///
/// Not constructed directly — returned by `QuantifiedContextBuilder::gt/eq/…`:
/// ```rust,ignore
/// // NodeFilter::temporal_property("score").any().gt(10i64)
/// //   → QuantifiedNodeFilter<TemporalPropertyExpr<NodeFilter>, AnyMode, i64>
/// //   compiles to: AnyNodeOp { inner: PropListCompareOp { …, op: Gt } }
///
/// // NodeFilter::temporal_property("score").all().gt(0i64)
/// //   → QuantifiedNodeFilter<TemporalPropertyExpr<NodeFilter>, AllMode, i64>
/// //   compiles to: AllNodeOp { inner: PropListCompareOp { …, op: Gt } }
/// ```
pub struct QuantifiedNodeFilter<E, Q, R>
where
    E: NodeExpr<Output = Prop>,
    Q: QuantifierMode,
    R: NodeExpr<Output = Option<Prop>>,
{
    pub expr: E,
    pub rhs: R,
    pub op: BinaryOp,
    pub(crate) _q: PhantomData<Q>,
}

impl<E, Q, R> QuantifiedNodeFilter<E, Q, R>
where
    E: NodeExpr<Output = Prop>,
    Q: QuantifierMode,
    R: NodeExpr<Output = Option<Prop>>,
{
    pub fn new(expr: E, op: BinaryOp, rhs: R) -> Self {
        Self {
            expr,
            rhs,
            op,
            _q: PhantomData,
        }
    }
}

impl<E, Q, R> Clone for QuantifiedNodeFilter<E, Q, R>
where
    E: NodeExpr<Output = Prop>,
    Q: QuantifierMode,
    R: NodeExpr<Output = Option<Prop>>,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            rhs: self.rhs.clone(),
            op: self.op,
            _q: PhantomData,
        }
    }
}

impl<E, Q, R> ComposableFilter for QuantifiedNodeFilter<E, Q, R>
where
    E: NodeExpr<Output = Prop>,
    Q: QuantifierMode,
    R: NodeExpr<Output = Option<Prop>>,
{
}

impl<E, R> CreateFilter for QuantifiedNodeFilter<E, AnyMode, R>
where
    E: NodeExpr<Output = Prop>,
    R: NodeExpr<Output = Option<Prop>>,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G, AnyNodeOp<'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = AnyNodeOp<'graph>;
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
        let inner = Arc::new(PropListCompareOp {
            inner: self.expr.create_node_op(graph.clone())?,
            rhs: self.rhs.create_node_op(graph)?,
            op: self.op,
        });
        Ok(AnyNodeOp { inner })
    }
}

impl<E, R> CreateFilter for QuantifiedNodeFilter<E, AllMode, R>
where
    E: NodeExpr<Output = Prop>,
    R: NodeExpr<Output = Option<Prop>>,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G, AllNodeOp<'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = AllNodeOp<'graph>;
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
        let inner = Arc::new(PropListCompareOp {
            inner: self.expr.create_node_op(graph.clone())?,
            rhs: self.rhs.create_node_op(graph)?,
            op: self.op,
        });
        Ok(AllNodeOp { inner })
    }
}

impl<E, Q, R> TryAsCompositeFilter for QuantifiedNodeFilter<E, Q, R>
where
    E: NodeExpr<Output = Prop>,
    Q: QuantifierMode,
    R: NodeExpr<Output = Option<Prop>>,
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
// Context builders — carry expression through the builder chain
// ─────────────────────────────────────────────────────────────────────────────

/// Intermediate builder returned by [`TemporalPropContext::any`] / [`TemporalPropContext::all`].
///
/// Carries the temporal expression `E` and the quantifier `Q` until a comparison
/// operator is called, which produces the final [`QuantifiedNodeFilter`]:
/// ```rust,ignore
/// NodeFilter::temporal_property("score").any()   // → QuantifiedContextBuilder<TemporalPropertyExpr<..>, AnyMode>
///     .gt(10i64)                                  // → QuantifiedNodeFilter<TemporalPropertyExpr<..>, AnyMode, i64>
/// ```
pub struct QuantifiedContextBuilder<E, Q>
where
    E: NodeExpr<Output = Prop>,
    Q: QuantifierMode,
{
    pub(crate) expr: E,
    pub(crate) _q: PhantomData<Q>,
}

impl<E, Q> QuantifiedContextBuilder<E, Q>
where
    E: NodeExpr<Output = Prop>,
    Q: QuantifierMode,
{
    fn finish<R: NodeExpr<Output = Option<Prop>>>(
        self,
        op: BinaryOp,
        rhs: R,
    ) -> QuantifiedNodeFilter<E, Q, R> {
        QuantifiedNodeFilter::new(self.expr, op, rhs)
    }

    pub fn eq<R: NodeExpr<Output = Option<Prop>>>(self, rhs: R) -> QuantifiedNodeFilter<E, Q, R> {
        self.finish(BinaryOp::Eq, rhs)
    }

    pub fn ne<R: NodeExpr<Output = Option<Prop>>>(self, rhs: R) -> QuantifiedNodeFilter<E, Q, R> {
        self.finish(BinaryOp::Ne, rhs)
    }

    pub fn gt<R: NodeExpr<Output = Option<Prop>>>(self, rhs: R) -> QuantifiedNodeFilter<E, Q, R> {
        self.finish(BinaryOp::Gt, rhs)
    }

    pub fn ge<R: NodeExpr<Output = Option<Prop>>>(self, rhs: R) -> QuantifiedNodeFilter<E, Q, R> {
        self.finish(BinaryOp::Ge, rhs)
    }

    pub fn lt<R: NodeExpr<Output = Option<Prop>>>(self, rhs: R) -> QuantifiedNodeFilter<E, Q, R> {
        self.finish(BinaryOp::Lt, rhs)
    }

    pub fn le<R: NodeExpr<Output = Option<Prop>>>(self, rhs: R) -> QuantifiedNodeFilter<E, Q, R> {
        self.finish(BinaryOp::Le, rhs)
    }
}

/// Intermediate builder returned by [`TemporalPropContext::sum`], `.avg()`, `.min()` etc.
///
/// Wraps the aggregator expression `E` (e.g. `SumExpr<TemporalPropertyExpr<..>>`) until
/// a comparison operator is called, which produces a [`BinaryCmpNodeFilter`]:
/// ```rust,ignore
/// NodeFilter::temporal_property("price").sum()   // → NodeExprContextBuilder<SumExpr<TemporalPropertyExpr<..>>>
///     .gt(100i64)                                 // → BinaryCmpNodeFilter<SumExpr<TemporalPropertyExpr<..>>, i64>
/// ```
pub struct NodeExprContextBuilder<E: NodeExpr> {
    pub(crate) expr: E,
}

impl<E: NodeExpr> NodeExprContextBuilder<E> {
    fn finish<R: NodeExpr<Output = E::Output>>(
        self,
        op: BinaryOp,
        rhs: R,
    ) -> BinaryCmpNodeFilter<E, R> {
        BinaryCmpNodeFilter::new(self.expr, op, rhs)
    }

    pub fn eq<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<E, R> {
        self.finish(BinaryOp::Eq, rhs)
    }

    pub fn ne<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<E, R> {
        self.finish(BinaryOp::Ne, rhs)
    }

    pub fn gt<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<E, R> {
        self.finish(BinaryOp::Gt, rhs)
    }

    pub fn ge<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<E, R> {
        self.finish(BinaryOp::Ge, rhs)
    }

    pub fn lt<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<E, R> {
        self.finish(BinaryOp::Lt, rhs)
    }

    pub fn le<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<E, R> {
        self.finish(BinaryOp::Le, rhs)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalPropContext<E> — entry point returned from `.temporal_property(name)`
// ─────────────────────────────────────────────────────────────────────────────

/// Entry point returned by `NodeFilter::temporal_property(name)`.
///
/// `E` is the view expression (e.g. `NodeFilter`, `Windowed<NodeFilter>`, `Layered<NodeFilter>`)
/// that scopes which temporal property values are visible.
///
/// Calling a method on this builder creates the next step in the chain:
/// ```rust,ignore
/// NodeFilter::temporal_property("score")         // → TemporalPropContext<NodeFilter>
///     .any()                                      // → QuantifiedContextBuilder<TemporalPropertyExpr<..>, AnyMode>
///     .gt(10i64)                                  // → QuantifiedNodeFilter<.., AnyMode, i64>
///
/// NodeFilter::temporal_property("price")         // → TemporalPropContext<NodeFilter>
///     .sum()                                      // → NodeExprContextBuilder<SumExpr<TemporalPropertyExpr<..>>>
///     .gt(100i64)                                 // → BinaryCmpNodeFilter<SumExpr<..>, i64>
///
/// NodeFilter.window(0, 100)
///     .temporal_property("score")                // → TemporalPropContext<Windowed<NodeFilter>>
///     .any().gt(10i64)
/// ```
pub struct TemporalPropContext<E: CreateView + Clone> {
    pub(crate) view_expr: E,
    pub(crate) name: String,
}

impl<E: CreateView + Clone + Send + Sync + 'static> TemporalPropContext<E> {
    pub(crate) fn new(view_expr: E, name: impl Into<String>) -> Self {
        Self {
            view_expr,
            name: name.into(),
        }
    }

    fn make_expr(self) -> TemporalPropertyExpr<E> {
        TemporalPropertyExpr {
            view_expr: self.view_expr,
            name: self.name,
        }
    }

    pub fn any(self) -> QuantifiedContextBuilder<TemporalPropertyExpr<E>, AnyMode> {
        QuantifiedContextBuilder {
            expr: self.make_expr(),
            _q: PhantomData,
        }
    }

    pub fn all(self) -> QuantifiedContextBuilder<TemporalPropertyExpr<E>, AllMode> {
        QuantifiedContextBuilder {
            expr: self.make_expr(),
            _q: PhantomData,
        }
    }

    pub fn sum(self) -> NodeExprContextBuilder<SumExpr<TemporalPropertyExpr<E>>> {
        NodeExprContextBuilder {
            expr: SumExpr(self.make_expr()),
        }
    }

    pub fn avg(self) -> NodeExprContextBuilder<AvgExpr<TemporalPropertyExpr<E>>> {
        NodeExprContextBuilder {
            expr: AvgExpr(self.make_expr()),
        }
    }

    pub fn min(self) -> NodeExprContextBuilder<MinExpr<TemporalPropertyExpr<E>>> {
        NodeExprContextBuilder {
            expr: MinExpr(self.make_expr()),
        }
    }

    pub fn max(self) -> NodeExprContextBuilder<MaxExpr<TemporalPropertyExpr<E>>> {
        NodeExprContextBuilder {
            expr: MaxExpr(self.make_expr()),
        }
    }

    pub fn first(self) -> NodeExprContextBuilder<FirstExpr<TemporalPropertyExpr<E>>> {
        NodeExprContextBuilder {
            expr: FirstExpr(self.make_expr()),
        }
    }

    pub fn last(self) -> NodeExprContextBuilder<LastExpr<TemporalPropertyExpr<E>>> {
        NodeExprContextBuilder {
            expr: LastExpr(self.make_expr()),
        }
    }

    pub fn len(self) -> NodeExprContextBuilder<LenExpr<TemporalPropertyExpr<E>>> {
        NodeExprContextBuilder {
            expr: LenExpr(self.make_expr()),
        }
    }
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
    fn gt<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Gt, rhs)
    }

    fn ge<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Ge, rhs)
    }

    fn lt<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Lt, rhs)
    }

    fn le<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Le, rhs)
    }

    fn eq<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Eq, rhs)
    }

    fn ne<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpNodeFilter<Self, R> {
        BinaryCmpNodeFilter::new(self, BinaryOp::Ne, rhs)
    }

    fn starts_with<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> StringNodeFilter<Self, R>
    where
        Self::Output: StringComparable,
    {
        StringNodeFilter::new(self, StringOp::StartsWith, rhs)
    }

    fn ends_with<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> StringNodeFilter<Self, R>
    where
        Self::Output: StringComparable,
    {
        StringNodeFilter::new(self, StringOp::EndsWith, rhs)
    }

    fn contains<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> StringNodeFilter<Self, R>
    where
        Self::Output: StringComparable,
    {
        StringNodeFilter::new(self, StringOp::Contains, rhs)
    }

    fn not_contains<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> StringNodeFilter<Self, R>
    where
        Self::Output: StringComparable,
    {
        StringNodeFilter::new(self, StringOp::NotContains, rhs)
    }

    fn fuzzy_search<R: NodeExpr<Output = Self::Output>>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringNodeFilter<Self, R>
    where
        Self::Output: StringComparable,
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
        Self: NodeExpr<Output = Option<Inner>>,
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
        Self: NodeExpr<Output = Option<Inner>>,
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
        Self: NodeExpr<Output = Option<Prop>>,
    {
        self.eq(Prop::Bool(true))
    }

    fn is_false(self) -> BinaryCmpNodeFilter<Self, Prop>
    where
        Self: NodeExpr<Output = Option<Prop>>,
    {
        self.eq(Prop::Bool(false))
    }

    fn is_in<Inner, Iter>(self, values: Iter) -> SetNodeFilter<Self, Inner>
    where
        Self: NodeExpr<Output = Option<Inner>>,
        Inner: Eq + Hash + Clone + Send + Sync + 'static,
        Iter: IntoIterator<Item = Inner>,
    {
        let set: HashSet<_> = values.into_iter().collect();
        SetNodeFilter {
            expr: self,
            op: SetOp::IsIn,
            values: Arc::new(set),
            _phantom: PhantomData,
        }
    }

    fn is_not_in<Inner, Iter>(self, values: Iter) -> SetNodeFilter<Self, Inner>
    where
        Self: NodeExpr<Output = Option<Inner>>,
        Inner: Eq + Hash + Clone + Send + Sync + 'static,
        Iter: IntoIterator<Item = Inner>,
    {
        let set: HashSet<_> = values.into_iter().collect();
        SetNodeFilter {
            expr: self,
            op: SetOp::IsNotIn,
            values: Arc::new(set),
            _phantom: PhantomData,
        }
    }
}

impl<E: NodeExpr> NodeExprFilterOps for E {}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalExprOps — blanket trait for E: NodeExpr<Output = Prop>
// ─────────────────────────────────────────────────────────────────────────────

/// Quantifier and aggregator operators for temporal property sequences.
///
/// Available on any `NodeExpr<Output = Prop>` that returns a `Prop::List` (e.g. [`TemporalPropertyExpr`]).
pub trait TemporalExprOps: NodeExpr<Output = Prop> + Sized {
    fn any(self) -> QuantifiedContextBuilder<Self, AnyMode> {
        QuantifiedContextBuilder {
            expr: self,
            _q: PhantomData,
        }
    }

    fn all(self) -> QuantifiedContextBuilder<Self, AllMode> {
        QuantifiedContextBuilder {
            expr: self,
            _q: PhantomData,
        }
    }

    fn sum(self) -> SumExpr<Self> {
        SumExpr(self)
    }

    fn avg(self) -> AvgExpr<Self> {
        AvgExpr(self)
    }

    fn min(self) -> MinExpr<Self> {
        MinExpr(self)
    }

    fn max(self) -> MaxExpr<Self> {
        MaxExpr(self)
    }

    fn first(self) -> FirstExpr<Self> {
        FirstExpr(self)
    }

    fn last(self) -> LastExpr<Self> {
        LastExpr(self)
    }

    fn len(self) -> LenExpr<Self> {
        LenExpr(self)
    }
}

impl<E: NodeExpr<Output = Prop>> TemporalExprOps for E {}
