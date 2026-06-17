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
        BinaryCmpNodeOp, ListAwareCmpNodeOp, ListAwareSetNodeOp, ListAwareStringNodeOp,
        PropValueSetNodeOp, StringNodeOp, UnaryNodeOp,
    },
    AllExpr, AnyExpr, EntityExpr, NodeExpr, NodeExprMarker,
};
use crate::{
    db::{
        api::{state::ops::NodeOp, view::internal::GraphView},
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                filter_operator::{BinaryOp, SetOp, StringOp, UnaryOp},
                node_filter::NodeFilterFactory,
                ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter, CreateFilter,
                CreateView, MetadataExpr, PropertyExpr, TryAsCompositeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, NodeFilter},
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;
// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpNodeFilter<L, R> — binary expression filter
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that compares two [`NodeExpr`] values using a [`BinaryOp`].
///
/// Both sides produce `Option<Prop>` at runtime. Created by [`EntityExprFilterOps`] methods
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
#[derive(Clone)]
pub struct BinaryCmpFilter<L, R, Entity> {
    pub left: L,
    pub op: BinaryOp,
    pub right: R,
    pub entity: Entity,
}

impl<L, R, E> BinaryCmpFilter<L, R, E> {
    pub fn new(left: L, op: BinaryOp, right: R, entity: E) -> Self {
        Self {
            left,
            op,
            right,
            entity,
        }
    }
}

impl<L, R, E> ComposableFilter for BinaryCmpFilter<L, R, E> {}

/// Reject ordering operators on boolean properties.
//. TODO: Also check if both the types are comparable.
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

impl<L, R> CreateFilter for BinaryCmpFilter<L, R, NodeFilter>
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
        // TODO: validate_binary_op(&self.op, &left.prop_type(), &right.prop_type())?;
        Ok(Arc::new(BinaryCmpNodeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<L, R> TryAsCompositeFilter for BinaryCmpFilter<L, R, NodeFilter>
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
#[derive(Clone)]
pub struct UnaryFilter<E, Entity> {
    pub expr: E,
    pub op: UnaryOp,
    pub entity: Entity,
}

impl<E, Entity> ComposableFilter for UnaryFilter<E, Entity> {}

impl<E> CreateFilter for UnaryFilter<E, NodeFilter>
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

impl<E> TryAsCompositeFilter for UnaryFilter<E, NodeFilter>
where
    E: NodeExpr,
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
/// Created by the string methods on [`EntityExprFilterOps`] (`.starts_with`, `.ends_with`,
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
#[derive(Clone)]
pub struct StringFilter<L, R, Entity> {
    pub left: L,
    pub op: StringOp,
    pub right: R,
    pub entity: Entity,
}

impl<L, R, Entity> StringFilter<L, R, Entity> {
    pub fn new(left: L, op: StringOp, right: R, entity: Entity) -> Self {
        Self { left, op, right, entity }
    }
}

impl<L, R, Entity> ComposableFilter for StringFilter<L, R, Entity> {}

impl<L: NodeExpr, R: NodeExpr> CreateFilter for StringFilter<L, R, NodeFilter> {
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

impl<L, R> TryAsCompositeFilter for StringFilter<L, R, NodeFilter>
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

/// A filter that checks whether a scalar property value is in (or not in) a fixed set.
///
/// Uses linear scan because `Prop` may contain floats that don't implement `Hash`.
/// Works for both nodes (`Entity = NodeFilter`) and edges (`Entity = EdgeFilter`).
#[derive(Clone)]
pub struct PropValueSetFilter<E, Entity> {
    pub(crate) expr: E,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
    pub(crate) entity: Entity,
}

impl<E, Entity> ComposableFilter for PropValueSetFilter<E, Entity> {}

impl<E: NodeExpr> CreateFilter for PropValueSetFilter<E, NodeFilter> {
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

impl<E: NodeExpr> TryAsCompositeFilter for PropValueSetFilter<E, NodeFilter> {
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
// EntityExprFilterOps — comparison and set operators on any EntityExpr
// ─────────────────────────────────────────────────────────────────────────────

/// Comparison, string, set, and presence operators on any [`NodeExpr`].
///
/// `.any()` / `.all()` are terminal: they wrap `self` in `AnyExpr`/`AllExpr` and compare the
/// result to `Bool(true)`. For element-wise comparison before reduction, chain in order:
/// `.gt(10i64).any()` not `.any().gt(10i64)`.
///
/// ```rust,ignore
/// NodeFilter.degree().gt(2usize)
/// NodeFilter.degree().sum() // TODO: Throw an error
/// NodeFilter.out_degree().gt(NodeFilter.in_degree())
/// NodeFilter.property("age").gt(30i64)
/// NodeFilter.property("score").temporal().gt(10i64).any()
/// ```
pub trait EntityExprFilterOps: EntityExpr + Sized {
    fn gt<R: EntityExpr>(self, rhs: R) -> BinaryCmpFilter<Self, R, Self::Marker> {
        // TODO: validate ops
        BinaryCmpFilter::new(self, BinaryOp::Gt, rhs, Self::Marker::default())
    }

    fn ge<R: EntityExpr>(self, rhs: R) -> BinaryCmpFilter<Self, R, Self::Marker> {
        BinaryCmpFilter::new(self, BinaryOp::Ge, rhs, Self::Marker::default())
    }

    fn lt<R: EntityExpr>(self, rhs: R) -> BinaryCmpFilter<Self, R, Self::Marker> {
        BinaryCmpFilter::new(self, BinaryOp::Lt, rhs, Self::Marker::default())
    }

    fn le<R: EntityExpr>(self, rhs: R) -> BinaryCmpFilter<Self, R, Self::Marker> {
        BinaryCmpFilter::new(self, BinaryOp::Le, rhs, Self::Marker::default())
    }

    fn eq<R: EntityExpr>(self, rhs: R) -> BinaryCmpFilter<Self, R, Self::Marker> {
        BinaryCmpFilter::new(self, BinaryOp::Eq, rhs, Self::Marker::default())
    }

    fn ne<R: EntityExpr>(self, rhs: R) -> BinaryCmpFilter<Self, R, Self::Marker> {
        BinaryCmpFilter::new(self, BinaryOp::Ne, rhs, Self::Marker::default())
    }

    fn starts_with<R: EntityExpr>(self, rhs: R) -> StringFilter<Self, R, Self::Marker> {
        StringFilter::new(self, StringOp::StartsWith, rhs, Self::Marker::default())
    }

    fn ends_with<R: EntityExpr>(self, rhs: R) -> StringFilter<Self, R, Self::Marker> {
        StringFilter::new(self, StringOp::EndsWith, rhs, Self::Marker::default())
    }

    fn contains<R: EntityExpr>(self, rhs: R) -> StringFilter<Self, R, Self::Marker> {
        StringFilter::new(self, StringOp::Contains, rhs, Self::Marker::default())
    }

    fn not_contains<R: EntityExpr>(self, rhs: R) -> StringFilter<Self, R, Self::Marker> {
        StringFilter::new(self, StringOp::NotContains, rhs, Self::Marker::default())
    }

    fn fuzzy_search<R: EntityExpr>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringFilter<Self, R, Self::Marker> {
        StringFilter::new(
            self,
            StringOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            rhs,
            Self::Marker::default(),
        )
    }

    fn is_some(self) -> UnaryFilter<Self, Self::Marker> {
        UnaryFilter {
            expr: self,
            op: UnaryOp::IsSome,
            entity: Self::Marker::default(),
        }
    }

    fn is_none(self) -> UnaryFilter<Self, Self::Marker> {
        UnaryFilter {
            expr: self,
            op: UnaryOp::IsNone,
            entity: Self::Marker::default(),
        }
    }

    fn is_in(self, values: impl IntoIterator<Item = Prop>) -> PropValueSetFilter<Self, Self::Marker> {
        PropValueSetFilter {
            expr: self,
            values: values.into_iter().collect(),
            op: SetOp::IsIn,
            entity: Self::Marker::default(),
        }
    }

    fn is_not_in(self, values: impl IntoIterator<Item = Prop>) -> PropValueSetFilter<Self, Self::Marker> {
        PropValueSetFilter {
            expr: self,
            values: values.into_iter().collect(),
            op: SetOp::IsNotIn,
            entity: Self::Marker::default(),
        }
    }

    fn is_true(self) -> BinaryCmpFilter<Self, Prop, Self::Marker> {
        BinaryCmpFilter::new(self, BinaryOp::Eq, Prop::Bool(true), Self::Marker::default())
    }

    fn is_false(self) -> BinaryCmpFilter<Self, Prop, Self::Marker> {
        BinaryCmpFilter::new(self, BinaryOp::Eq, Prop::Bool(false), Self::Marker::default())
    }

    fn not(self) -> BinaryCmpFilter<Self, Prop, Self::Marker> {
        self.eq(Prop::Bool(false))
    }

    fn any(self) -> BinaryCmpFilter<AnyExpr<Self>, Prop, Self::Marker> {
        BinaryCmpFilter::new(AnyExpr(self), BinaryOp::Eq, Prop::Bool(true), Self::Marker::default())
    }

    fn all(self) -> BinaryCmpFilter<AllExpr<Self>, Prop, Self::Marker> {
        BinaryCmpFilter::new(AllExpr(self), BinaryOp::Eq, Prop::Bool(true), Self::Marker::default())
    }
}

impl<E: EntityExpr> EntityExprFilterOps for E {}

// ─────────────────────────────────────────────────────────────────────────────
// NodeExpr impls for filter types — enables mid-chain use before .any()/.all()
//
// e.g. temporal().sum().gt(5).any()
//      temporal().contains("rock").all()
//      temporal().is_in([...]).any()
// ─────────────────────────────────────────────────────────────────────────────

impl<L: EntityExpr, R: EntityExpr, E: Copy + Default + Send + Sync + 'static> EntityExpr for BinaryCmpFilter<L, R, E> {
    type Marker = E;
    fn prop_type(&self) -> PropType {
        // TODO: depending on the types of left and right, we should figure out the type to return here
        PropType::Empty
    }
}

impl<L: NodeExprMarker, R, E> NodeExprMarker for BinaryCmpFilter<L, R, E> {}

impl<L: NodeExpr, R: NodeExpr> NodeExpr for BinaryCmpFilter<L, R, NodeFilter> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        Ok(Arc::new(ListAwareCmpNodeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<L: EntityExpr, R: EntityExpr, Entity: Copy + Default + Send + Sync + 'static> EntityExpr for StringFilter<L, R, Entity> {
    type Marker = Entity;
    fn prop_type(&self) -> PropType {
        PropType::Empty
    }
}

impl<L: NodeExpr, R: NodeExpr> NodeExpr for StringFilter<L, R, NodeFilter> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        Ok(Arc::new(ListAwareStringNodeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<E: EntityExpr, Entity: Copy + Default + Send + Sync + 'static> EntityExpr for PropValueSetFilter<E, Entity> {
    type Marker = Entity;
    fn prop_type(&self) -> PropType {
        PropType::Empty
    }
}

impl<E: EntityExpr, Entity: Copy + Default + Send + Sync + 'static> EntityExpr for UnaryFilter<E, Entity> {
    type Marker = Entity;
}

impl<E: NodeExpr> NodeExpr for PropValueSetFilter<E, NodeFilter> {
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
