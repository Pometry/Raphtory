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
//!   ──► BinaryCmpExpr { left: Property("age"), op: Gt, right: 30i64 }
//!
//! Phase 2 — Compile (bind to graph, resolve names):
//!   BinaryCmpExpr::create_node_filter(graph)?
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
//! ──► BinaryCmpExpr<AnyExpr<BinaryCmpExpr<TemporalPropertyExpr, i64>>, Prop>
//!   create_node_filter(graph)?
//!   ──► BinaryCmpNodeOp { left: AnyNodeOp { inner: ListAwareCmpNodeOp { TemporalNodePropOp,
//!                                                                         Const(I64(10)), Gt } },
//!                          right: Const(Bool(true)), op: Eq }
//!
//! // "pass if sum of 'score' > 100"
//! NodeFilter.property("score").temporal().sum().gt(100i64)
//! ──► BinaryCmpExpr<SumExpr<TemporalPropertyExpr>, i64>
//! ```

use super::{
    ops::{
        BinaryCmpNodeOp, ListAwareCmpNodeOp, ListAwareSetNodeOp, ListAwareStringNodeOp,
        PropValueSetNodeOp, StringNodeOp, UnaryNodeOp,
    },
    AllExpr, AnyExpr, CreateOp, EntityExpr,
};
use crate::{
    db::{
        api::{
            state::ops::NodeOp,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                filter_operator::{BinaryOp, SetOp, StringOp, UnaryOp},
                ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter, CreateFilter,
                EntityMarker, ExplodedEdgeFilter, TryAsCompositeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
    prelude::{EdgeFilter, GraphViewOps, NodeFilter},
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;
// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpExpr<L, R> — binary expression filter
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that compares two [`CreateOp`] values using a [`BinaryOp`].
///
/// Both sides produce `Option<Prop>` at runtime. Created by [`EntityExprFilterOps`] methods
/// (`.gt`, `.lt`, `.eq`, `.ne`, `.ge`, `.le`).
///
/// As a **terminal filter** (`CreateFilter`): compiles to `BinaryCmpNodeOp` → bool.
/// As a **mid-chain expression** (`NodeExpr`): compiles to `ListAwareCmpNodeOp` → `Option<Prop::List([Bool]...)>`.
///
/// ```rust,ignore
/// NodeFilter.degree().gt(2usize)
///   → BinaryCmpExpr<DegreeExpr<..>, usize>
///   → BinaryCmpNodeOp { left: Degree(..).map(Prop::U64), right: Const(Some(U64(2))), op: Gt }
///
/// NodeFilter.property("age").eq(30i64)
///   → BinaryCmpExpr<Property, i64>
///   → BinaryCmpNodeOp { left: NodePropOp(prop_id=N), right: Const(Some(I64(30))), op: Eq }
/// ```
#[derive(Clone)]
pub struct BinaryCmpExpr<L, R, Entity> {
    pub left: L,
    pub op: BinaryOp,
    pub right: R,
    pub entity: Entity,
}

impl<L, R, E> BinaryCmpExpr<L, R, E> {
    pub fn new(left: L, op: BinaryOp, right: R, entity: E) -> Self {
        Self {
            left,
            op,
            right,
            entity,
        }
    }
}

impl<L, R, E> ComposableFilter for BinaryCmpExpr<L, R, E> {}

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

/// Pick the more specific of the two known prop types.
///
/// Compiled `NodeOp`s and `EntityExpr`s may both have a known prop type, but
/// expression-level info (e.g. `DegreeExpr::prop_type()` → U64) is not always
/// propagated through generic wrappers like `Map<Op, V>`. Prefer whichever side
/// has a concrete type so validation can fire early.
fn resolved_prop_type(expr_pt: PropType, op_pt: PropType) -> PropType {
    if expr_pt != PropType::Empty {
        expr_pt
    } else {
        op_pt
    }
}

/// Reject a constant RHS value whose type cannot be coerced to the LHS type.
///
/// Only fires when both sides are known and the RHS is a literal/const. Defers
/// to runtime when the LHS type is unknown (`PropType::Empty`) or the RHS isn't
/// a const value.
fn validate_const_castable(lhs_pt: &PropType, rhs_const: Option<&Prop>) -> Result<(), GraphError> {
    if *lhs_pt == PropType::Empty {
        return Ok(());
    }
    if let Some(rhs) = rhs_const {
        if rhs.dtype() != *lhs_pt && rhs.clone().try_cast(lhs_pt.clone()).is_none() {
            return Err(GraphError::InvalidFilter(format!(
                "value {:?} of type {} cannot be coerced to {}",
                rhs,
                rhs.dtype(),
                lhs_pt
            )));
        }
    }
    Ok(())
}

/// Cast every value in an `is_in`/`is_not_in` set to the LHS type.
///
/// If the LHS type is unknown (`PropType::Empty`), the values are returned
/// unchanged and coercion is deferred to runtime. Otherwise, any value whose
/// type cannot be coerced produces `Err(InvalidFilter)`. Successful casts are
/// substituted so the runtime set comparison sees same-typed values.
fn coerce_set_values(lhs_pt: &PropType, values: Vec<Prop>) -> Result<Vec<Prop>, GraphError> {
    if *lhs_pt == PropType::Empty {
        return Ok(values);
    }
    values
        .into_iter()
        .map(|v| {
            if v.dtype() == *lhs_pt {
                Ok(v)
            } else {
                let original_dtype = v.dtype();
                v.clone().try_cast(lhs_pt.clone()).ok_or_else(|| {
                    GraphError::InvalidFilter(format!(
                        "value {:?} of type {} cannot be coerced to {}",
                        v, original_dtype, lhs_pt
                    ))
                })
            }
        })
        .collect()
}

impl<L, R> CreateFilter for BinaryCmpExpr<L, R, NodeFilter>
where
    L: CreateOp,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, Self::NodeFilter<'graph, G>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = Arc<dyn NodeOp<Output = bool> + 'graph>;

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
        let expr_pt = self.left.prop_type();
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        let lhs_pt = resolved_prop_type(expr_pt, left.prop_type());
        validate_binary_op(&self.op, &lhs_pt)?;
        validate_const_castable(
            &lhs_pt,
            right.const_value().as_ref().and_then(|o| o.as_ref()),
        )?;
        Ok(Arc::new(BinaryCmpNodeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<L, R> CreateFilter for BinaryCmpExpr<L, R, EntityMarker>
where
    L: CreateOp,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    type NodeFilter<'graph, G: GraphView + 'graph> = Arc<dyn NodeOp<Output = bool> + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(match self.entity {
            EntityMarker::Node => Arc::new(
                BinaryCmpExpr::new(self.left, self.op, self.right, NodeFilter)
                    .create_filter(graph)?,
            ),
            EntityMarker::Edge => Arc::new(
                BinaryCmpExpr::new(self.left, self.op, self.right, EdgeFilter)
                    .create_filter(graph)?,
            ),
            EntityMarker::ExplodedEdge => Arc::new(
                BinaryCmpExpr::new(self.left, self.op, self.right, ExplodedEdgeFilter)
                    .create_filter(graph)?,
            ),
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        match self.entity {
            EntityMarker::Node => Ok(BinaryCmpExpr::new(
                self.left, self.op, self.right, NodeFilter,
            )
            .create_node_filter(graph)?),
            EntityMarker::Edge => Err(GraphError::NotEdgeFilter),
            EntityMarker::ExplodedEdge => Err(GraphError::NotExplodedEdgeFilter),
        }
    }
}

impl<L, R> TryAsCompositeFilter for BinaryCmpExpr<L, R, NodeFilter>
where
    L: CreateOp,
    R: CreateOp,
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
// UnaryExpr<E> — is_some / is_none on nullable expressions
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that tests the presence of an `Option`-valued expression.
///
/// Created by `.is_some()` / `.is_none()` on any `NodeExpr<Output = Option<I>>`.
/// Compiles to a `UnaryNodeOp { inner, op }`.
///
/// ```rust,ignore
/// NodeFilter.property("age").is_some::<Prop>()
///   → UnaryExpr<Property, Prop>
///   → UnaryNodeOp { inner: NodePropOp(prop_id=N), op: IsSome }
/// ```
#[derive(Clone)]
pub struct UnaryExpr<E, Entity> {
    pub expr: E,
    pub op: UnaryOp,
    pub entity: Entity,
}

impl<E, Entity> UnaryExpr<E, Entity> {
    fn with_entity<T>(self, entity: T) -> UnaryExpr<E, T> {
        UnaryExpr {
            expr: self.expr,
            op: self.op,
            entity,
        }
    }
}

impl<E, Entity> ComposableFilter for UnaryExpr<E, Entity> {}

impl<E> CreateFilter for UnaryExpr<E, NodeFilter>
where
    E: CreateOp,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, UnaryNodeOp<'graph, Prop>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = UnaryNodeOp<'graph, Prop>;

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
        if !self.expr.nullable() {
            return Err(GraphError::InvalidFilter(format!(
                "operator {:?} is not valid for non-nullable expression",
                self.op
            )));
        }
        let inner = self.expr.create_node_op(graph)?;
        Ok(UnaryNodeOp { inner, op: self.op })
    }
}

impl<E> CreateFilter for UnaryExpr<E, EntityMarker>
where
    E: CreateOp,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    type NodeFilter<'graph, G: GraphView + 'graph> = UnaryNodeOp<'graph, Prop>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(match self.entity {
            EntityMarker::Node => Arc::new(self.with_entity(NodeFilter).create_filter(graph)?),
            EntityMarker::Edge => Arc::new(self.with_entity(EdgeFilter).create_filter(graph)?),
            EntityMarker::ExplodedEdge => {
                Arc::new(self.with_entity(ExplodedEdgeFilter).create_filter(graph)?)
            }
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        match self.entity {
            EntityMarker::Node => Ok(self.with_entity(NodeFilter).create_node_filter(graph)?),
            EntityMarker::Edge => Err(GraphError::NotEdgeFilter),
            EntityMarker::ExplodedEdge => Err(GraphError::NotExplodedEdgeFilter),
        }
    }
}

impl<E> TryAsCompositeFilter for UnaryExpr<E, NodeFilter>
where
    E: CreateOp,
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
// StringExpr<L, R> — string expression filter
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that applies a [`StringOp`] to two [`CreateOp`] values.
///
/// Both sides must produce the same string-comparable type (`L::Output: StringComparable`).
/// Created by the string methods on [`EntityExprFilterOps`] (`.starts_with`, `.ends_with`,
/// `.contains`, `.not_contains`, `.fuzzy_search`).
/// Compiles to a `StringNodeOp` wrapped in `Arc<dyn NodeOp<Output = bool>>`.
///
/// ```rust,ignore
/// NodeFilter.name().starts_with("Al")
///   → StringExpr<Name, &str>
///   → StringNodeOp { left: Name.map(...), right: Const(Some(Str("Al"))), op: StartsWith }
///
/// NodeFilter.property("tag").contains(Prop::Str("foo".into()))
///   → StringExpr<Property, Prop>
///   → StringNodeOp { left: NodePropOp(prop_id=N), right: Const(Some(Str("foo"))), op: Contains }
/// ```
#[derive(Clone)]
pub struct StringExpr<L, R, Entity> {
    pub left: L,
    pub op: StringOp,
    pub right: R,
    pub entity: Entity,
}

impl<L, R, Entity> StringExpr<L, R, Entity> {
    pub fn new(left: L, op: StringOp, right: R, entity: Entity) -> Self {
        Self {
            left,
            op,
            right,
            entity,
        }
    }

    fn with_entity<T>(self, entity: T) -> StringExpr<L, R, T> {
        Self {
            left: self.left,
            op: self.op,
            right: self.right,
            entity,
        }
    }
}

impl<L, R, Entity> ComposableFilter for StringExpr<L, R, Entity> {}

impl<L: CreateOp, R: CreateOp> CreateFilter for StringExpr<L, R, NodeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, Self::NodeFilter<'graph, G>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = Arc<dyn NodeOp<Output = bool> + 'graph>;

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
        let expr_pt = self.left.prop_type();
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        validate_string_op(&resolved_prop_type(expr_pt, left.prop_type()))?;
        Ok(Arc::new(StringNodeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<L: CreateOp, R: CreateOp> CreateFilter for StringExpr<L, R, EntityMarker> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    type NodeFilter<'graph, G: GraphView + 'graph> = Arc<dyn NodeOp<Output = bool> + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(match self.entity {
            EntityMarker::Node => Arc::new(self.with_entity(NodeFilter).create_filter(graph)?),
            EntityMarker::Edge => Arc::new(self.with_entity(EdgeFilter).create_filter(graph)?),
            EntityMarker::ExplodedEdge => {
                Arc::new(self.with_entity(ExplodedEdgeFilter).create_filter(graph)?)
            }
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        match self.entity {
            EntityMarker::Node => Ok(self.with_entity(NodeFilter).create_node_filter(graph)?),
            EntityMarker::Edge => Err(GraphError::NotNodeFilter),
            EntityMarker::ExplodedEdge => Err(GraphError::NotExplodedEdgeFilter),
        }
    }
}
impl<L, R> TryAsCompositeFilter for StringExpr<L, R, NodeFilter>
where
    L: CreateOp,
    R: CreateOp,
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
// PropValueSetExpr<E> — is_in / is_not_in for aggregated Option<Prop> values
// ─────────────────────────────────────────────────────────────────────────────

/// A filter that checks whether a scalar property value is in (or not in) a fixed set.
///
/// Uses linear scan because `Prop` may contain floats that don't implement `Hash`.
/// Works for both nodes (`Entity = NodeFilter`) and edges (`Entity = EdgeFilter`).
#[derive(Clone)]
pub struct PropValueSetExpr<E, Entity> {
    pub(crate) expr: E,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
    pub(crate) entity: Entity,
}

impl<E, Entity> PropValueSetExpr<E, Entity> {
    fn with_entity<T>(self, entity: T) -> PropValueSetExpr<E, T> {
        PropValueSetExpr {
            expr: self.expr,
            values: self.values,
            op: self.op,
            entity,
        }
    }
}

impl<E, Entity> ComposableFilter for PropValueSetExpr<E, Entity> {}

impl<E: CreateOp> CreateFilter for PropValueSetExpr<E, NodeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, PropValueSetNodeOp<'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = PropValueSetNodeOp<'graph>;

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
        let expr_pt = self.expr.prop_type();
        let inner = self.expr.create_node_op(graph)?;
        let lhs_pt = resolved_prop_type(expr_pt, inner.prop_type());
        let values = coerce_set_values(&lhs_pt, self.values)?;
        Ok(PropValueSetNodeOp {
            inner,
            values,
            op: self.op,
        })
    }
}

impl<E: CreateOp> CreateFilter for PropValueSetExpr<E, EntityMarker> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;
    type NodeFilter<'graph, G: GraphView + 'graph> = PropValueSetNodeOp<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(match self.entity {
            EntityMarker::Node => Arc::new(self.with_entity(NodeFilter).create_filter(graph)?),
            EntityMarker::Edge => Arc::new(self.with_entity(EdgeFilter).create_filter(graph)?),
            EntityMarker::ExplodedEdge => {
                Arc::new(self.with_entity(ExplodedEdgeFilter).create_filter(graph)?)
            }
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        match self.entity {
            EntityMarker::Node => Ok(self.with_entity(NodeFilter).create_node_filter(graph)?),
            EntityMarker::Edge => Err(GraphError::NotEdgeFilter),
            EntityMarker::ExplodedEdge => Err(GraphError::NotExplodedEdgeFilter),
        }
    }
}

impl<E: CreateOp> TryAsCompositeFilter for PropValueSetExpr<E, NodeFilter> {
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
// EntityExprFilterOps — comparison and set operators on any EntityExpr
// ─────────────────────────────────────────────────────────────────────────────

/// Comparison, string, set, and presence operators on any [`CreateOp`].
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
    fn gt<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Gt, rhs, entity)
    }

    fn ge<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Ge, rhs, entity)
    }

    fn lt<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Lt, rhs, entity)
    }

    fn le<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Le, rhs, entity)
    }

    fn eq<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Eq, rhs, entity)
    }

    fn ne<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Ne, rhs, entity)
    }

    fn starts_with<R: EntityExpr>(self, rhs: R) -> StringExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        StringExpr::new(self, StringOp::StartsWith, rhs, entity)
    }

    fn ends_with<R: EntityExpr>(self, rhs: R) -> StringExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        StringExpr::new(self, StringOp::EndsWith, rhs, entity)
    }

    fn contains<R: EntityExpr>(self, rhs: R) -> StringExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        StringExpr::new(self, StringOp::Contains, rhs, entity)
    }

    fn not_contains<R: EntityExpr>(self, rhs: R) -> StringExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        StringExpr::new(self, StringOp::NotContains, rhs, entity)
    }

    fn fuzzy_search<R: EntityExpr>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringExpr<Self, R, Self::Marker> {
        let entity = self.entity();
        StringExpr::new(
            self,
            StringOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            rhs,
            entity,
        )
    }

    fn is_some(self) -> UnaryExpr<Self, Self::Marker> {
        let entity = self.entity();
        UnaryExpr {
            expr: self,
            op: UnaryOp::IsSome,
            entity,
        }
    }

    fn is_none(self) -> UnaryExpr<Self, Self::Marker> {
        let entity = self.entity();
        UnaryExpr {
            expr: self,
            op: UnaryOp::IsNone,
            entity,
        }
    }

    fn is_in<V: Into<Prop>>(
        self,
        values: impl IntoIterator<Item = V>,
    ) -> PropValueSetExpr<Self, Self::Marker> {
        let entity = self.entity();
        PropValueSetExpr {
            expr: self,
            values: values.into_iter().map(Into::into).collect(),
            op: SetOp::IsIn,
            entity,
        }
    }

    fn is_not_in<V: Into<Prop>>(
        self,
        values: impl IntoIterator<Item = V>,
    ) -> PropValueSetExpr<Self, Self::Marker> {
        let entity = self.entity();
        PropValueSetExpr {
            expr: self,
            values: values.into_iter().map(Into::into).collect(),
            op: SetOp::IsNotIn,
            entity,
        }
    }

    fn is_true(self) -> BinaryCmpExpr<Self, Prop, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Eq, Prop::Bool(true), entity)
    }

    fn is_false(self) -> BinaryCmpExpr<Self, Prop, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(self, BinaryOp::Eq, Prop::Bool(false), entity)
    }

    fn not(self) -> BinaryCmpExpr<Self, Prop, Self::Marker> {
        self.eq(Prop::Bool(false))
    }

    fn any(self) -> BinaryCmpExpr<AnyExpr<Self>, Prop, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(AnyExpr(self), BinaryOp::Eq, Prop::Bool(true), entity)
    }

    fn all(self) -> BinaryCmpExpr<AllExpr<Self>, Prop, Self::Marker> {
        let entity = self.entity();
        BinaryCmpExpr::new(AllExpr(self), BinaryOp::Eq, Prop::Bool(true), entity)
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

impl<L: EntityExpr, R: EntityExpr, E: Copy + Default + Send + Sync + 'static> EntityExpr
    for BinaryCmpExpr<L, R, E>
{
    type Marker = E;
    fn entity(&self) -> Self::Marker {
        self.entity
    }
    fn prop_type(&self) -> PropType {
        // TODO: depending on the types of left and right, we should figure out the type to return here
        PropType::Empty
    }
}

impl<L: CreateOp, R: CreateOp> CreateOp for BinaryCmpExpr<L, R, NodeFilter> {
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

impl<L: EntityExpr, R: EntityExpr, Entity: Copy + Default + Send + Sync + 'static> EntityExpr
    for StringExpr<L, R, Entity>
{
    type Marker = Entity;
    fn entity(&self) -> Self::Marker {
        self.entity
    }
    fn prop_type(&self) -> PropType {
        PropType::Empty
    }
}

impl<L: CreateOp, R: CreateOp> CreateOp for StringExpr<L, R, NodeFilter> {
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

impl<E: EntityExpr, Entity: Copy + Default + Send + Sync + 'static> EntityExpr
    for PropValueSetExpr<E, Entity>
{
    type Marker = Entity;
    fn entity(&self) -> Self::Marker {
        self.entity
    }
    fn prop_type(&self) -> PropType {
        PropType::Empty
    }
}

impl<E: EntityExpr, Entity: Copy + Default + Send + Sync + 'static> EntityExpr
    for UnaryExpr<E, Entity>
{
    type Marker = Entity;
    fn entity(&self) -> Self::Marker {
        self.entity
    }
}

impl<E: CreateOp> CreateOp for PropValueSetExpr<E, NodeFilter> {
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
