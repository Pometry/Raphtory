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
        AllNodeOp, AnyNodeOp, BinaryCmpNodeOp, ListAwareCmpNodeOp, ListAwareSetNodeOp,
        ListAwareStringNodeOp, ListAwareUnaryNodeOp, PropValueSetNodeOp, StringNodeOp, UnaryNodeOp,
    },
    CreateOp, EntityExpr, EntityExprBuilder, Marker,
};
use crate::{
    db::{
        api::{
            state::ops::NodeOp,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{
                coerce_set_values,
                edge_expr::{
                    ops::{
                        ListAwareCmpEdgeOp, ListAwareSetEdgeOp, ListAwareStringEdgeOp,
                        ListAwareUnaryEdgeOp,
                    },
                    EdgeOp,
                },
                elem_prop_type,
                filter_operator::{BinaryOp, ElemQual, SetOp, StringOp, UnaryOp},
                resolved_prop_type, validate_binary_op, validate_const_castable,
                validate_string_op, validate_types_compatible, ComposableFilter, CreateFilter,
                EntityMarker, ExplodedEdgeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
    prelude::{EdgeFilter, NodeFilter},
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

/// Collapse elementwise boolean results per the collected qualifiers,
/// innermost list level first, and adapt to a boolean node filter.
fn qualify_node_filter<'g>(
    elemwise: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    quals: &[ElemQual],
) -> Arc<dyn NodeOp<Output = bool> + 'g> {
    let mut op = elemwise;
    // Qualifiers are collected in call order (outermost list level first);
    // wrapping starts at the innermost level, so iterate in reverse.
    for q in quals.iter().rev() {
        op = match q {
            ElemQual::Any => Arc::new(AnyNodeOp { inner: op }),
            ElemQual::All => Arc::new(AllNodeOp { inner: op }),
        };
    }
    Arc::new(op.map(|v| matches!(v, Some(Prop::Bool(true)))))
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

    fn with_entity<T>(self, entity: T) -> BinaryCmpExpr<L, R, T> {
        BinaryCmpExpr {
            left: self.left,
            op: self.op,
            right: self.right,
            entity,
        }
    }
}

impl<L, R, E> ComposableFilter for BinaryCmpExpr<L, R, E> {}

impl<L: EntityExpr, R: EntityExpr, E: Marker> EntityExprBuilder for BinaryCmpExpr<L, R, E> {}

impl<L: EntityExpr, R: EntityExpr, E: Marker> EntityExpr for BinaryCmpExpr<L, R, E> {
    type Marker = E;
    fn entity(&self) -> Self::Marker {
        self.entity
    }
    fn prop_type(&self) -> PropType {
        // TODO: depending on the types of left and right, we should figure out the type to return here
        PropType::Empty
    }
}

impl<L: CreateOp, R: CreateOp, E: Marker> CreateOp for BinaryCmpExpr<L, R, E> {
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

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph)?;
        Ok(Arc::new(ListAwareCmpEdgeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<L, R> CreateFilter for BinaryCmpExpr<L, R, NodeFilter>
where
    L: CreateOp,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        NodeFilteredGraph<G, Self::NodeFilter<'graph, G, F>>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let filter = self.create_node_filter(graph.clone(), filtered)?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        let expr_pt = self.left.prop_type();
        let (left, quals) = self.left.create_qualified_node_op(filtered.clone())?;
        let right = self.right.create_node_op(filtered)?;
        let lhs_pt = elem_prop_type(&resolved_prop_type(expr_pt, left.prop_type()), quals.len())?;
        let rhs_pt = resolved_prop_type(self.right.prop_type(), right.prop_type());
        validate_binary_op(&self.op, &lhs_pt)?;
        match right.const_value() {
            Some(c) => validate_const_castable(&lhs_pt, c.as_ref())?,
            None => validate_types_compatible(&lhs_pt, &rhs_pt)?,
        }
        if quals.is_empty() {
            Ok(Arc::new(BinaryCmpNodeOp {
                left,
                right,
                op: self.op,
            }))
        } else {
            let elemwise = Arc::new(ListAwareCmpNodeOp {
                left,
                right,
                op: self.op,
            });
            Ok(qualify_node_filter(elemwise, &quals))
        }
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<L, R> CreateFilter for BinaryCmpExpr<L, R, EntityMarker>
where
    L: CreateOp<Marker = EntityMarker>,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn BoxableGraphView + 'graph>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        Ok(match self.entity {
            EntityMarker::Node => Arc::new(
                self.with_entity(NodeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::Edge => Arc::new(
                self.with_entity(EdgeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::ExplodedEdge => Arc::new(
                self.with_entity(ExplodedEdgeFilter)
                    .create_filter(graph, filtered)?,
            ),

            EntityMarker::Const => Err(GraphError::NotSupported)?,
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        match self.entity {
            EntityMarker::Node => Ok(self
                .with_entity(NodeFilter)
                .create_node_filter(graph, filtered)?),
            EntityMarker::Edge => Err(GraphError::NotNodeFilter),
            EntityMarker::ExplodedEdge => Err(GraphError::NotNodeFilter),
            EntityMarker::Const => Err(GraphError::NotSupported)?,
        }
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
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

impl<E: EntityExpr, M: Marker> EntityExprBuilder for UnaryExpr<E, M> {}

impl<E: EntityExpr, M: Marker> EntityExpr for UnaryExpr<E, M> {
    type Marker = M;
    fn entity(&self) -> Self::Marker {
        self.entity
    }
}

impl<E: CreateOp, M: Marker> CreateOp for UnaryExpr<E, M> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.expr.create_node_op(graph)?;
        Ok(Arc::new(ListAwareUnaryNodeOp { inner, op: self.op }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.expr.create_edge_op(graph)?;
        Ok(Arc::new(ListAwareUnaryEdgeOp { inner, op: self.op }))
    }
}

impl<E> CreateFilter for UnaryExpr<E, NodeFilter>
where
    E: CreateOp,
{
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        NodeFilteredGraph<G, Self::NodeFilter<'graph, G, F>>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let filter = self.create_node_filter(graph.clone(), filtered)?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        if !self.expr.nullable() {
            return Err(GraphError::InvalidFilter(format!(
                "operator {:?} is not valid for non-nullable expression",
                self.op
            )));
        }
        let (inner, quals) = self.expr.create_qualified_node_op(filtered)?;
        if quals.is_empty() {
            Ok(Arc::new(UnaryNodeOp { inner, op: self.op }))
        } else {
            let elemwise = Arc::new(ListAwareUnaryNodeOp { inner, op: self.op });
            Ok(qualify_node_filter(elemwise, &quals))
        }
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<E> CreateFilter for UnaryExpr<E, EntityMarker>
where
    E: CreateOp,
{
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn BoxableGraphView + 'graph>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        Ok(match self.entity {
            EntityMarker::Node => Arc::new(
                self.with_entity(NodeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::Edge => Arc::new(
                self.with_entity(EdgeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::ExplodedEdge => Arc::new(
                self.with_entity(ExplodedEdgeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::Const => Err(GraphError::NotSupported)?,
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        match self.entity {
            EntityMarker::Node => Ok(self
                .with_entity(NodeFilter)
                .create_node_filter(graph, filtered)?),
            EntityMarker::Edge => Err(GraphError::NotNodeFilter),
            EntityMarker::ExplodedEdge => Err(GraphError::NotNodeFilter),
            EntityMarker::Const => Err(GraphError::NotSupported)?,
        }
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
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
        StringExpr {
            left: self.left,
            op: self.op,
            right: self.right,
            entity,
        }
    }
}

impl<L, R, Entity> ComposableFilter for StringExpr<L, R, Entity> {}

impl<L: EntityExpr, R: EntityExpr, M: Marker> EntityExprBuilder for StringExpr<L, R, M> {}

impl<L: EntityExpr, R: EntityExpr, M: Marker> EntityExpr for StringExpr<L, R, M> {
    type Marker = M;
    fn entity(&self) -> Self::Marker {
        self.entity
    }
    fn prop_type(&self) -> PropType {
        PropType::Empty
    }
}

impl<L: CreateOp, R: CreateOp, M: Marker> CreateOp for StringExpr<L, R, M> {
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

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph)?;
        Ok(Arc::new(ListAwareStringEdgeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<L: CreateOp, R: CreateOp> CreateFilter for StringExpr<L, R, NodeFilter> {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        NodeFilteredGraph<G, Self::NodeFilter<'graph, G, F>>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let filter = self.create_node_filter(graph.clone(), filtered)?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        let expr_pt = self.left.prop_type();
        let (left, quals) = self.left.create_qualified_node_op(filtered.clone())?;
        let right = self.right.create_node_op(filtered)?;
        validate_string_op(&elem_prop_type(
            &resolved_prop_type(expr_pt, left.prop_type()),
            quals.len(),
        )?)?;
        match right.const_value() {
            Some(c) => validate_const_castable(&PropType::Str, c.as_ref())?,
            None => {}
        }
        if quals.is_empty() {
            Ok(Arc::new(StringNodeOp {
                left,
                right,
                op: self.op,
            }))
        } else {
            let elemwise = Arc::new(ListAwareStringNodeOp {
                left,
                right,
                op: self.op,
            });
            Ok(qualify_node_filter(elemwise, &quals))
        }
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<L: CreateOp, R: CreateOp> CreateFilter for StringExpr<L, R, EntityMarker> {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn BoxableGraphView + 'graph>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        Ok(match self.entity {
            EntityMarker::Node => Arc::new(
                self.with_entity(NodeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::Edge => Arc::new(
                self.with_entity(EdgeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::ExplodedEdge => Arc::new(
                self.with_entity(ExplodedEdgeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::Const => Err(GraphError::NotSupported)?,
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        match self.entity {
            EntityMarker::Node => Ok(self
                .with_entity(NodeFilter)
                .create_node_filter(graph, filtered)?),
            EntityMarker::Edge => Err(GraphError::NotNodeFilter),
            EntityMarker::ExplodedEdge => Err(GraphError::NotNodeFilter),
            EntityMarker::Const => Err(GraphError::NotSupported)?,
        }
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
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

impl<E: EntityExpr, M: Marker> EntityExprBuilder for PropValueSetExpr<E, M> {}

impl<E: EntityExpr, M: Marker> EntityExpr for PropValueSetExpr<E, M> {
    type Marker = M;
    fn entity(&self) -> Self::Marker {
        self.entity
    }
    fn prop_type(&self) -> PropType {
        PropType::Empty
    }
}

impl<E: CreateOp, M: Marker> CreateOp for PropValueSetExpr<E, M> {
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

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.expr.create_edge_op(graph)?;
        Ok(Arc::new(ListAwareSetEdgeOp {
            inner,
            values: self.values.clone(),
            op: self.op,
        }))
    }
}

impl<E: CreateOp> CreateFilter for PropValueSetExpr<E, NodeFilter> {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        NodeFilteredGraph<G, Self::NodeFilter<'graph, G, F>>;
    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let filter = self.create_node_filter(graph.clone(), filtered)?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        let expr_pt = self.expr.prop_type();
        let (inner, quals) = self.expr.create_qualified_node_op(filtered)?;
        let lhs_pt = elem_prop_type(&resolved_prop_type(expr_pt, inner.prop_type()), quals.len())?;
        let values = coerce_set_values(&lhs_pt, self.values)?;
        if quals.is_empty() {
            Ok(Arc::new(PropValueSetNodeOp {
                inner,
                values,
                op: self.op,
            }))
        } else {
            let elemwise = Arc::new(ListAwareSetNodeOp {
                inner,
                values,
                op: self.op,
            });
            Ok(qualify_node_filter(elemwise, &quals))
        }
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<E: CreateOp> CreateFilter for PropValueSetExpr<E, EntityMarker> {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn BoxableGraphView + 'graph>;
    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        Ok(match self.entity {
            EntityMarker::Node => Arc::new(
                self.with_entity(NodeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::Edge => Arc::new(
                self.with_entity(EdgeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::ExplodedEdge => Arc::new(
                self.with_entity(ExplodedEdgeFilter)
                    .create_filter(graph, filtered)?,
            ),
            EntityMarker::Const => Err(GraphError::NotSupported)?,
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        match self.entity {
            EntityMarker::Node => Ok(self
                .with_entity(NodeFilter)
                .create_node_filter(graph, filtered)?),
            EntityMarker::Edge => Err(GraphError::NotNodeFilter),
            EntityMarker::ExplodedEdge => Err(GraphError::NotNodeFilter),
            EntityMarker::Const => Err(GraphError::NotSupported)?,
        }
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

// ── The expr layer has no composite form: these filters exist only as compiled ops. ──
// The conversion is fallible by design, so "not representable" is an answer, not a lie;
// the composite path survives solely for its remaining GraphQL and grant-lowering consumers.

use crate::db::graph::views::filter::{
    edge_expr_filtered_graph::EdgeExprFilteredGraph,
    exploded_edge_expr_filtered_graph::ExplodedEdgeExprFilteredGraph,
    model::{
        edge_expr::filters::qualify_edge_filter,
        edge_filter::CompositeEdgeFilter,
        exploded_edge_filter::CompositeExplodedEdgeFilter,
        node_expr::exprs::{AllExpr, AnyExpr},
        node_filter::CompositeNodeFilter,
        FilterTree,
    },
};

/// A bare leading qualifier used directly as a filter keeps its historical
/// meaning: each element compares equal to `true` and the qualifier chain
/// collapses the results. One impl serves every entity, dispatching on the
/// runtime marker like the other expression filters with erased entities.
macro_rules! impl_qualifier_filter {
    ($($ty:ident),+ $(,)?) => {$(
        impl<E: CreateOp> CreateFilter for $ty<E>
        where
            E::Marker: Into<EntityMarker>,
        {
            type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
                Arc<dyn BoxableGraphView + 'graph>;
            type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
                Arc<dyn NodeOp<Output = bool> + 'graph>;

            type FilteredGraph<'graph, G>
                = G
            where
                Self: 'graph,
                G: GraphView + 'graph;

            fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
                self,
                graph: G,
                filtered: F,
            ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
                match self.0.entity().into() {
                    EntityMarker::Node => {
                        let filter = self.create_node_filter(graph.clone(), filtered)?;
                        Ok(Arc::new(NodeFilteredGraph::new(graph, filter)))
                    }
                    EntityMarker::Edge => {
                        let (left, mut quals) = self.create_qualified_edge_op(filtered.clone())?;
                        quals.reverse();
                        elem_prop_type(&left.prop_type(), quals.len())?;
                        let right = Prop::Bool(true).create_edge_op(filtered)?;
                        let elemwise = Arc::new(ListAwareCmpEdgeOp {
                            left,
                            right,
                            op: BinaryOp::Eq,
                        });
                        Ok(Arc::new(EdgeExprFilteredGraph::new(
                            graph,
                            qualify_edge_filter(elemwise, &quals),
                        )))
                    }
                    EntityMarker::ExplodedEdge => {
                        let (left, mut quals) = self.create_qualified_edge_op(filtered.clone())?;
                        quals.reverse();
                        elem_prop_type(&left.prop_type(), quals.len())?;
                        let right = Prop::Bool(true).create_edge_op(filtered)?;
                        let elemwise = Arc::new(ListAwareCmpEdgeOp {
                            left,
                            right,
                            op: BinaryOp::Eq,
                        });
                        Ok(Arc::new(ExplodedEdgeExprFilteredGraph::new(
                            graph,
                            qualify_edge_filter(elemwise, &quals),
                        )))
                    }
                    EntityMarker::Const => Err(GraphError::NotSupported),
                }
            }

            fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
                self,
                _graph: G,
                filtered: F,
            ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
                if !matches!(self.0.entity().into(), EntityMarker::Node) {
                    return Err(GraphError::NotNodeFilter);
                }
                let (left, mut quals) = self.create_qualified_node_op(filtered.clone())?;
                // Trailing qualifiers collect innermost level first; the
                // collapse helper wraps innermost first after reversing, so
                // reverse here to cancel it.
                quals.reverse();
                elem_prop_type(&left.prop_type(), quals.len())?;
                let right = Prop::Bool(true).create_node_op(filtered)?;
                let elemwise = Arc::new(ListAwareCmpNodeOp {
                    left,
                    right,
                    op: BinaryOp::Eq,
                });
                Ok(qualify_node_filter(elemwise, &quals))
            }

            fn filter_graph_view<'graph, G: GraphView + 'graph>(
                &self,
                graph: G,
            ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
                Ok(graph)
            }
        }

        impl<E: CreateOp> ComposableFilter for $ty<E> where E::Marker: Into<EntityMarker> {}

    )+};
}

impl_qualifier_filter!(AnyExpr, AllExpr);
