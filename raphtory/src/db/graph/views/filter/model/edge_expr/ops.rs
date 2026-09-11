//! Runtime edge evaluators — given an EdgeRef, return a typed value.
//!
//! Parallel to `node_expr/ops.rs` — same design, different subject.

use crate::{
    db::{
        api::{
            properties::internal::{InternalMetadataOps, InternalTemporalPropertyViewOps},
            state::ops::Const,
            view::internal::GraphView,
        },
        graph::edge::EdgeView,
    },
    prelude::EdgeViewOps,
};
use raphtory_api::core::entities::{
    edges::edge_ref::EdgeRef,
    properties::prop::{Prop, PropType},
};
use raphtory_storage::graph::graph::GraphStorage;

use super::EdgeOp;
use crate::db::{
    api::state::ops::NodeOp,
    graph::views::filter::model::{
        edge_filter::Endpoint,
        filter_operator::{BinaryOp, Comparable},
        node_expr::ops::{broadcast_binary, broadcast_unary},
    },
};
use raphtory_api::core::entities::properties::prop::PropArray;
use std::sync::Arc;

use crate::db::graph::views::filter::model::{
    filter_operator::UnaryOp, SetOp, StringComparable, StringOp,
};
// ─────────────────────────────────────────────────────────────────────────────
// Arc<dyn EdgeOp> — blanket impl so Arc-boxed ops satisfy EdgeOp
// ─────────────────────────────────────────────────────────────────────────────

impl<'a, V: Clone + Send + Sync> EdgeOp for Arc<dyn EdgeOp<Output = V> + 'a> {
    type Output = V;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> V {
        self.as_ref().apply(storage, edge)
    }

    fn prop_type(&self) -> PropType {
        self.as_ref().prop_type()
    }

    fn const_value(&self) -> Option<V> {
        self.as_ref().const_value()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Const<V> — constant literal op (RHS in comparisons)
// ─────────────────────────────────────────────────────────────────────────────

impl<V: Clone + Send + Sync + 'static> EdgeOp for Const<V> {
    type Output = V;

    fn apply(&self, _storage: &GraphStorage, _edge: EdgeRef) -> V {
        self.0.clone()
    }

    fn const_value(&self) -> Option<V> {
        Some(self.0.clone())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgePropOp<G> — latest temporal property value by pre-resolved column ID
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct EdgePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> EdgeOp for EdgePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        EdgeView::new(&self.graph, edge).temporal_value(self.prop_id)
    }

    fn prop_type(&self) -> PropType {
        self.graph
            .edge_meta()
            .temporal_prop_mapper()
            .get_dtype(self.prop_id)
            .unwrap_or_default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeMetaOp<G> — static metadata field by pre-resolved column ID
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct EdgeMetaOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> EdgeOp for EdgeMetaOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        EdgeView::new(&self.graph, edge).get_metadata(self.prop_id)
    }

    // No declared type: the runtime shape depends on the edge's layers (a
    // multi-layer edge yields a map keyed by layer, a single-layer edge the
    // plain value), so comparisons defer to runtime coercion.
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalEdgePropOp<G> — all temporal values for a property in the view window
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct TemporalEdgePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> EdgeOp for TemporalEdgePropOp<G> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        self.graph
            .edge_meta()
            .temporal_prop_mapper()
            .get_dtype(self.prop_id)
            .map_or(PropType::Empty, |dt| PropType::List(Box::new(dt)))
    }

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        let vals: Vec<Prop> = EdgeView::new(&self.graph, edge)
            .temporal_iter(self.prop_id)
            .map(|(_, v)| v)
            .collect();
        Some(Prop::List(PropArray::from(vals)))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpEdgeOp<'g> — compares two EdgeOp outputs, returns bool
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct BinaryCmpEdgeOp<'g, L> {
    pub(crate) left: Arc<dyn EdgeOp<Output = L> + 'g>,
    pub(crate) right: Arc<dyn EdgeOp<Output = L> + 'g>,
    pub(crate) op: BinaryOp,
}

impl<'g, L: Comparable + Clone + Send + Sync + 'static> EdgeOp for BinaryCmpEdgeOp<'g, L> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        let l = self.left.apply(storage, edge);
        let r = self.right.apply(storage, edge);
        L::binary_cmp(&self.op, &l, &r)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnaryEdgeOp<'g, I> — is_some / is_none on Option<I>-valued expressions
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct UnaryEdgeOp<'g, I: Clone + Send + Sync + 'static> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<I>> + 'g>,
    pub(crate) op: UnaryOp,
}

impl<'g, I: Clone + Send + Sync + 'static> EdgeOp for UnaryEdgeOp<'g, I> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        let v = self.inner.apply(storage, edge);
        match self.op {
            UnaryOp::IsSome => v.is_some(),
            UnaryOp::IsNone => v.is_none(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// StringEdgeOp<'g, T> — applies a StringOp to two EdgeOp<Output = T> values
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct StringEdgeOp<'g, T: StringComparable> {
    pub(crate) left: Arc<dyn EdgeOp<Output = T> + 'g>,
    pub(crate) right: Arc<dyn EdgeOp<Output = T> + 'g>,
    pub(crate) op: StringOp,
}

impl<'g, T: StringComparable> EdgeOp for StringEdgeOp<'g, T> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        T::string_cmp(
            &self.op,
            &self.left.apply(storage, edge),
            &self.right.apply(storage, edge),
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropValueSetEdgeOp<'g> — is_in / is_not_in for Option<Prop> (linear scan)
// ─────────────────────────────────────────────────────────────────────────────

/// Checks whether an `Option<Prop>` value is in (or not in) a fixed `Vec<Prop>`.
/// Uses linear scan because `Prop` may contain floats that don't implement `Hash`.
#[derive(Clone)]
pub(crate) struct PropValueSetEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
}

impl<'g> EdgeOp for PropValueSetEdgeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        match self.inner.apply(storage, edge) {
            None => false,
            Some(v) => match self.op {
                SetOp::IsIn => self
                    .values
                    .iter()
                    .any(|x| Prop::binary_cmp(&BinaryOp::Eq, x, &v)),
                SetOp::IsNotIn => self
                    .values
                    .iter()
                    .all(|x| Prop::binary_cmp(&BinaryOp::Ne, x, &v)),
            },
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ListAwareCmpEdgeOp<'g> — element-wise comparison via broadcast_binary
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct ListAwareCmpEdgeOp<'g> {
    pub(crate) left: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) right: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) op: BinaryOp,
}

impl<'g> EdgeOp for ListAwareCmpEdgeOp<'g> {
    type Output = Option<Prop>;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        let lv = self.left.apply(storage, edge);
        let rhs = self.right.apply(storage, edge);
        let op = &self.op;
        broadcast_binary(lv, rhs, &|lv, rhs| {
            Some(Prop::Bool(Prop::binary_cmp(op, &lv?, &rhs?)))
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ListAwareStringEdgeOp<'g> — element-wise string comparison via broadcast_binary
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct ListAwareStringEdgeOp<'g> {
    pub(crate) left: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) right: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) op: StringOp,
}

impl<'g> EdgeOp for ListAwareStringEdgeOp<'g> {
    type Output = Option<Prop>;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        let lv = self.left.apply(storage, edge);
        let rhs = self.right.apply(storage, edge);
        let op = &self.op;
        broadcast_binary(lv, rhs, &|lv, rhs| {
            Some(Prop::Bool(Option::<Prop>::string_cmp(op, &lv, &rhs)))
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ListAwareSetEdgeOp<'g> — element-wise set membership via broadcast_unary
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct ListAwareSetEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
}

impl<'g> EdgeOp for ListAwareSetEdgeOp<'g> {
    type Output = Option<Prop>;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        let vals = self.inner.apply(storage, edge);
        let values = &self.values;
        let op = &self.op;
        broadcast_unary(vals, |v| {
            let v = v?;
            Some(Prop::Bool(match op {
                SetOp::IsIn => values
                    .iter()
                    .any(|x| Prop::binary_cmp(&BinaryOp::Eq, x, &v)),
                SetOp::IsNotIn => values
                    .iter()
                    .all(|x| Prop::binary_cmp(&BinaryOp::Ne, x, &v)),
            }))
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ListAwareUnaryEdgeOp — element-wise is_some / is_none via broadcast_unary
//
// Unlike `UnaryEdgeOp` (which returns `bool` for use in `CreateFilter`), this
// op returns `Option<Prop::Bool>` so it can plug into the expression chain via
// `CreateOp`. The closure intentionally does NOT `?`-propagate the inner
// `None` — the whole purpose of `is_some`/`is_none` is to test that case.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct ListAwareUnaryEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) op: UnaryOp,
}

impl<'g> EdgeOp for ListAwareUnaryEdgeOp<'g> {
    type Output = Option<Prop>;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        let vals = self.inner.apply(storage, edge);
        let op = &self.op;
        broadcast_unary(vals, |v| {
            Some(Prop::Bool(match op {
                UnaryOp::IsSome => v.is_some(),
                UnaryOp::IsNone => v.is_none(),
            }))
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeEndpointNodeOp — applies a node op to the src or dst VID of an edge
//
// Bridges EdgeEndpointWrapper<T: NodeExpr> into the EdgeExpr system:
// EdgeFilter::src().name().eq("Alice") compiles the name NodeOp once, then
// at evaluation time looks up the src VID and applies the node op to it.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct EdgeEndpointNodeOp<'g> {
    pub(crate) node_op: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) endpoint: Endpoint,
}

impl<'g> EdgeOp for EdgeEndpointNodeOp<'g> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        self.node_op.prop_type()
    }

    fn const_value(&self) -> Option<Self::Output> {
        self.node_op.const_value()
    }

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        let vid = match self.endpoint {
            Endpoint::Src => edge.src(),
            Endpoint::Dst => edge.dst(),
        };
        self.node_op.apply(storage, vid)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-edge predicate ops — produce Some(Prop::Bool(...)) per edge.
// Used by `CreateOp::create_edge_op` for the expression-mode path of the
// structural edge predicates (IsActiveEdge, IsValidEdge, IsDeletedEdge,
// IsSelfLoopEdge).
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct IsActiveEdgePropOp<G> {
    pub(crate) graph: G,
}

impl<G: GraphView> EdgeOp for IsActiveEdgePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        Some(Prop::Bool(EdgeView::new(&self.graph, edge).is_active()))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

#[derive(Clone)]
pub(crate) struct IsValidEdgePropOp<G> {
    pub(crate) graph: G,
}

impl<G: GraphView> EdgeOp for IsValidEdgePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        Some(Prop::Bool(EdgeView::new(&self.graph, edge).is_valid()))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

#[derive(Clone)]
pub(crate) struct IsDeletedEdgePropOp<G> {
    pub(crate) graph: G,
}

impl<G: GraphView> EdgeOp for IsDeletedEdgePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        Some(Prop::Bool(EdgeView::new(&self.graph, edge).is_deleted()))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

#[derive(Clone)]
pub(crate) struct IsSelfLoopEdgePropOp<G> {
    pub(crate) graph: G,
}

impl<G: GraphView> EdgeOp for IsSelfLoopEdgePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        Some(Prop::Bool(EdgeView::new(&self.graph, edge).is_self_loop()))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}
