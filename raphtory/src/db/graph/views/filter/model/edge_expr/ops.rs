//! Runtime edge evaluators — given an EdgeRef, return a typed value.
//!
//! Parallel to `node_expr/ops.rs` — same design, different subject.

use crate::db::{
    api::{
        properties::internal::{InternalMetadataOps, InternalTemporalPropertyViewOps},
        state::ops::Const,
        view::internal::GraphView,
    },
    graph::edge::EdgeView,
};
use raphtory_api::core::entities::{
    edges::edge_ref::EdgeRef,
    properties::prop::{Prop, PropType},
};
use raphtory_storage::graph::graph::GraphStorage;
use std::{collections::HashSet, hash::Hash};

use super::EdgeOp;
use crate::db::{
    api::state::ops::NodeOp,
    graph::views::filter::model::{
        edge_filter::Endpoint,
        node_expr::ops::{broadcast_binary, broadcast_unary},
    },
};
use raphtory_api::core::entities::properties::prop::PropArray;
use std::sync::Arc;
// ─────────────────────────────────────────────────────────────────────────────
// Arc<dyn EdgeOp> — blanket impl so Arc-boxed ops satisfy EdgeOp
// ─────────────────────────────────────────────────────────────────────────────

impl<'a, V: Clone + Send + Sync> EdgeOp for Arc<dyn EdgeOp<Output = V> + 'a> {
    type Output = V;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> V {
        self.as_ref().apply(storage, edge)
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

    fn prop_type(&self) -> PropType {
        self.graph
            .edge_meta()
            .metadata_mapper()
            .get_dtype(self.prop_id)
            .unwrap_or_default()
    }
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

use crate::db::graph::views::filter::model::filter_operator::{BinaryOp, Comparable};

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

use crate::db::graph::views::filter::model::{
    filter_operator::UnaryOp, SetOp, StringComparable, StringOp,
};

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
// SetEdgeOp<'g, I> — is_in / is_not_in for Option<I> (HashSet, O(1))
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct SetEdgeOp<'g, I: Eq + Hash + Clone + Send + Sync + 'static> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<I>> + 'g>,
    pub(crate) values: Arc<HashSet<I>>,
    pub(crate) op: SetOp,
}

impl<'g, I: Eq + Hash + Clone + Send + Sync + 'static> EdgeOp for SetEdgeOp<'g, I> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        match self.inner.apply(storage, edge) {
            None => false,
            Some(v) => match self.op {
                SetOp::IsIn => self.values.contains(&v),
                SetOp::IsNotIn => !self.values.contains(&v),
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
// AndBoolEdgeOp / OrBoolEdgeOp — boolean AND/OR over two Option<Prop> edge ops
//
// Used by AndFilter<L, R> / OrFilter<L, R> when they implement EdgeExpr so that
// .not() (and other EntityExprFilterOps) can be chained on composed edge filters.
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct AndBoolEdgeOp<'g> {
    pub(crate) left: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) right: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
}

impl<'g> Clone for AndBoolEdgeOp<'g> {
    fn clone(&self) -> Self {
        Self {
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<'g> EdgeOp for AndBoolEdgeOp<'g> {
    type Output = Option<Prop>;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        let l = self.left.apply(storage, edge);
        let r = self.right.apply(storage, edge);
        broadcast_binary(l, r, &|lv, rv| {
            let lb = matches!(lv, Some(Prop::Bool(true)));
            let rb = matches!(rv, Some(Prop::Bool(true)));
            Some(Prop::Bool(lb && rb))
        })
    }
}

pub(crate) struct OrBoolEdgeOp<'g> {
    pub(crate) left: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) right: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
}

impl<'g> Clone for OrBoolEdgeOp<'g> {
    fn clone(&self) -> Self {
        Self {
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<'g> EdgeOp for OrBoolEdgeOp<'g> {
    type Output = Option<Prop>;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        let l = self.left.apply(storage, edge);
        let r = self.right.apply(storage, edge);
        broadcast_binary(l, r, &|lv, rv| {
            let lb = matches!(lv, Some(Prop::Bool(true)));
            let rb = matches!(rv, Some(Prop::Bool(true)));
            Some(Prop::Bool(lb || rb))
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

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        let vid = match self.endpoint {
            Endpoint::Src => edge.src(),
            Endpoint::Dst => edge.dst(),
        };
        self.node_op.apply(storage, vid)
    }
}
