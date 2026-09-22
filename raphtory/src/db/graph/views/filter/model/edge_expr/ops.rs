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
use crate::db::{api::state::ops::NodeOp, graph::views::filter::model::edge_filter::Endpoint};
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
