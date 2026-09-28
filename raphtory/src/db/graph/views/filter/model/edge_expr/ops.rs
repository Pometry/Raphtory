//! Runtime edge evaluators — given an edge's storage entry, return a typed value.
//!
//! Parallel to `node_expr/ops.rs` — same design, different subject.

use crate::db::{
    api::{
        state::ops::Const,
        view::internal::{FilterOps, GraphView},
    },
    graph::edge_reads::{self, EdgeAt},
};
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropType},
        LayerId, ELID,
    },
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::{edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage};
use storage::EdgeEntryRef;

use super::EdgeOp;
use crate::db::{api::state::ops::NodeOp, graph::views::filter::model::edge_filter::Endpoint};
use raphtory_api::core::entities::properties::prop::PropArray;
use std::sync::Arc;

// ─────────────────────────────────────────────────────────────────────────────
// Arc<dyn EdgeOp> — blanket impl so Arc-boxed ops satisfy EdgeOp
// ─────────────────────────────────────────────────────────────────────────────

impl<'a, V: Clone + Send + Sync> EdgeOp for Arc<dyn EdgeOp<Output = V> + 'a> {
    type Output = V;

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> V {
        self.as_ref().apply(storage, edge)
    }

    fn apply_layer(&self, storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> V {
        self.as_ref().apply_layer(storage, edge, layer)
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> V {
        self.as_ref().apply_exploded(storage, edge, layer, t)
    }

    fn prop_type(&self) -> PropType {
        self.as_ref().prop_type()
    }

    fn const_value(&self) -> Option<V> {
        self.as_ref().const_value()
    }

    fn filters_exploded(&self) -> bool {
        self.as_ref().filters_exploded()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Const<V> — constant literal op (RHS in comparisons)
// ─────────────────────────────────────────────────────────────────────────────

impl<V: Clone + Send + Sync + 'static> EdgeOp for Const<V> {
    type Output = V;

    fn apply(&self, _storage: &GraphStorage, _edge: EdgeEntryRef) -> V {
        self.0.clone()
    }

    fn apply_layer(&self, _storage: &GraphStorage, _edge: EdgeEntryRef, _layer: LayerId) -> V {
        self.0.clone()
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        _edge: EdgeEntryRef,
        _layer: LayerId,
        _t: EventTime,
    ) -> V {
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

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        edge_reads::temporal_value(&self.graph, edge, EdgeAt::Whole, self.prop_id)
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        edge_reads::temporal_value(&self.graph, edge, EdgeAt::Layer(layer), self.prop_id)
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        edge_reads::temporal_value(&self.graph, edge, EdgeAt::Exploded(layer, t), self.prop_id)
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

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        edge_reads::metadata(&self.graph, edge, EdgeAt::Whole, self.prop_id)
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        edge_reads::metadata(&self.graph, edge, EdgeAt::Layer(layer), self.prop_id)
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        edge_reads::metadata(&self.graph, edge, EdgeAt::Exploded(layer, t), self.prop_id)
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

impl<G: GraphView> TemporalEdgePropOp<G> {
    fn history(&self, edge: EdgeEntryRef, at: EdgeAt) -> Option<Prop> {
        let vals: Vec<Prop> = edge_reads::temporal_hist(&self.graph, edge, at, self.prop_id)
            .map(|(_, v)| v)
            .collect();
        Some(Prop::List(PropArray::from(vals)))
    }
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

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        self.history(edge, EdgeAt::Whole)
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        self.history(edge, EdgeAt::Layer(layer))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        self.history(edge, EdgeAt::Exploded(layer, t))
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

impl<'g> EdgeEndpointNodeOp<'g> {
    fn at_endpoint(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        let vid = match self.endpoint {
            Endpoint::Src => edge.src(),
            Endpoint::Dst => edge.dst(),
        };
        self.node_op.apply(storage, vid)
    }
}

impl<'g> EdgeOp for EdgeEndpointNodeOp<'g> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        self.node_op.prop_type()
    }

    fn const_value(&self) -> Option<Self::Output> {
        self.node_op.const_value()
    }

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        self.at_endpoint(storage, edge)
    }

    fn apply_layer(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        _layer: LayerId,
    ) -> Option<Prop> {
        self.at_endpoint(storage, edge)
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        _layer: LayerId,
        _t: EventTime,
    ) -> Option<Prop> {
        self.at_endpoint(storage, edge)
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

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        Some(Prop::Bool(edge_reads::is_active(
            &self.graph,
            edge,
            EdgeAt::Whole,
        )))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        Some(Prop::Bool(edge_reads::is_active(
            &self.graph,
            edge,
            EdgeAt::Layer(layer),
        )))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        Some(Prop::Bool(edge_reads::is_active(
            &self.graph,
            edge,
            EdgeAt::Exploded(layer, t),
        )))
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

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        Some(Prop::Bool(edge_reads::is_valid(
            &self.graph,
            edge,
            EdgeAt::Whole,
        )))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        Some(Prop::Bool(edge_reads::is_valid(
            &self.graph,
            edge,
            EdgeAt::Layer(layer),
        )))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        Some(Prop::Bool(edge_reads::is_valid(
            &self.graph,
            edge,
            EdgeAt::Exploded(layer, t),
        )))
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

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        Some(Prop::Bool(edge_reads::is_deleted(
            &self.graph,
            edge,
            EdgeAt::Whole,
        )))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        Some(Prop::Bool(edge_reads::is_deleted(
            &self.graph,
            edge,
            EdgeAt::Layer(layer),
        )))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        Some(Prop::Bool(edge_reads::is_deleted(
            &self.graph,
            edge,
            EdgeAt::Exploded(layer, t),
        )))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

#[derive(Clone)]
pub(crate) struct IsSelfLoopEdgePropOp;

impl EdgeOp for IsSelfLoopEdgePropOp {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        Some(Prop::Bool(edge.src() == edge.dst()))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        _layer: LayerId,
    ) -> Option<Prop> {
        Some(Prop::Bool(edge.src() == edge.dst()))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        _layer: LayerId,
        _t: EventTime,
    ) -> Option<Prop> {
        Some(Prop::Bool(edge.src() == edge.dst()))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge predicates: existence in a view and the boolean combinators
// ─────────────────────────────────────────────────────────────────────────────

/// Whether the edge exists in `graph`; the edge side of `NodeExistsOp`.
#[derive(Debug, Clone)]
pub struct EdgeExistsOp<G> {
    graph: G,
}

impl<G> EdgeExistsOp<G> {
    pub(crate) fn new(graph: G) -> Self {
        Self { graph }
    }
}

impl<G: GraphView> EdgeOp for EdgeExistsOp<G> {
    type Output = bool;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> bool {
        self.graph.filter_edge(edge)
    }

    fn apply_layer(&self, _storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.graph.filter_edge_layer(edge, layer)
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> bool {
        self.graph
            .filter_exploded_edge(ELID::new(edge.eid(), layer), t)
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn filters_exploded(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct AndEdgeOp<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: EdgeOp<Output = bool>, R: EdgeOp<Output = bool>> EdgeOp for AndEdgeOp<L, R> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> bool {
        self.left.apply(storage, edge) && self.right.apply(storage, edge)
    }

    fn apply_layer(&self, storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.left.apply_layer(storage, edge, layer) && self.right.apply_layer(storage, edge, layer)
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> bool {
        self.left.apply_exploded(storage, edge, layer, t)
            && self.right.apply_exploded(storage, edge, layer, t)
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn const_value(&self) -> Option<bool> {
        match (self.left.const_value(), self.right.const_value()) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        }
    }

    fn filters_exploded(&self) -> bool {
        self.left.filters_exploded() || self.right.filters_exploded()
    }
}

#[derive(Debug, Clone)]
pub struct OrEdgeOp<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: EdgeOp<Output = bool>, R: EdgeOp<Output = bool>> EdgeOp for OrEdgeOp<L, R> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> bool {
        self.left.apply(storage, edge) || self.right.apply(storage, edge)
    }

    fn apply_layer(&self, storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.left.apply_layer(storage, edge, layer) || self.right.apply_layer(storage, edge, layer)
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> bool {
        self.left.apply_exploded(storage, edge, layer, t)
            || self.right.apply_exploded(storage, edge, layer, t)
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn const_value(&self) -> Option<bool> {
        match (self.left.const_value(), self.right.const_value()) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        }
    }

    fn filters_exploded(&self) -> bool {
        self.left.filters_exploded() || self.right.filters_exploded()
    }
}

#[derive(Debug, Clone)]
pub struct NotEdgeOp<T>(pub(crate) T);

impl<T: EdgeOp<Output = bool>> EdgeOp for NotEdgeOp<T> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> bool {
        !self.0.apply(storage, edge)
    }

    fn apply_layer(&self, storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> bool {
        !self.0.apply_layer(storage, edge, layer)
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> bool {
        !self.0.apply_exploded(storage, edge, layer, t)
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn const_value(&self) -> Option<bool> {
        self.0.const_value().map(|v| !v)
    }

    fn filters_exploded(&self) -> bool {
        self.0.filters_exploded()
    }
}
