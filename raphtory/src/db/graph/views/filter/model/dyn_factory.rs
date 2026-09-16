//! Type-erased filter factories.
//!
//! The typed factories (`NodeFilter`, `EdgeFilter`, and their view wrappers)
//! form an open family of generic types. Anything that builds a filter from
//! runtime data — the python bindings, a deserialised filter tree — needs one
//! type to hold whichever factory the data names, so each family is erased
//! behind a trait object here. Views wrap the *erased* factory, which keeps
//! the set of concrete types finite; wrapping the typed factory would ask the
//! compiler for a vtable per wrapper combination.

use crate::db::graph::views::filter::model::{
    after_bounds, at_bounds, before_bounds,
    is_active_edge_filter::IsActiveEdge,
    is_active_node_filter::IsActiveNode,
    is_deleted_filter::IsDeletedEdge,
    is_self_loop_filter::IsSelfLoopEdge,
    is_valid_filter::IsValidEdge,
    node_expr::{DynCreateOp, DynEntityExpr, DynTemporal, EntityExpr},
    windowed_filter::Windowed,
    CombinedFilter, CreateView, DynCreateFilter, DynCreateView, DynPropertyExprFactory,
    EdgeFilterFactory, EdgeViewFilterOps, EntityMarker, InternalViewWrapOps, NodeFilterFactory,
    NodeViewFilterOps, PropertyExprFactory, ViewWrapOps,
};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;

pub trait DynNodeFilterFactory:
    DynPropertyExprFactory + DynEntityExpr + DynCreateView + Send + Sync + 'static
{
    fn dyn_id(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_name(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_node_type(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_degree(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_in_degree(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_out_degree(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter>;
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp>;

    fn dyn_build_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynNodeFilterFactory>;

    fn dyn_bounds(&self) -> (EventTime, EventTime);
}

impl InternalViewWrapOps for Arc<dyn DynNodeFilterFactory> {
    type Window = Arc<dyn DynNodeFilterFactory>;

    // Both calls dispatch through the vtable explicitly: plain method syntax
    // would select the DynNodeFilterFactory blanket on Arc itself and loop.
    fn bounds(&self) -> (EventTime, EventTime) {
        self.as_ref().dyn_bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.as_ref().dyn_build_window(start, end)
    }
}

impl<T> DynNodeFilterFactory for T
where
    T: NodeFilterFactory + NodeViewFilterOps + Send + Sync + 'static,
{
    fn dyn_id(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.id())
    }
    fn dyn_name(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.name())
    }
    fn dyn_node_type(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.node_type())
    }

    fn dyn_degree(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.degree())
    }
    fn dyn_in_degree(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.in_degree())
    }
    fn dyn_out_degree(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.out_degree())
    }

    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_active())
    }

    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp> {
        Arc::new(PropertyExprFactory::metadata(self, name))
    }

    fn dyn_build_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynNodeFilterFactory> {
        Arc::new(self.clone().build_window(start, end))
    }

    fn dyn_bounds(&self) -> (EventTime, EventTime) {
        self.bounds()
    }
}

impl NodeFilterFactory for Arc<dyn DynNodeFilterFactory> {
    type NodeWindow = Self::Window;
}

impl NodeViewFilterOps for Arc<dyn DynNodeFilterFactory> {
    type Output<T: CombinedFilter> = Arc<dyn DynCreateFilter>;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        self.as_ref().dyn_is_active()
    }
}

pub trait DynEdgeFilterFactory: DynEntityExpr + DynCreateView + Send + Sync + 'static {
    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal>;
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp>;

    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter>;
    fn dyn_is_valid(&self) -> Arc<dyn DynCreateFilter>;
    fn dyn_is_deleted(&self) -> Arc<dyn DynCreateFilter>;
    fn dyn_is_self_loop(&self) -> Arc<dyn DynCreateFilter>;

    fn dyn_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_after(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_before(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_latest(&self) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_snapshot_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_snapshot_latest(&self) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_layer(&self, layers: Vec<String>) -> Arc<dyn DynEdgeFilterFactory>;
}

impl EdgeFilterFactory for Arc<dyn DynEdgeFilterFactory> {}

impl EdgeViewFilterOps for Arc<dyn DynEdgeFilterFactory> {
    type Output<T: CombinedFilter> = Arc<dyn DynCreateFilter>;

    fn is_active(&self) -> Self::Output<IsActiveEdge> {
        self.as_ref().dyn_is_active()
    }

    fn is_valid(&self) -> Self::Output<IsValidEdge> {
        self.as_ref().dyn_is_valid()
    }

    fn is_deleted(&self) -> Self::Output<IsDeletedEdge> {
        self.as_ref().dyn_is_deleted()
    }

    fn is_self_loop(&self) -> Self::Output<IsSelfLoopEdge> {
        self.as_ref().dyn_is_self_loop()
    }
}

impl InternalViewWrapOps for Arc<dyn DynEdgeFilterFactory> {
    type Window = Arc<dyn DynEdgeFilterFactory>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.as_ref().dyn_window(start, end)
    }
}

impl<T> DynEdgeFilterFactory for T
where
    T: EdgeFilterFactory + EdgeViewFilterOps + ViewWrapOps + CreateView + EntityExpr + Clone,
    T: Send + Sync + 'static,
    <T as EntityExpr>::Marker: Into<EntityMarker>,
{
    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal> {
        Arc::new(PropertyExprFactory::property(self, name))
    }
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp> {
        Arc::new(PropertyExprFactory::metadata(self, name))
    }

    fn dyn_is_active(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_active())
    }
    fn dyn_is_valid(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_valid())
    }
    fn dyn_is_deleted(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_deleted())
    }
    fn dyn_is_self_loop(&self) -> Arc<dyn DynCreateFilter> {
        Arc::new(self.is_self_loop())
    }

    // The window wrapper is constructed over the erased factory directly:
    // routing through ViewWrapOps::window would dispatch straight back into
    // this method through the erased build_window.
    fn dyn_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        let dyn_self: Arc<dyn DynEdgeFilterFactory> = Arc::new(self.clone());
        let (old_start, old_end) = self.bounds();
        let end = end.min(old_end);
        let start = start.max(old_start).min(end);
        Arc::new(Windowed::new(start, end, dyn_self))
    }
    fn dyn_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        let (start, end) = at_bounds(time);
        self.dyn_window(start, end)
    }
    fn dyn_after(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        let (start, end) = after_bounds(time);
        self.dyn_window(start, end)
    }
    fn dyn_before(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        let (start, end) = before_bounds(time);
        self.dyn_window(start, end)
    }
    // Same erasure trick as dyn_window: wrapping the erased factory keeps the
    // set of vtable-instantiated types finite; wrapping `self` directly would
    // materialise a vtable for every wrapper combination.
    fn dyn_latest(&self) -> Arc<dyn DynEdgeFilterFactory> {
        let dyn_self: Arc<dyn DynEdgeFilterFactory> = Arc::new(self.clone());
        Arc::new(dyn_self.latest())
    }
    fn dyn_snapshot_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        let dyn_self: Arc<dyn DynEdgeFilterFactory> = Arc::new(self.clone());
        Arc::new(dyn_self.snapshot_at(time))
    }
    fn dyn_snapshot_latest(&self) -> Arc<dyn DynEdgeFilterFactory> {
        let dyn_self: Arc<dyn DynEdgeFilterFactory> = Arc::new(self.clone());
        Arc::new(dyn_self.snapshot_latest())
    }
    fn dyn_layer(&self, layers: Vec<String>) -> Arc<dyn DynEdgeFilterFactory> {
        let dyn_self: Arc<dyn DynEdgeFilterFactory> = Arc::new(self.clone());
        Arc::new(dyn_self.layer(layers))
    }
}
