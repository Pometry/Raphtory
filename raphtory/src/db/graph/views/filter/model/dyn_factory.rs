//! Type-erased filter factories.
//!
//! The typed factories (`NodeFilter`, `EdgeFilter`, and their view wrappers)
//! form an open family of generic types. Anything that builds a filter from
//! runtime data — the python bindings, a deserialised filter tree — needs one
//! type to hold whichever factory the data names, so each family is erased
//! behind a trait object here. The typed factories carry the [`Static`] marker
//! and get the erased trait through a blanket impl; the trait object itself
//! does not, and forwards each call through its vtable instead.
//!
//! A view wraps the *erased* factory, which keeps the set of concrete types
//! finite: wrapping the typed one would ask the compiler for a vtable per
//! wrapper combination. Windows are the exception, because a window over a
//! window merges into one, so the typed wrapper never nests.

use crate::db::{
    api::view::internal::Static,
    graph::views::filter::model::{
        after_bounds, at_bounds, before_bounds,
        node_expr::{DynCreateOp, DynEntityExpr, DynTemporal, EntityExpr},
        CreateView, DynCreateView, DynPropertyExprFactory, EdgeFilterFactory, EntityMarker,
        InternalViewWrapOps, NodeFilterFactory, PropertyExprFactory, ViewWrapOps,
    },
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
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp>;

    fn dyn_build_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynNodeFilterFactory>;

    fn dyn_bounds(&self) -> (EventTime, EventTime);
}

impl InternalViewWrapOps for Arc<dyn DynNodeFilterFactory> {
    type Window = Arc<dyn DynNodeFilterFactory>;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.dyn_bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.dyn_build_window(start, end)
    }
}

impl DynNodeFilterFactory for Arc<dyn DynNodeFilterFactory> {
    fn dyn_id(&self) -> Arc<dyn DynCreateOp> {
        self.as_ref().dyn_id()
    }
    fn dyn_name(&self) -> Arc<dyn DynCreateOp> {
        self.as_ref().dyn_name()
    }
    fn dyn_node_type(&self) -> Arc<dyn DynCreateOp> {
        self.as_ref().dyn_node_type()
    }
    fn dyn_degree(&self) -> Arc<dyn DynCreateOp> {
        self.as_ref().dyn_degree()
    }
    fn dyn_in_degree(&self) -> Arc<dyn DynCreateOp> {
        self.as_ref().dyn_in_degree()
    }
    fn dyn_out_degree(&self) -> Arc<dyn DynCreateOp> {
        self.as_ref().dyn_out_degree()
    }
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp> {
        self.as_ref().dyn_metadata(name)
    }

    fn dyn_build_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynNodeFilterFactory> {
        self.as_ref().dyn_build_window(start, end)
    }

    fn dyn_bounds(&self) -> (EventTime, EventTime) {
        self.as_ref().dyn_bounds()
    }
}

impl<T> DynNodeFilterFactory for T
where
    T: NodeFilterFactory + Static + Send + Sync + 'static,
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

pub trait DynEdgeFilterFactory: DynEntityExpr + DynCreateView + Send + Sync + 'static {
    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal>;
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp>;

    fn dyn_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_after(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_before(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_latest(&self) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_snapshot_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_snapshot_latest(&self) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_layer(&self, layers: Vec<String>) -> Arc<dyn DynEdgeFilterFactory>;
}

impl EdgeFilterFactory for Arc<dyn DynEdgeFilterFactory> {
    type EdgeWindow = Self;
}

impl InternalViewWrapOps for Arc<dyn DynEdgeFilterFactory> {
    type Window = Arc<dyn DynEdgeFilterFactory>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.dyn_window(start, end)
    }
}

impl DynEdgeFilterFactory for Arc<dyn DynEdgeFilterFactory> {
    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal> {
        self.as_ref().dyn_property(name)
    }
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp> {
        self.as_ref().dyn_metadata(name)
    }

    fn dyn_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        self.as_ref().dyn_window(start, end)
    }
    fn dyn_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        self.as_ref().dyn_at(time)
    }
    fn dyn_after(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        self.as_ref().dyn_after(time)
    }
    fn dyn_before(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        self.as_ref().dyn_before(time)
    }
    fn dyn_latest(&self) -> Arc<dyn DynEdgeFilterFactory> {
        self.as_ref().dyn_latest()
    }
    fn dyn_snapshot_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        self.as_ref().dyn_snapshot_at(time)
    }
    fn dyn_snapshot_latest(&self) -> Arc<dyn DynEdgeFilterFactory> {
        self.as_ref().dyn_snapshot_latest()
    }
    fn dyn_layer(&self, layers: Vec<String>) -> Arc<dyn DynEdgeFilterFactory> {
        self.as_ref().dyn_layer(layers)
    }
}

impl<T> DynEdgeFilterFactory for T
where
    T: EdgeFilterFactory + Static + ViewWrapOps + CreateView + EntityExpr + Clone,
    T: Send + Sync + 'static,
    <T as EntityExpr>::Marker: Into<EntityMarker>,
{
    fn dyn_property(&self, name: String) -> Arc<dyn DynTemporal> {
        Arc::new(PropertyExprFactory::property(self, name))
    }
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp> {
        Arc::new(PropertyExprFactory::metadata(self, name))
    }

    fn dyn_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        Arc::new(self.clone().window(start, end))
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
    // These wrap the erased factory (see the module docs): `latest` over
    // `latest` does not merge the way a window does, so wrapping `self`
    // directly would materialise a vtable for every wrapper combination.
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
