use crate::{
    db::{
        api::state::ops::node::{Id, Name, Type},
        graph::views::filter::model::{
            edge_filter::{EdgeEndpointWrapper, EdgeFilter},
            is_active_edge_filter::IsActiveEdge,
            is_deleted_filter::IsDeletedEdge,
            is_self_loop_filter::IsSelfLoopEdge,
            is_valid_filter::IsValidEdge,
            node_expr::{DynCreateOp, EntityExpr, Scoped},
            node_filter::NodeFilter,
            CreateView, EdgeFilterFactory, EdgeViewFilterOps, EntityMarker, InternalViewWrapOps,
            PropertyExprFactory, ViewWrapOps, Wrap,
        },
    },
    prelude::EdgeViewOps,
    python::{filter::node_expr::PyExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;

/// Entry point for filtering an edge endpoint (source or destination).
///
/// An `EdgeEndpoint` is obtained from `Edge.src()` or `Edge.dst()` and allows
/// you to filter on endpoint fields (id, name, type) as well as endpoint
/// properties and metadata.
///
/// Examples:
///     Edge.src().id() == 1
///     Edge.dst().name().starts_with("user:")
///     Edge.src().property("country") == "UK"
#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
pub struct PyEdgeEndpoint(pub EdgeEndpointWrapper<NodeFilter>);

#[pymethods]
impl PyEdgeEndpoint {
    /// Selects the endpoint node ID field for filtering.
    fn id(&self) -> PyExpr {
        self.0.wrap(Id).into()
    }

    /// Selects the endpoint node name field for filtering.
    fn name(&self) -> PyExpr {
        self.0.wrap(Name).into()
    }

    /// Selects the endpoint node type field for filtering.
    fn node_type(&self) -> PyExpr {
        self.0.wrap(Type).into()
    }

    /// Filters an endpoint node property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    fn property(&self, name: String) -> PyExpr {
        self.0.wrap(NodeFilter.property(name)).into()
    }

    /// Filters an endpoint node metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    fn metadata(&self, name: String) -> PyExpr {
        self.0
            .wrap(PropertyExprFactory::metadata(&NodeFilter, name))
            .into()
    }
}

pub trait DynEdgeFilterFactory: Send + Sync + 'static {
    fn dyn_property(&self, name: String) -> Arc<dyn DynCreateOp>;
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp>;

    fn dyn_is_active(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_is_valid(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_is_deleted(&self) -> Arc<dyn DynCreateOp>;
    fn dyn_is_self_loop(&self) -> Arc<dyn DynCreateOp>;

    fn dyn_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_after(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_before(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_latest(&self) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_snapshot_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_snapshot_latest(&self) -> Arc<dyn DynEdgeFilterFactory>;
    fn dyn_layer(&self, layers: Vec<String>) -> Arc<dyn DynEdgeFilterFactory>;
}

impl InternalViewWrapOps for Arc<dyn DynEdgeFilterFactory> {
    type Window = Arc<dyn DynEdgeFilterFactory>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.dyn_window(start, end)
    }
}

impl<T> DynEdgeFilterFactory for T
where
    T: EdgeFilterFactory
        + EdgeViewFilterOps
        + ViewWrapOps
        + CreateView
        + EntityExpr
        + Clone
        + Send
        + Sync
        + 'static,
    <T as EntityExpr>::Marker: Into<EntityMarker>,
{
    fn dyn_property(&self, name: String) -> Arc<dyn DynCreateOp> {
        Arc::new(PropertyExprFactory::property(self, name))
    }
    fn dyn_metadata(&self, name: String) -> Arc<dyn DynCreateOp> {
        Arc::new(PropertyExprFactory::metadata(self, name))
    }

    fn dyn_is_active(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(Scoped {
            view: self.clone(),
            inner: IsActiveEdge,
        })
    }
    fn dyn_is_valid(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(Scoped {
            view: self.clone(),
            inner: IsValidEdge,
        })
    }
    fn dyn_is_deleted(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(Scoped {
            view: self.clone(),
            inner: IsDeletedEdge,
        })
    }
    fn dyn_is_self_loop(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(Scoped {
            view: self.clone(),
            inner: IsSelfLoopEdge,
        })
    }

    // Go dynamic before calling window — the Arc<dyn DynEdgeFilterFactory> impl
    // has Window = Self, which terminates the recursive bound resolution.
    fn dyn_window(&self, start: EventTime, end: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        let dyn_self: Arc<dyn DynEdgeFilterFactory> = Arc::new(self.clone());
        dyn_self.window(start, end)
    }
    fn dyn_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        let dyn_self: Arc<dyn DynEdgeFilterFactory> = Arc::new(self.clone());
        dyn_self.at(time)
    }
    fn dyn_after(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        let dyn_self: Arc<dyn DynEdgeFilterFactory> = Arc::new(self.clone());
        dyn_self.after(time)
    }
    fn dyn_before(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        let dyn_self: Arc<dyn DynEdgeFilterFactory> = Arc::new(self.clone());
        dyn_self.before(time)
    }
    fn dyn_latest(&self) -> Arc<dyn DynEdgeFilterFactory> {
        Arc::new(self.clone().latest())
    }
    fn dyn_snapshot_at(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        Arc::new(self.clone().snapshot_at(time))
    }
    fn dyn_snapshot_latest(&self) -> Arc<dyn DynEdgeFilterFactory> {
        Arc::new(self.clone().snapshot_latest())
    }
    fn dyn_layer(&self, layers: Vec<String>) -> Arc<dyn DynEdgeFilterFactory> {
        Arc::new(self.clone().layer(layers))
    }
}

impl From<Arc<dyn DynEdgeFilterFactory>> for PyEdgeFilter {
    fn from(value: Arc<dyn DynEdgeFilterFactory>) -> Self {
        PyEdgeFilter(value)
    }
}

/// Entry point for constructing edge filter expressions.
///
/// The `Edge` filter provides:
/// - endpoint filters via `src()` and `dst()`,
/// - property and metadata filters,
/// - view restrictions (time windows, snapshots, layers),
/// - and structural predicates over edge state (active/valid/deleted/self-loop).
///
/// Examples:
///     Edge.src().id() == 1
///     Edge.property("weight") > 0.5
///     Edge.window(0, 10).is_active()
///     Edge.layer("fire_nation").is_valid()
#[pyclass(frozen, name = "Edge", module = "raphtory.filter")]
pub struct PyEdgeFilter(Arc<dyn DynEdgeFilterFactory>);

#[pymethods]
impl PyEdgeFilter {
    #[new]
    fn new() -> PyEdgeFilter {
        PyEdgeFilter(Arc::new(EdgeFilter))
    }

    /// Selects the edge **source endpoint** for filtering.
    fn src(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::src())
    }

    /// Selects the edge **destination endpoint** for filtering.
    fn dst(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::dst())
    }

    /// Filters an edge property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    fn property(&self, name: String) -> PyExpr {
        self.0.dyn_property(name).into()
    }

    /// Filters an edge metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    fn metadata(&self, name: String) -> PyExpr {
        self.0.dyn_metadata(name).into()
    }

    /// Restricts edge evaluation to the given time window.
    fn window(&self, start: EventTime, end: EventTime) -> PyEdgeFilter {
        self.0.dyn_window(start, end).into()
    }

    /// Restricts edge evaluation to a single point in time.
    fn at(&self, time: EventTime) -> PyEdgeFilter {
        self.0.dyn_at(time).into()
    }

    /// Restricts edge evaluation to times strictly after the given time.
    fn after(&self, time: EventTime) -> PyEdgeFilter {
        self.0.dyn_after(time).into()
    }

    /// Restricts edge evaluation to times strictly before the given time.
    fn before(&self, time: EventTime) -> PyEdgeFilter {
        self.0.dyn_before(time).into()
    }

    /// Evaluates edge predicates against the latest available edge state.
    fn latest(&self) -> PyEdgeFilter {
        self.0.dyn_latest().into()
    }

    /// Evaluates edge predicates against a snapshot of the graph at a given time.
    fn snapshot_at(&self, time: EventTime) -> PyEdgeFilter {
        self.0.dyn_snapshot_at(time).into()
    }

    /// Evaluates edge predicates against the most recent snapshot of the graph.
    fn snapshot_latest(&self) -> PyEdgeFilter {
        self.0.dyn_snapshot_latest().into()
    }

    /// Restricts evaluation to edges belonging to the given layer.
    fn layer(&self, layer: String) -> PyEdgeFilter {
        self.0.dyn_layer(vec![layer]).into()
    }

    /// Restricts evaluation to edges belonging to any of the given layers.
    fn layers(&self, layers: FromIterable<String>) -> PyEdgeFilter {
        self.0.dyn_layer(layers.to_vec()).into()
    }

    /// Matches edges that have at least one event in the current view.
    fn is_active(&self) -> PyExpr {
        self.0.dyn_is_active().into()
    }

    /// Matches edges that are structurally valid in the current view.
    fn is_valid(&self) -> PyExpr {
        self.0.dyn_is_valid().into()
    }

    /// Matches edges that have been deleted.
    fn is_deleted(&self) -> PyExpr {
        self.0.dyn_is_deleted().into()
    }

    /// Matches edges that are self-loops (source == destination).
    fn is_self_loop(&self) -> PyExpr {
        self.0.dyn_is_self_loop().into()
    }
}
