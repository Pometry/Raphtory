use crate::{
    db::graph::views::filter::model::{
        edge_filter::{EdgeEndpointWrapper, EdgeFilter},
        is_active_edge_filter::IsActiveEdge,
        is_deleted_filter::IsDeletedEdge,
        is_self_loop_filter::IsSelfLoopEdge,
        is_valid_filter::IsValidEdge,
        node_expr::{DynCreateOp, DynEntityExpr, DynTemporal, EntityExpr},
        node_filter::NodeFilter,
        windowed_filter::Windowed,
        CombinedFilter, CreateView, DynCreateFilter, DynCreateView, EdgeFilterFactory,
        EdgeViewFilterOps, EntityMarker, InternalViewWrapOps, PropertyExprFactory, ViewWrapOps,
        Wrap,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            node_expr::{PyExpr, PyPropertyExpr},
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::storage::timeindex::{AsTime, EventTime};
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
        self.0.id().into()
    }

    /// Selects the endpoint node name field for filtering.
    fn name(&self) -> PyExpr {
        self.0.name().into()
    }

    /// Selects the endpoint node type field for filtering.
    fn node_type(&self) -> PyExpr {
        self.0.node_type().into()
    }

    /// Filters an endpoint node property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    fn property(&self, name: String) -> PyPropertyExpr {
        let expr: Arc<dyn DynTemporal> = Arc::new(self.0.property(name));
        expr.into()
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
        self.dyn_window(time, EventTime::from(time.t().saturating_add(1)))
    }
    fn dyn_after(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        let start = time.t().saturating_add(1);
        self.dyn_window(EventTime::start(start), EventTime::end(i64::MAX))
    }
    fn dyn_before(&self, time: EventTime) -> Arc<dyn DynEdgeFilterFactory> {
        self.dyn_window(EventTime::start(i64::MIN), EventTime::end(time.t()))
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

impl PyEdgeFilter {
    pub(crate) fn root() -> Self {
        PyEdgeFilter(Arc::new(EdgeFilter))
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
    fn property(&self, name: String) -> PyPropertyExpr {
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
    fn is_active(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_active())
    }

    /// Matches edges that are structurally valid in the current view.
    fn is_valid(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_valid())
    }

    /// Matches edges that have been deleted.
    fn is_deleted(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_deleted())
    }

    /// Matches edges that are self-loops (source == destination).
    fn is_self_loop(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_self_loop())
    }
}
