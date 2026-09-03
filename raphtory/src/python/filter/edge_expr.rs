use crate::{
    db::graph::views::filter::model::{
        edge_filter::{CompositeEdgeFilter, EdgeEndpointWrapper, EdgeFilter, Endpoint},
        is_active_edge_filter::IsActiveEdge,
        is_deleted_filter::IsDeletedEdge,
        is_self_loop_filter::IsSelfLoopEdge,
        is_valid_filter::IsValidEdge,
        node_expr::{DynCreateOp, DynEntityExpr, DynTemporal, EntityExpr},
        node_filter::NodeFilter,
        property_filter::PropertyRef,
        windowed_filter::Windowed,
        CombinedFilter, CreateView, DynCreateFilter, DynCreateView, EdgeFilterFactory,
        EdgeViewFilterOps, EntityMarker, FilterTree, InternalViewWrapOps, PropertyExprFactory,
        ViewWrapOps, Wrap,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            node_expr::{PyExpr, PyPropertyExpr},
            wire::{wrap_edge_views, WireEntity, WireLhs, WireTarget, WireView},
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
pub struct PyEdgeEndpoint(
    pub EdgeEndpointWrapper<NodeFilter>,
    pub(crate) Endpoint,
    pub(crate) Vec<WireView>,
);

impl PyEdgeEndpoint {
    fn lhs(&self, target: WireTarget) -> WireLhs {
        WireLhs {
            entity: WireEntity::Edge,
            endpoint: Some(self.1),
            target,
            ops: Vec::new(),
            views: self.2.clone(),
        }
    }
}

#[pymethods]
impl PyEdgeEndpoint {
    /// Selects the endpoint node ID field for filtering.
    fn id(&self) -> PyExpr {
        PyExpr::new(
            Arc::new(self.0.id()),
            Some(self.lhs(WireTarget::Field("node_id"))),
        )
    }

    /// Selects the endpoint node name field for filtering.
    fn name(&self) -> PyExpr {
        PyExpr::new(
            Arc::new(self.0.name()),
            Some(self.lhs(WireTarget::Field("node_name"))),
        )
    }

    /// Selects the endpoint node type field for filtering.
    fn node_type(&self) -> PyExpr {
        PyExpr::new(
            Arc::new(self.0.node_type()),
            Some(self.lhs(WireTarget::Field("node_type"))),
        )
    }

    /// Filters an endpoint node property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    fn property(&self, name: String) -> PyPropertyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Property(name.clone())));
        PyPropertyExpr::new(Arc::new(self.0.property(name)), Some(lhs))
    }

    /// Filters an endpoint node metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    fn metadata(&self, name: String) -> PyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Metadata(name.clone())));
        PyExpr::new(
            Arc::new(
                self.0
                    .wrap(PropertyExprFactory::metadata(&NodeFilter, name)),
            ),
            Some(lhs),
        )
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
        PyEdgeFilter(Arc::new(EdgeFilter), Vec::new())
    }

    fn wrap(&self, factory: Arc<dyn DynEdgeFilterFactory>, view: WireView) -> Self {
        let mut views = self.1.clone();
        views.push(view);
        PyEdgeFilter(factory, views)
    }

    fn lhs(&self, target: WireTarget) -> WireLhs {
        WireLhs {
            entity: WireEntity::Edge,
            endpoint: None,
            target,
            ops: Vec::new(),
            views: self.1.clone(),
        }
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
pub struct PyEdgeFilter(Arc<dyn DynEdgeFilterFactory>, Vec<WireView>);

#[pymethods]
impl PyEdgeFilter {
    #[new]
    fn new() -> PyEdgeFilter {
        Self::root()
    }

    /// Selects the edge **source endpoint** for filtering.
    fn src(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::src(), Endpoint::Src, self.1.clone())
    }

    /// Selects the edge **destination endpoint** for filtering.
    fn dst(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::dst(), Endpoint::Dst, self.1.clone())
    }

    /// Filters an edge property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    fn property(&self, name: String) -> PyPropertyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Property(name.clone())));
        PyPropertyExpr::new(self.0.dyn_property(name), Some(lhs))
    }

    /// Filters an edge metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    fn metadata(&self, name: String) -> PyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Metadata(name.clone())));
        PyExpr::new(self.0.dyn_metadata(name), Some(lhs))
    }

    /// Restricts edge evaluation to the given time window.
    fn window(&self, start: EventTime, end: EventTime) -> PyEdgeFilter {
        self.wrap(self.0.dyn_window(start, end), WireView::Window(start, end))
    }

    /// Restricts edge evaluation to a single point in time.
    fn at(&self, time: EventTime) -> PyEdgeFilter {
        self.wrap(
            self.0.dyn_at(time),
            WireView::Window(time, EventTime::end(time.t().saturating_add(1))),
        )
    }

    /// Restricts edge evaluation to times strictly after the given time.
    fn after(&self, time: EventTime) -> PyEdgeFilter {
        self.wrap(
            self.0.dyn_after(time),
            WireView::Window(
                EventTime::start(time.t().saturating_add(1)),
                EventTime::end(i64::MAX),
            ),
        )
    }

    /// Restricts edge evaluation to times strictly before the given time.
    fn before(&self, time: EventTime) -> PyEdgeFilter {
        self.wrap(
            self.0.dyn_before(time),
            WireView::Window(EventTime::start(i64::MIN), EventTime::end(time.t())),
        )
    }

    /// Evaluates edge predicates against the latest available edge state.
    fn latest(&self) -> PyEdgeFilter {
        self.wrap(self.0.dyn_latest(), WireView::Latest)
    }

    /// Evaluates edge predicates against a snapshot of the graph at a given time.
    fn snapshot_at(&self, time: EventTime) -> PyEdgeFilter {
        self.wrap(self.0.dyn_snapshot_at(time), WireView::SnapshotAt(time))
    }

    /// Evaluates edge predicates against the most recent snapshot of the graph.
    fn snapshot_latest(&self) -> PyEdgeFilter {
        self.wrap(self.0.dyn_snapshot_latest(), WireView::SnapshotLatest)
    }

    /// Restricts evaluation to edges belonging to the given layer.
    fn layer(&self, layer: String) -> PyEdgeFilter {
        self.wrap(
            self.0.dyn_layer(vec![layer.clone()]),
            WireView::Layers(vec![layer]),
        )
    }

    /// Restricts evaluation to edges belonging to any of the given layers.
    fn layers(&self, layers: FromIterable<String>) -> PyEdgeFilter {
        let layers = layers.to_vec();
        self.wrap(self.0.dyn_layer(layers.clone()), WireView::Layers(layers))
    }

    /// Matches edges that have at least one event in the current view.
    fn is_active(&self) -> PyFilterExpr {
        let tree = FilterTree::Edge(wrap_edge_views(
            CompositeEdgeFilter::IsActiveEdge(IsActiveEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_active(), Some(tree))
    }

    /// Matches edges that are structurally valid in the current view.
    fn is_valid(&self) -> PyFilterExpr {
        let tree = FilterTree::Edge(wrap_edge_views(
            CompositeEdgeFilter::IsValidEdge(IsValidEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_valid(), Some(tree))
    }

    /// Matches edges that have been deleted.
    fn is_deleted(&self) -> PyFilterExpr {
        let tree = FilterTree::Edge(wrap_edge_views(
            CompositeEdgeFilter::IsDeletedEdge(IsDeletedEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_deleted(), Some(tree))
    }

    /// Matches edges that are self-loops (source == destination).
    fn is_self_loop(&self) -> PyFilterExpr {
        let tree = FilterTree::Edge(wrap_edge_views(
            CompositeEdgeFilter::IsSelfLoopEdge(IsSelfLoopEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_self_loop(), Some(tree))
    }
}
