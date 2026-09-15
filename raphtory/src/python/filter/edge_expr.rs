use crate::{
    db::graph::views::filter::model::{
        edge_filter::{CompositeEdgeFilter, EdgeEndpointWrapper, EdgeFilter, Endpoint},
        filter::{NODE_ID_FIELD, NODE_NAME_FIELD, NODE_TYPE_FIELD},
        is_active_edge_filter::IsActiveEdge,
        is_deleted_filter::IsDeletedEdge,
        is_self_loop_filter::IsSelfLoopEdge,
        is_valid_filter::IsValidEdge,
        node_expr::{DynCreateOp, DynEntityExpr, DynTemporal, EntityExpr},
        node_filter::NodeFilter,
        property_filter::PropertyRef,
        windowed_filter::Windowed,
        CombinedFilter, CreateView, DynCreateFilter, DynCreateView, DynPropertyExprFactory,
        EdgeFilterFactory, EdgeViewFilterOps, EntityMarker, FilterTree, InternalViewWrapOps,
        PropertyExprFactory, ViewWrapOps,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            node_expr::{DynNodeFilterFactory, PyExpr, PyPropertyExpr},
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
    pub(crate) Arc<dyn DynNodeFilterFactory>,
    pub(crate) Endpoint,
    pub(crate) Vec<WireView>,
);

/// The node an endpoint read evaluates on, scoped by the same views as the
/// edge chain that reached it: `Edge.window(0, 5).src().property("p")` reads
/// the source node's property inside the window.
fn node_scope(views: &[WireView]) -> Arc<dyn DynNodeFilterFactory> {
    let mut node: Arc<dyn DynNodeFilterFactory> = Arc::new(NodeFilter);
    for view in views {
        node = match view {
            WireView::Window(start, end) => node.window(*start, *end),
            WireView::Latest => Arc::new(node.latest()),
            WireView::SnapshotAt(time) => Arc::new(node.snapshot_at(*time)),
            WireView::SnapshotLatest => Arc::new(node.snapshot_latest()),
            WireView::Layers(names) => Arc::new(node.layer(names.clone())),
        };
    }
    node
}

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
    ///
    /// Returns:
    ///     filter.Expr:
    fn id(&self) -> PyExpr {
        PyExpr::new(
            Arc::new(EdgeEndpointWrapper::new(self.0.dyn_id(), self.1)),
            Some(self.lhs(WireTarget::Field(NODE_ID_FIELD))),
        )
    }

    /// Selects the endpoint node name field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn name(&self) -> PyExpr {
        PyExpr::new(
            Arc::new(EdgeEndpointWrapper::new(self.0.dyn_name(), self.1)),
            Some(self.lhs(WireTarget::Field(NODE_NAME_FIELD))),
        )
    }

    /// Selects the endpoint node type field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn node_type(&self) -> PyExpr {
        PyExpr::new(
            Arc::new(EdgeEndpointWrapper::new(self.0.dyn_node_type(), self.1)),
            Some(self.lhs(WireTarget::Field(NODE_TYPE_FIELD))),
        )
    }

    /// Filters an endpoint node property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    fn property(&self, name: String) -> PyPropertyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Property(name.clone())));
        PyPropertyExpr::new(
            Arc::new(EdgeEndpointWrapper::new(self.0.dyn_property(name), self.1)),
            Some(lhs),
        )
    }

    /// Filters an endpoint node metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    fn metadata(&self, name: String) -> PyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Metadata(name.clone())));
        PyExpr::new(
            Arc::new(EdgeEndpointWrapper::new(self.0.dyn_metadata(name), self.1)),
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

/// An edge filter scoped to a view.
///
/// Obtained from the view methods on [`Edge`] (`Edge.window(...)`,
/// `Edge.layer(...)`, ...); its endpoint, property and structural predicates
/// evaluate within that view, and its own view methods narrow it further.
#[pyclass(frozen, name = "EdgeFilter", module = "raphtory.filter")]
pub struct PyEdgeFilter(Arc<dyn DynEdgeFilterFactory>, Vec<WireView>);

#[pymethods]
impl PyEdgeFilter {
    #[new]
    fn new() -> PyEdgeFilter {
        Self::root()
    }

    /// Selects the edge **source endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint:
    fn src(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint(node_scope(&self.1), Endpoint::Src, self.1.clone())
    }

    /// Selects the edge **destination endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint:
    fn dst(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint(node_scope(&self.1), Endpoint::Dst, self.1.clone())
    }

    /// Filters an edge property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    fn property(&self, name: String) -> PyPropertyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Property(name.clone())));
        PyPropertyExpr::new(self.0.as_ref().dyn_property(name), Some(lhs))
    }

    /// Filters an edge metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    fn metadata(&self, name: String) -> PyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Metadata(name.clone())));
        PyExpr::new(self.0.dyn_metadata(name), Some(lhs))
    }

    /// Restricts edge evaluation to the given time window.
    ///
    /// Arguments:
    ///     start (int): Start time.
    ///     end (int): End time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn window(&self, start: EventTime, end: EventTime) -> PyEdgeFilter {
        self.wrap(self.0.dyn_window(start, end), WireView::Window(start, end))
    }

    /// Restricts edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn at(&self, time: EventTime) -> PyEdgeFilter {
        self.wrap(
            self.0.dyn_at(time),
            WireView::Window(time, EventTime::end(time.t().saturating_add(1))),
        )
    }

    /// Restricts edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
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
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn before(&self, time: EventTime) -> PyEdgeFilter {
        self.wrap(
            self.0.dyn_before(time),
            WireView::Window(EventTime::start(i64::MIN), EventTime::end(time.t())),
        )
    }

    /// Evaluates edge predicates against the latest available edge state.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn latest(&self) -> PyEdgeFilter {
        self.wrap(self.0.dyn_latest(), WireView::Latest)
    }

    /// Evaluates edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn snapshot_at(&self, time: EventTime) -> PyEdgeFilter {
        self.wrap(self.0.dyn_snapshot_at(time), WireView::SnapshotAt(time))
    }

    /// Evaluates edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn snapshot_latest(&self) -> PyEdgeFilter {
        self.wrap(self.0.dyn_snapshot_latest(), WireView::SnapshotLatest)
    }

    /// Restricts evaluation to edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn layer(&self, layer: String) -> PyEdgeFilter {
        self.wrap(
            self.0.dyn_layer(vec![layer.clone()]),
            WireView::Layers(vec![layer]),
        )
    }

    /// Restricts evaluation to edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn layers(&self, layers: FromIterable<String>) -> PyEdgeFilter {
        let layers: Vec<String> = layers.into();
        self.wrap(self.0.dyn_layer(layers.clone()), WireView::Layers(layers))
    }

    /// Matches edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_active(&self) -> PyFilterExpr {
        let tree = FilterTree::Edge(wrap_edge_views(
            CompositeEdgeFilter::IsActiveEdge(IsActiveEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_active(), Some(tree))
    }

    /// Matches edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_valid(&self) -> PyFilterExpr {
        let tree = FilterTree::Edge(wrap_edge_views(
            CompositeEdgeFilter::IsValidEdge(IsValidEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_valid(), Some(tree))
    }

    /// Matches edges that have been deleted.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_deleted(&self) -> PyFilterExpr {
        let tree = FilterTree::Edge(wrap_edge_views(
            CompositeEdgeFilter::IsDeletedEdge(IsDeletedEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_deleted(), Some(tree))
    }

    /// Matches edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_self_loop(&self) -> PyFilterExpr {
        let tree = FilterTree::Edge(wrap_edge_views(
            CompositeEdgeFilter::IsSelfLoopEdge(IsSelfLoopEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_self_loop(), Some(tree))
    }
}

/// Entry point for constructing edge filter expressions.
///
/// Every method is static: `Edge.src().name() == "alice"` selects edges
/// directly, and the view methods return an [`EdgeFilter`] scoped to that
/// view for further chaining.
#[pyclass(frozen, name = "Edge", module = "raphtory.filter")]
pub struct PyEdge;

#[pymethods]
impl PyEdge {
    /// Selects the edge **source endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint:
    #[staticmethod]
    fn src() -> PyEdgeEndpoint {
        PyEdgeFilter::root().src()
    }

    /// Selects the edge **destination endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint:
    #[staticmethod]
    fn dst() -> PyEdgeEndpoint {
        PyEdgeFilter::root().dst()
    }

    /// Filters an edge property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    #[staticmethod]
    fn property(name: String) -> PyPropertyExpr {
        PyEdgeFilter::root().property(name)
    }

    /// Filters an edge metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn metadata(name: String) -> PyExpr {
        PyEdgeFilter::root().metadata(name)
    }

    /// Restricts edge evaluation to the given time window.
    ///
    /// Arguments:
    ///     start (int): Start time.
    ///     end (int): End time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().window(start, end)
    }

    /// Restricts edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn at(time: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().at(time)
    }

    /// Restricts edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn after(time: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().after(time)
    }

    /// Restricts edge evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn before(time: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().before(time)
    }

    /// Evaluates edge predicates against the latest available edge state.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn latest() -> PyEdgeFilter {
        PyEdgeFilter::root().latest()
    }

    /// Evaluates edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().snapshot_at(time)
    }

    /// Evaluates edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn snapshot_latest() -> PyEdgeFilter {
        PyEdgeFilter::root().snapshot_latest()
    }

    /// Restricts evaluation to edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn layer(layer: String) -> PyEdgeFilter {
        PyEdgeFilter::root().layer(layer)
    }

    /// Restricts evaluation to edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyEdgeFilter {
        PyEdgeFilter::root().layers(layers)
    }

    /// Matches edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    #[staticmethod]
    fn is_active() -> PyFilterExpr {
        PyEdgeFilter::root().is_active()
    }

    /// Matches edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    #[staticmethod]
    fn is_valid() -> PyFilterExpr {
        PyEdgeFilter::root().is_valid()
    }

    /// Matches edges that have been deleted.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    #[staticmethod]
    fn is_deleted() -> PyFilterExpr {
        PyEdgeFilter::root().is_deleted()
    }

    /// Matches edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.FilterExpr:
    #[staticmethod]
    fn is_self_loop() -> PyFilterExpr {
        PyEdgeFilter::root().is_self_loop()
    }
}
