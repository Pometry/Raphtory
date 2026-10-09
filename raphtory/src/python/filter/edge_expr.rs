use crate::{
    db::graph::views::filter::model::{
        edge_filter::{EdgeFilter, Endpoint},
        expr::{Chain, EdgeEndpoint, EdgeLeaf},
        EdgeViewFilterOps, PropertyExprFactory, ViewWrapOps,
    },
    python::{
        filter::{
            node_expr::{PyExpr, PyPropertyExpr},
            repr,
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods, PyResult, Python};
use raphtory_api::core::{entities::GID, storage::timeindex::EventTime};

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
pub struct PyEdgeEndpoint(EdgeEndpoint);

#[pymethods]
impl PyEdgeEndpoint {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let end = match self.0.endpoint() {
            Endpoint::Src => "src",
            Endpoint::Dst => "dst",
        };
        Ok(format!(
            "{}.{end}()",
            repr::factory(py, "Edge", self.0.views())?
        ))
    }

    /// Selects the endpoint node ID field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn id(&self) -> PyExpr {
        PyExpr(self.0.id().into())
    }

    /// Selects the endpoint node name field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn name(&self) -> PyExpr {
        PyExpr(self.0.name().into())
    }

    /// Selects the endpoint node type field for filtering.
    ///
    /// Returns:
    ///     filter.Expr:
    fn node_type(&self) -> PyExpr {
        PyExpr(self.0.node_type().into())
    }

    /// Filters an endpoint node property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    fn property(&self, name: String) -> PyPropertyExpr {
        self.0.property(name).into()
    }

    /// Filters an endpoint node metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    fn metadata(&self, name: String) -> PyExpr {
        PyExpr(self.0.metadata(name).into())
    }
}

impl PyEdgeFilter {
    pub(crate) fn root() -> Self {
        PyEdgeFilter(EdgeFilter.into())
    }
}

/// An edge filter scoped to a view.
///
/// Obtained from the view methods on `Edge` (`Edge.window(...)`,
/// `Edge.layer(...)`, ...); its endpoint, property and structural predicates
/// evaluate within that view, and its own view methods narrow it further.
#[pyclass(frozen, name = "EdgeFilter", module = "raphtory.filter")]
pub struct PyEdgeFilter(pub(crate) Chain<EdgeLeaf>);

#[pymethods]
impl PyEdgeFilter {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        repr::factory(py, "Edge", self.0.views())
    }

    /// Selects the edge **source endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint:
    fn src(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint(self.0.src())
    }

    /// Selects the edge **destination endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint:
    fn dst(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint(self.0.dst())
    }

    /// Filters an edge property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    fn property(&self, name: String) -> PyPropertyExpr {
        self.0.property(name).into()
    }

    /// Filters an edge metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    fn metadata(&self, name: String) -> PyExpr {
        PyExpr(self.0.metadata(name).into())
    }

    /// Restricts edge evaluation to the given time window.
    ///
    /// Arguments:
    ///     start (TimeInput): Start time.
    ///     end (TimeInput): End time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn window(&self, start: EventTime, end: EventTime) -> PyEdgeFilter {
        Self(self.0.clone().window(start, end))
    }

    /// Restricts edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (TimeInput): Event time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn at(&self, time: EventTime) -> PyEdgeFilter {
        Self(self.0.clone().at(time))
    }

    /// Restricts edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Lower time bound.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn after(&self, time: EventTime) -> PyEdgeFilter {
        Self(self.0.clone().after(time))
    }

    /// Restricts edge evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Upper time bound.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn before(&self, time: EventTime) -> PyEdgeFilter {
        Self(self.0.clone().before(time))
    }

    /// Evaluates edge predicates against the latest available edge state.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn latest(&self) -> PyEdgeFilter {
        Self(self.0.clone().latest())
    }

    /// Evaluates edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Snapshot time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn snapshot_at(&self, time: EventTime) -> PyEdgeFilter {
        Self(self.0.clone().snapshot_at(time))
    }

    /// Evaluates edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn snapshot_latest(&self) -> PyEdgeFilter {
        Self(self.0.clone().snapshot_latest())
    }

    /// Restricts evaluation to edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn layer(&self, layer: String) -> PyEdgeFilter {
        Self(self.0.clone().layer(layer))
    }

    /// Restricts evaluation to edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn layers(&self, layers: FromIterable<String>) -> PyEdgeFilter {
        Self(self.0.clone().layer(Vec::<String>::from(layers)))
    }

    /// Reads through a view of the default layer only.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn default_layer(&self) -> PyEdgeFilter {
        Self(self.0.clone().default_layer())
    }

    /// Reads through a view of every layer except the given one.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn exclude_layer(&self, layer: String) -> PyEdgeFilter {
        Self(self.0.clone().exclude_layer(layer))
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn exclude_layers(&self, layers: FromIterable<String>) -> PyEdgeFilter {
        Self(self.0.clone().exclude_layers(layers))
    }

    /// Reads through a view of the given layers.
    ///
    /// A layer name the graph does not have is ignored, where `layers` raises.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn valid_layers(&self, layers: FromIterable<String>) -> PyEdgeFilter {
        Self(self.0.clone().valid_layers(layers))
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// A layer name the graph does not have is ignored, where `exclude_layers` raises.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn exclude_valid_layers(&self, layers: FromIterable<String>) -> PyEdgeFilter {
        Self(self.0.clone().exclude_valid_layers(layers))
    }

    /// Moves the start of the current window to `start` when that is later.
    ///
    /// The window only ever narrows: a start before the current one changes nothing.
    ///
    /// Arguments:
    ///     start (TimeInput): New start time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn shrink_start(&self, start: EventTime) -> PyEdgeFilter {
        Self(self.0.clone().shrink_start(start))
    }

    /// Moves the end of the current window to `end` when that is earlier.
    ///
    /// The window only ever narrows: an end after the current one changes nothing.
    ///
    /// Arguments:
    ///     end (TimeInput): New end time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn shrink_end(&self, end: EventTime) -> PyEdgeFilter {
        Self(self.0.clone().shrink_end(end))
    }

    /// Reads through a view of every node except the given ones, with their edges.
    ///
    /// An id the view does not hold changes nothing.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn exclude_nodes(&self, nodes: FromIterable<GID>) -> PyEdgeFilter {
        Self(self.0.clone().exclude_nodes(nodes))
    }

    /// Reads through a view of the given nodes and the edges between them.
    ///
    /// An id the view does not hold is skipped.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn subgraph(&self, nodes: FromIterable<GID>) -> PyEdgeFilter {
        Self(self.0.clone().subgraph(nodes))
    }

    /// Reads through a view of the nodes of the given types and the edges between them.
    ///
    /// Arguments:
    ///     node_types (list[str]): Node types.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn subgraph_node_types(&self, node_types: FromIterable<String>) -> PyEdgeFilter {
        Self(self.0.clone().subgraph_node_types(node_types))
    }

    /// Reads through a view of the edges that are valid in the current view.
    ///
    /// On a persistent graph an edge is valid when its last update is an addition;
    /// on an event graph when it has at least one addition. Nodes are untouched.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    fn valid(&self) -> PyEdgeFilter {
        Self(self.0.clone().valid())
    }

    /// Matches edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_active(&self) -> PyExpr {
        PyExpr(self.0.is_active().into())
    }

    /// Matches edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_valid(&self) -> PyExpr {
        PyExpr(self.0.is_valid().into())
    }

    /// Matches edges that have been deleted.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_deleted(&self) -> PyExpr {
        PyExpr(self.0.is_deleted().into())
    }

    /// Matches edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_self_loop(&self) -> PyExpr {
        PyExpr(self.0.is_self_loop().into())
    }
}

/// Entry point for constructing edge filter expressions.
///
/// Every method is static: `Edge.src().name() == "alice"` selects edges
/// directly, and the view methods return an `EdgeFilter` scoped to that
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
    ///     start (TimeInput): Start time.
    ///     end (TimeInput): End time.
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
    ///     time (TimeInput): Event time.
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
    ///     time (TimeInput): Lower time bound.
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
    ///     time (TimeInput): Upper time bound.
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
    ///     time (TimeInput): Snapshot time.
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

    /// Reads through a view of the default layer only.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn default_layer() -> PyEdgeFilter {
        PyEdgeFilter::root().default_layer()
    }

    /// Reads through a view of every layer except the given one.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn exclude_layer(layer: String) -> PyEdgeFilter {
        PyEdgeFilter::root().exclude_layer(layer)
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn exclude_layers(layers: FromIterable<String>) -> PyEdgeFilter {
        PyEdgeFilter::root().exclude_layers(layers)
    }

    /// Reads through a view of the given layers.
    ///
    /// A layer name the graph does not have is ignored, where `layers` raises.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn valid_layers(layers: FromIterable<String>) -> PyEdgeFilter {
        PyEdgeFilter::root().valid_layers(layers)
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// A layer name the graph does not have is ignored, where `exclude_layers` raises.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn exclude_valid_layers(layers: FromIterable<String>) -> PyEdgeFilter {
        PyEdgeFilter::root().exclude_valid_layers(layers)
    }

    /// Moves the start of the current window to `start` when that is later.
    ///
    /// The window only ever narrows: a start before the current one changes nothing.
    ///
    /// Arguments:
    ///     start (TimeInput): New start time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn shrink_start(start: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().shrink_start(start)
    }

    /// Moves the end of the current window to `end` when that is earlier.
    ///
    /// The window only ever narrows: an end after the current one changes nothing.
    ///
    /// Arguments:
    ///     end (TimeInput): New end time.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn shrink_end(end: EventTime) -> PyEdgeFilter {
        PyEdgeFilter::root().shrink_end(end)
    }

    /// Reads through a view of every node except the given ones, with their edges.
    ///
    /// An id the view does not hold changes nothing.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn exclude_nodes(nodes: FromIterable<GID>) -> PyEdgeFilter {
        PyEdgeFilter::root().exclude_nodes(nodes)
    }

    /// Reads through a view of the given nodes and the edges between them.
    ///
    /// An id the view does not hold is skipped.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn subgraph(nodes: FromIterable<GID>) -> PyEdgeFilter {
        PyEdgeFilter::root().subgraph(nodes)
    }

    /// Reads through a view of the nodes of the given types and the edges between them.
    ///
    /// Arguments:
    ///     node_types (list[str]): Node types.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn subgraph_node_types(node_types: FromIterable<String>) -> PyEdgeFilter {
        PyEdgeFilter::root().subgraph_node_types(node_types)
    }

    /// Reads through a view of the edges that are valid in the current view.
    ///
    /// On a persistent graph an edge is valid when its last update is an addition;
    /// on an event graph when it has at least one addition. Nodes are untouched.
    ///
    /// Returns:
    ///     filter.EdgeFilter:
    #[staticmethod]
    fn valid() -> PyEdgeFilter {
        PyEdgeFilter::root().valid()
    }

    /// Matches edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_active() -> PyExpr {
        PyEdgeFilter::root().is_active()
    }

    /// Matches edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_valid() -> PyExpr {
        PyEdgeFilter::root().is_valid()
    }

    /// Matches edges that have been deleted.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_deleted() -> PyExpr {
        PyEdgeFilter::root().is_deleted()
    }

    /// Matches edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_self_loop() -> PyExpr {
        PyEdgeFilter::root().is_self_loop()
    }
}
