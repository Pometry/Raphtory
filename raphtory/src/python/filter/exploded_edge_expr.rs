use crate::{
    db::graph::views::filter::model::{
        exploded_edge_filter::ExplodedEdgeFilter,
        expr::{Chain, ExplodedEdgeLeaf},
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

/// An exploded-edge filter scoped to a view.
///
/// An exploded edge is one temporal event of an edge, addressed individually
/// rather than as the edge aggregated across time. Obtained from the view
/// methods on `ExplodedEdge`; its property and structural predicates evaluate
/// within that view, and its own view methods narrow it further.
#[pyclass(frozen, name = "ExplodedEdgeFilter", module = "raphtory.filter")]
pub struct PyExplodedEdgeFilter(pub(crate) Chain<ExplodedEdgeLeaf>);

impl PyExplodedEdgeFilter {
    pub(crate) fn root() -> Self {
        PyExplodedEdgeFilter(ExplodedEdgeFilter.into())
    }
}

#[pymethods]
impl PyExplodedEdgeFilter {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        repr::factory(py, "ExplodedEdge", self.0.views())
    }

    /// Filters an exploded edge property by name.
    ///
    /// Reads the property's latest value; `temporal()` switches to its history.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    fn property(&self, name: String) -> PyPropertyExpr {
        self.0.property(name).into()
    }

    /// Filters an exploded edge metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of an exploded edge.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    fn metadata(&self, name: String) -> PyExpr {
        PyExpr(self.0.metadata(name).into())
    }

    /// Restricts exploded edge evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (TimeInput): Start time.
    ///     end (TimeInput): End time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn window(&self, start: EventTime, end: EventTime) -> PyExplodedEdgeFilter {
        Self(self.0.clone().window(start, end))
    }

    /// Restricts exploded edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (TimeInput): Event time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn at(&self, time: EventTime) -> PyExplodedEdgeFilter {
        Self(self.0.clone().at(time))
    }

    /// Restricts exploded edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Lower time bound.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn after(&self, time: EventTime) -> PyExplodedEdgeFilter {
        Self(self.0.clone().after(time))
    }

    /// Restricts exploded edge evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Upper time bound.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn before(&self, time: EventTime) -> PyExplodedEdgeFilter {
        Self(self.0.clone().before(time))
    }

    /// Evaluates exploded edge predicates against the latest available state.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn latest(&self) -> PyExplodedEdgeFilter {
        Self(self.0.clone().latest())
    }

    /// Evaluates exploded edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Snapshot time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn snapshot_at(&self, time: EventTime) -> PyExplodedEdgeFilter {
        Self(self.0.clone().snapshot_at(time))
    }

    /// Evaluates exploded edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn snapshot_latest(&self) -> PyExplodedEdgeFilter {
        Self(self.0.clone().snapshot_latest())
    }

    /// Restricts evaluation to exploded edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn layer(&self, layer: String) -> PyExplodedEdgeFilter {
        Self(self.0.clone().layer(layer))
    }

    /// Restricts evaluation to exploded edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn layers(&self, layers: FromIterable<String>) -> PyExplodedEdgeFilter {
        Self(self.0.clone().layer(Vec::<String>::from(layers)))
    }

    /// Reads through a view of the default layer only.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn default_layer(&self) -> PyExplodedEdgeFilter {
        Self(self.0.clone().default_layer())
    }

    /// Reads through a view of every layer except the given one.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn exclude_layer(&self, layer: String) -> PyExplodedEdgeFilter {
        Self(self.0.clone().exclude_layer(layer))
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn exclude_layers(&self, layers: FromIterable<String>) -> PyExplodedEdgeFilter {
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
    ///     filter.ExplodedEdgeFilter:
    fn valid_layers(&self, layers: FromIterable<String>) -> PyExplodedEdgeFilter {
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
    ///     filter.ExplodedEdgeFilter:
    fn exclude_valid_layers(&self, layers: FromIterable<String>) -> PyExplodedEdgeFilter {
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
    ///     filter.ExplodedEdgeFilter:
    fn shrink_start(&self, start: EventTime) -> PyExplodedEdgeFilter {
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
    ///     filter.ExplodedEdgeFilter:
    fn shrink_end(&self, end: EventTime) -> PyExplodedEdgeFilter {
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
    ///     filter.ExplodedEdgeFilter:
    fn exclude_nodes(&self, nodes: FromIterable<GID>) -> PyExplodedEdgeFilter {
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
    ///     filter.ExplodedEdgeFilter:
    fn subgraph(&self, nodes: FromIterable<GID>) -> PyExplodedEdgeFilter {
        Self(self.0.clone().subgraph(nodes))
    }

    /// Reads through a view of the nodes of the given types and the edges between them.
    ///
    /// Arguments:
    ///     node_types (list[str]): Node types.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn subgraph_node_types(&self, node_types: FromIterable<String>) -> PyExplodedEdgeFilter {
        Self(self.0.clone().subgraph_node_types(node_types))
    }

    /// Reads through a view of the edges that are valid in the current view.
    ///
    /// On a persistent graph an edge is valid when its last update is an addition;
    /// on an event graph when it has at least one addition. Nodes are untouched.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    fn valid(&self) -> PyExplodedEdgeFilter {
        Self(self.0.clone().valid())
    }

    /// Matches exploded edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_active(&self) -> PyExpr {
        PyExpr(self.0.is_active().into())
    }

    /// Matches exploded edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_valid(&self) -> PyExpr {
        PyExpr(self.0.is_valid().into())
    }

    /// Matches exploded edges that have been deleted.
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_deleted(&self) -> PyExpr {
        PyExpr(self.0.is_deleted().into())
    }

    /// Matches exploded edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.Expr:
    fn is_self_loop(&self) -> PyExpr {
        PyExpr(self.0.is_self_loop().into())
    }
}

/// Entry point for constructing exploded-edge filter expressions.
///
/// Every method is static; the view methods return an
/// `ExplodedEdgeFilter` scoped to that view for further chaining.
#[pyclass(frozen, name = "ExplodedEdge", module = "raphtory.filter")]
pub struct PyExplodedEdge;

#[pymethods]
impl PyExplodedEdge {
    /// Filters an exploded edge property by name.
    ///
    /// Reads the property's latest value; `temporal()` switches to its history.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyExpr:
    #[staticmethod]
    fn property(name: String) -> PyPropertyExpr {
        PyExplodedEdgeFilter::root().property(name)
    }

    /// Filters an exploded edge metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of an exploded edge.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn metadata(name: String) -> PyExpr {
        PyExplodedEdgeFilter::root().metadata(name)
    }

    /// Restricts exploded edge evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (TimeInput): Start time.
    ///     end (TimeInput): End time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().window(start, end)
    }

    /// Restricts exploded edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (TimeInput): Event time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn at(time: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().at(time)
    }

    /// Restricts exploded edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Lower time bound.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn after(time: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().after(time)
    }

    /// Restricts exploded edge evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Upper time bound.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn before(time: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().before(time)
    }

    /// Evaluates exploded edge predicates against the latest available state.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn latest() -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().latest()
    }

    /// Evaluates exploded edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Snapshot time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().snapshot_at(time)
    }

    /// Evaluates exploded edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn snapshot_latest() -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().snapshot_latest()
    }

    /// Restricts evaluation to exploded edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn layer(layer: String) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().layer(layer)
    }

    /// Restricts evaluation to exploded edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().layers(layers)
    }

    /// Reads through a view of the default layer only.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn default_layer() -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().default_layer()
    }

    /// Reads through a view of every layer except the given one.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn exclude_layer(layer: String) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().exclude_layer(layer)
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn exclude_layers(layers: FromIterable<String>) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().exclude_layers(layers)
    }

    /// Reads through a view of the given layers.
    ///
    /// A layer name the graph does not have is ignored, where `layers` raises.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn valid_layers(layers: FromIterable<String>) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().valid_layers(layers)
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// A layer name the graph does not have is ignored, where `exclude_layers` raises.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn exclude_valid_layers(layers: FromIterable<String>) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().exclude_valid_layers(layers)
    }

    /// Moves the start of the current window to `start` when that is later.
    ///
    /// The window only ever narrows: a start before the current one changes nothing.
    ///
    /// Arguments:
    ///     start (TimeInput): New start time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn shrink_start(start: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().shrink_start(start)
    }

    /// Moves the end of the current window to `end` when that is earlier.
    ///
    /// The window only ever narrows: an end after the current one changes nothing.
    ///
    /// Arguments:
    ///     end (TimeInput): New end time.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn shrink_end(end: EventTime) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().shrink_end(end)
    }

    /// Reads through a view of every node except the given ones, with their edges.
    ///
    /// An id the view does not hold changes nothing.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn exclude_nodes(nodes: FromIterable<GID>) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().exclude_nodes(nodes)
    }

    /// Reads through a view of the given nodes and the edges between them.
    ///
    /// An id the view does not hold is skipped.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn subgraph(nodes: FromIterable<GID>) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().subgraph(nodes)
    }

    /// Reads through a view of the nodes of the given types and the edges between them.
    ///
    /// Arguments:
    ///     node_types (list[str]): Node types.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn subgraph_node_types(node_types: FromIterable<String>) -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().subgraph_node_types(node_types)
    }

    /// Reads through a view of the edges that are valid in the current view.
    ///
    /// On a persistent graph an edge is valid when its last update is an addition;
    /// on an event graph when it has at least one addition. Nodes are untouched.
    ///
    /// Returns:
    ///     filter.ExplodedEdgeFilter:
    #[staticmethod]
    fn valid() -> PyExplodedEdgeFilter {
        PyExplodedEdgeFilter::root().valid()
    }

    /// Matches exploded edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_active() -> PyExpr {
        PyExplodedEdgeFilter::root().is_active()
    }

    /// Matches exploded edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_valid() -> PyExpr {
        PyExplodedEdgeFilter::root().is_valid()
    }

    /// Matches exploded edges that have been deleted.
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_deleted() -> PyExpr {
        PyExplodedEdgeFilter::root().is_deleted()
    }

    /// Matches exploded edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.Expr:
    #[staticmethod]
    fn is_self_loop() -> PyExpr {
        PyExplodedEdgeFilter::root().is_self_loop()
    }
}
