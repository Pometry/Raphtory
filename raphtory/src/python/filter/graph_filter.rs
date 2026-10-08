use crate::{
    db::graph::views::filter::model::{
        expr::{Chain, FilterExpr},
        graph_filter::GraphFilter,
        ViewWrapOps,
    },
    python::{
        filter::{filter_expr::PyFilterExpr, repr},
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, PyResult, Python};
use raphtory_api::core::{entities::GID, storage::timeindex::EventTime};

/// A graph-level view scope.
///
/// Obtained from the view methods on `Graph` (`Graph.window(...)`,
/// `Graph.latest()`, ...). It carries no node or edge predicate of its own: it
/// fixes the temporal and layer scope that node and edge predicates compose
/// with, and its own view methods narrow it further.
#[pyclass(
    name = "GraphFilter",
    module = "raphtory.filter",
    extends = PyFilterExpr,
    frozen
)]
pub struct PyGraphFilter(pub(crate) Chain<()>);

impl PyGraphFilter {
    pub(crate) fn root() -> Self {
        PyGraphFilter(GraphFilter.into())
    }
}

#[pymethods]
impl PyGraphFilter {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        repr::factory(py, "Graph", self.0.views())
    }

    /// Restricts evaluation to events within a time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (TimeInput): Start time.
    ///     end (TimeInput): End time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn window(&self, start: EventTime, end: EventTime) -> PyGraphFilter {
        Self(self.0.clone().window(start, end))
    }

    /// Restricts evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (TimeInput): Event time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn at(&self, time: EventTime) -> PyGraphFilter {
        Self(self.0.clone().at(time))
    }

    /// Restricts evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Lower time bound.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn after(&self, time: EventTime) -> PyGraphFilter {
        Self(self.0.clone().after(time))
    }

    /// Restricts evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Upper time bound.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn before(&self, time: EventTime) -> PyGraphFilter {
        Self(self.0.clone().before(time))
    }

    /// Evaluates filters against the latest available state of the graph.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn latest(&self) -> PyGraphFilter {
        Self(self.0.clone().latest())
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Snapshot time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn snapshot_at(&self, time: EventTime) -> PyGraphFilter {
        Self(self.0.clone().snapshot_at(time))
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn snapshot_latest(&self) -> PyGraphFilter {
        Self(self.0.clone().snapshot_latest())
    }

    /// Restricts evaluation to a single layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn layer(&self, layer: String) -> PyGraphFilter {
        Self(self.0.clone().layer(layer))
    }

    /// Restricts evaluation to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn layers(&self, layers: FromIterable<String>) -> PyGraphFilter {
        Self(self.0.clone().layer(Vec::<String>::from(layers)))
    }

    /// Reads through a view of the default layer only.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn default_layer(&self) -> PyGraphFilter {
        Self(self.0.clone().default_layer())
    }

    /// Reads through a view of every layer except the given one.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn exclude_layer(&self, layer: String) -> PyGraphFilter {
        Self(self.0.clone().exclude_layer(layer))
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn exclude_layers(&self, layers: FromIterable<String>) -> PyGraphFilter {
        Self(self.0.clone().exclude_layers(layers))
    }

    /// Moves the start of the current window to `start` when that is later.
    ///
    /// The window only ever narrows: a start before the current one changes nothing.
    ///
    /// Arguments:
    ///     start (TimeInput): New start time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn shrink_start(&self, start: EventTime) -> PyGraphFilter {
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
    ///     filter.GraphFilter:
    fn shrink_end(&self, end: EventTime) -> PyGraphFilter {
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
    ///     filter.GraphFilter:
    fn exclude_nodes(&self, nodes: FromIterable<GID>) -> PyGraphFilter {
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
    ///     filter.GraphFilter:
    fn subgraph(&self, nodes: FromIterable<GID>) -> PyGraphFilter {
        Self(self.0.clone().subgraph(nodes))
    }

    /// Reads through a view of the nodes of the given types and the edges between them.
    ///
    /// Arguments:
    ///     node_types (list[str]): Node types.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn subgraph_node_types(&self, node_types: FromIterable<String>) -> PyGraphFilter {
        Self(self.0.clone().subgraph_node_types(node_types))
    }

    /// Reads through a view of the edges that are valid in the current view.
    ///
    /// On a persistent graph an edge is valid when its last update is an addition;
    /// on an event graph when it has at least one addition. Nodes are untouched.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn valid(&self) -> PyGraphFilter {
        Self(self.0.clone().valid())
    }
}

/// Entry point for graph-level view filters.
///
/// Every method is static and returns a `GraphFilter` carrying the view,
/// which composes with node and edge predicates.
#[pyclass(frozen, name = "Graph", module = "raphtory.filter")]
pub struct PyGraph;

#[pymethods]
impl PyGraph {
    /// Restricts evaluation to events within a time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (TimeInput): Start time.
    ///     end (TimeInput): End time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyGraphFilter {
        PyGraphFilter::root().window(start, end)
    }

    /// Restricts evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (TimeInput): Event time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn at(time: EventTime) -> PyGraphFilter {
        PyGraphFilter::root().at(time)
    }

    /// Restricts evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Lower time bound.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn after(time: EventTime) -> PyGraphFilter {
        PyGraphFilter::root().after(time)
    }

    /// Restricts evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Upper time bound.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn before(time: EventTime) -> PyGraphFilter {
        PyGraphFilter::root().before(time)
    }

    /// Evaluates filters against the latest available state of the graph.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn latest() -> PyGraphFilter {
        PyGraphFilter::root().latest()
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (TimeInput): Snapshot time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyGraphFilter {
        PyGraphFilter::root().snapshot_at(time)
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn snapshot_latest() -> PyGraphFilter {
        PyGraphFilter::root().snapshot_latest()
    }

    /// Restricts evaluation to a single layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn layer(layer: String) -> PyGraphFilter {
        PyGraphFilter::root().layer(layer)
    }

    /// Restricts evaluation to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyGraphFilter {
        PyGraphFilter::root().layers(layers)
    }

    /// Reads through a view of the default layer only.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn default_layer() -> PyGraphFilter {
        PyGraphFilter::root().default_layer()
    }

    /// Reads through a view of every layer except the given one.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn exclude_layer(layer: String) -> PyGraphFilter {
        PyGraphFilter::root().exclude_layer(layer)
    }

    /// Reads through a view of every layer except the given ones.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn exclude_layers(layers: FromIterable<String>) -> PyGraphFilter {
        PyGraphFilter::root().exclude_layers(layers)
    }

    /// Moves the start of the current window to `start` when that is later.
    ///
    /// The window only ever narrows: a start before the current one changes nothing.
    ///
    /// Arguments:
    ///     start (TimeInput): New start time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn shrink_start(start: EventTime) -> PyGraphFilter {
        PyGraphFilter::root().shrink_start(start)
    }

    /// Moves the end of the current window to `end` when that is earlier.
    ///
    /// The window only ever narrows: an end after the current one changes nothing.
    ///
    /// Arguments:
    ///     end (TimeInput): New end time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn shrink_end(end: EventTime) -> PyGraphFilter {
        PyGraphFilter::root().shrink_end(end)
    }

    /// Reads through a view of every node except the given ones, with their edges.
    ///
    /// An id the view does not hold changes nothing.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn exclude_nodes(nodes: FromIterable<GID>) -> PyGraphFilter {
        PyGraphFilter::root().exclude_nodes(nodes)
    }

    /// Reads through a view of the given nodes and the edges between them.
    ///
    /// An id the view does not hold is skipped.
    ///
    /// Arguments:
    ///     nodes (list[str | int]): Node ids or names.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn subgraph(nodes: FromIterable<GID>) -> PyGraphFilter {
        PyGraphFilter::root().subgraph(nodes)
    }

    /// Reads through a view of the nodes of the given types and the edges between them.
    ///
    /// Arguments:
    ///     node_types (list[str]): Node types.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn subgraph_node_types(node_types: FromIterable<String>) -> PyGraphFilter {
        PyGraphFilter::root().subgraph_node_types(node_types)
    }

    /// Reads through a view of the edges that are valid in the current view.
    ///
    /// On a persistent graph an edge is valid when its last update is an addition;
    /// on an event graph when it has at least one addition. Nodes are untouched.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    #[staticmethod]
    fn valid() -> PyGraphFilter {
        PyGraphFilter::root().valid()
    }
}

impl<'py> IntoPyObject<'py> for PyGraphFilter {
    type Target = PyGraphFilter;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyFilterExpr(FilterExpr::from(self.0.clone()));
        Bound::new(py, (self, parent))
    }
}
