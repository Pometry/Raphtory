use crate::{
    db::graph::views::filter::model::tree::{FilterExpr, ViewOp},
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};
use raphtory_api::core::storage::timeindex::EventTime;

/// A graph-level view scope.
///
/// Obtained from the view methods on [`Graph`] (`Graph.window(...)`,
/// `Graph.latest()`, ...). It carries no node or edge predicate of its own: it
/// fixes the temporal and layer scope that node and edge predicates compose
/// with, and its own view methods narrow it further.
#[pyclass(
    name = "GraphFilter",
    module = "raphtory.filter",
    extends = PyFilterExpr,
    frozen
)]
pub struct PyGraphFilter(pub(crate) Vec<ViewOp>);

impl PyGraphFilter {
    pub(crate) fn root() -> Self {
        PyGraphFilter(Vec::new())
    }

    fn with_view(&self, view: ViewOp) -> Self {
        let mut ops = self.0.clone();
        ops.push(view);
        PyGraphFilter(ops)
    }
}

#[pymethods]
impl PyGraphFilter {
    /// Restricts evaluation to events within a time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (int): Start time.
    ///     end (int): End time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn window(&self, start: EventTime, end: EventTime) -> PyGraphFilter {
        self.with_view(ViewOp::Window { start, end })
    }

    /// Restricts evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn at(&self, time: EventTime) -> PyGraphFilter {
        self.with_view(ViewOp::At(time))
    }

    /// Restricts evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn after(&self, time: EventTime) -> PyGraphFilter {
        self.with_view(ViewOp::After(time))
    }

    /// Restricts evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn before(&self, time: EventTime) -> PyGraphFilter {
        self.with_view(ViewOp::Before(time))
    }

    /// Evaluates filters against the latest available state of the graph.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn latest(&self) -> PyGraphFilter {
        self.with_view(ViewOp::Latest)
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn snapshot_at(&self, time: EventTime) -> PyGraphFilter {
        self.with_view(ViewOp::SnapshotAt(time))
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn snapshot_latest(&self) -> PyGraphFilter {
        self.with_view(ViewOp::SnapshotLatest)
    }

    /// Restricts evaluation to a single layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn layer(&self, layer: String) -> PyGraphFilter {
        self.with_view(ViewOp::Layers(vec![layer]))
    }

    /// Restricts evaluation to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.GraphFilter:
    fn layers(&self, layers: FromIterable<String>) -> PyGraphFilter {
        self.with_view(ViewOp::Layers(layers.into()))
    }
}

/// Entry point for graph-level view filters.
///
/// Every method is static and returns a [`GraphFilter`] carrying the view,
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
    ///     start (int): Start time.
    ///     end (int): End time.
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
    ///     time (int): Event time.
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
    ///     time (int): Lower time bound.
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
    ///     time (int): Upper time bound.
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
    ///     time (int): Snapshot time.
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
}

impl<'py> IntoPyObject<'py> for PyGraphFilter {
    type Target = PyGraphFilter;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyFilterExpr(FilterExpr::View(self.0.clone()));
        Bound::new(py, (self, parent))
    }
}
