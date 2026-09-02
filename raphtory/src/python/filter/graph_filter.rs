use crate::{
    db::graph::views::filter::model::{graph_filter::GraphFilter, DynView, ViewWrapOps},
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;

/// Entry point for constructing **graph-level view filters**.
///
/// The `Graph` filter restricts *when* and *where* the graph is evaluated,
/// independent of node or edge predicates. It defines the **temporal scope**
/// (windows, snapshots, latest state) and **layer scope** for subsequent
/// node and edge filters.
///
/// All methods are static and return a `Graph`, which can then
/// be refined further or combined with node/edge predicates.
///
/// Examples:
///     Graph.window(0, 10)
///     Graph.at(5)
///     Graph.latest().layer("fire_nation")
///     Graph.layers(["A", "B"]).snapshot_latest()
#[pyclass(
    name = "Graph",
    module = "raphtory.filter",
    extends = PyFilterExpr,
    frozen
)]
pub struct PyGraphFilter(pub(crate) DynView);

impl PyGraphFilter {
    pub(crate) fn root() -> Self {
        PyGraphFilter(Arc::new(GraphFilter))
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
    ///     filter.Graph:
    fn window(&self, start: EventTime, end: EventTime) -> PyGraphFilter {
        PyGraphFilter(self.0.clone().window(start, end))
    }

    /// Restricts evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.Graph:
    fn at(&self, time: EventTime) -> PyGraphFilter {
        PyGraphFilter(self.0.clone().at(time))
    }

    /// Restricts evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.Graph:
    fn after(&self, time: EventTime) -> PyGraphFilter {
        PyGraphFilter(self.0.clone().after(time))
    }

    /// Restricts evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.Graph:
    fn before(&self, time: EventTime) -> PyGraphFilter {
        PyGraphFilter(self.0.clone().before(time))
    }

    /// Evaluates filters against the latest available state of the graph.
    ///
    /// Returns:
    ///     filter.Graph:
    fn latest(&self) -> PyGraphFilter {
        PyGraphFilter(Arc::new(self.0.clone().latest()))
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.Graph:
    fn snapshot_at(&self, time: EventTime) -> PyGraphFilter {
        PyGraphFilter(Arc::new(self.0.clone().snapshot_at(time)))
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.Graph:
    fn snapshot_latest(&self) -> PyGraphFilter {
        PyGraphFilter(Arc::new(self.0.clone().snapshot_latest()))
    }

    /// Restricts evaluation to a single layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.Graph:
    fn layer(&self, layer: String) -> PyGraphFilter {
        PyGraphFilter(Arc::new(self.0.clone().layer(layer)))
    }

    /// Restricts evaluation to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.Graph:
    fn layers(&self, layers: FromIterable<String>) -> PyGraphFilter {
        PyGraphFilter(Arc::new(self.0.clone().layer(layers)))
    }
}

impl<'py> IntoPyObject<'py> for PyGraphFilter {
    type Target = PyGraphFilter;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyFilterExpr(self.0.clone());
        Bound::new(py, (self, parent))
    }
}
