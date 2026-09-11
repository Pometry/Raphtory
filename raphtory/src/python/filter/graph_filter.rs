use crate::{
    db::graph::views::filter::model::{
        graph_filter::GraphFilter, DynView, FilterTree, GraphViewOp, ViewWrapOps,
    },
    prelude::Layer,
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};
use raphtory_api::core::storage::timeindex::{AsTime, EventTime};
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
pub struct PyGraphFilter(pub(crate) DynView, pub(crate) Vec<GraphViewOp>);

impl PyGraphFilter {
    pub(crate) fn root() -> Self {
        PyGraphFilter(Arc::new(GraphFilter), Vec::new())
    }

    fn extend(&self, view: DynView, op: GraphViewOp) -> Self {
        let mut ops = self.1.clone();
        ops.push(op);
        PyGraphFilter(view, ops)
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
        self.extend(
            self.0.clone().window(start, end),
            GraphViewOp::Window { start, end },
        )
    }

    /// Restricts evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.Graph:
    fn at(&self, time: EventTime) -> PyGraphFilter {
        self.extend(
            self.0.clone().at(time),
            GraphViewOp::Window {
                start: time,
                end: EventTime::end(time.t().saturating_add(1)),
            },
        )
    }

    /// Restricts evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.Graph:
    fn after(&self, time: EventTime) -> PyGraphFilter {
        self.extend(
            self.0.clone().after(time),
            GraphViewOp::Window {
                start: EventTime::start(time.t().saturating_add(1)),
                end: EventTime::end(i64::MAX),
            },
        )
    }

    /// Restricts evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.Graph:
    fn before(&self, time: EventTime) -> PyGraphFilter {
        self.extend(
            self.0.clone().before(time),
            GraphViewOp::Window {
                start: EventTime::start(i64::MIN),
                end: EventTime::end(time.t()),
            },
        )
    }

    /// Evaluates filters against the latest available state of the graph.
    ///
    /// Returns:
    ///     filter.Graph:
    fn latest(&self) -> PyGraphFilter {
        self.extend(Arc::new(self.0.clone().latest()), GraphViewOp::Latest)
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.Graph:
    fn snapshot_at(&self, time: EventTime) -> PyGraphFilter {
        self.extend(
            Arc::new(self.0.clone().snapshot_at(time)),
            GraphViewOp::SnapshotAt(time),
        )
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.Graph:
    fn snapshot_latest(&self) -> PyGraphFilter {
        self.extend(
            Arc::new(self.0.clone().snapshot_latest()),
            GraphViewOp::SnapshotLatest,
        )
    }

    /// Restricts evaluation to a single layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.Graph:
    fn layer(&self, layer: String) -> PyGraphFilter {
        self.extend(
            Arc::new(self.0.clone().layer(layer.clone())),
            GraphViewOp::Layers(Layer::from(layer)),
        )
    }

    /// Restricts evaluation to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.Graph:
    fn layers(&self, layers: FromIterable<String>) -> PyGraphFilter {
        let names: Vec<String> = layers.into();
        self.extend(
            Arc::new(self.0.clone().layer(names.clone())),
            GraphViewOp::Layers(Layer::from(names)),
        )
    }
}

impl<'py> IntoPyObject<'py> for PyGraphFilter {
    type Target = PyGraphFilter;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyFilterExpr(self.0.clone(), Some(FilterTree::View(self.1.clone())));
        Bound::new(py, (self, parent))
    }
}
