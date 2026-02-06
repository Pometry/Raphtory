use crate::{
    db::graph::views::filter::model::{graph_filter::GraphFilter, ViewWrapOps},
    python::{
        filter::property_filter_builders::PyViewFilterBuilder, types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;

/// Entry point for constructing **graph-level view filters**.
///
/// The `Graph` filter restricts *when* and *where* the graph is evaluated,
/// independent of node or edge predicates. It defines the **temporal scope**
/// (windows, snapshots, latest state) and **layer scope** for subsequent
/// node and edge filters.
///
/// All methods are static and return a `ViewFilterBuilder`, which can then
/// be refined further or combined with node/edge predicates.
///
/// Examples:
///     Graph.window(0, 10)
///     Graph.at(5)
///     Graph.latest().layer("fire_nation")
///     Graph.layers(["A", "B"]).snapshot_latest()
#[pyclass(frozen, name = "Graph", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyGraphFilter;

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
    ///     filter.ViewFilterBuilder
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.window(start, end)))
    }

    /// Restricts evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.ViewFilterBuilder
    #[staticmethod]
    fn at(time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.at(time)))
    }

    /// Restricts evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.ViewFilterBuilder
    #[staticmethod]
    fn after(time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.after(time)))
    }

    /// Restricts evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.ViewFilterBuilder
    #[staticmethod]
    fn before(time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.before(time)))
    }

    /// Evaluates filters against the latest available state of the graph.
    ///
    /// Returns:
    ///     filter.ViewFilterBuilder
    #[staticmethod]
    fn latest() -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.latest()))
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.ViewFilterBuilder
    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.snapshot_at(time)))
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.ViewFilterBuilder
    #[staticmethod]
    fn snapshot_latest() -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.snapshot_latest()))
    }

    /// Restricts evaluation to a single layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.ViewFilterBuilder
    #[staticmethod]
    fn layer(layer: String) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.layer(layer)))
    }

    /// Restricts evaluation to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.ViewFilterBuilder
    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.layer(layers)))
    }
}
