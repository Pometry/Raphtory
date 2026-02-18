use crate::{
    db::graph::views::filter::model::{
        exploded_edge_filter::ExplodedEdgeFilter,
        property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
        EdgeViewFilterOps, PropertyFilterFactory, ViewWrapOps,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            property_filter_builders::{
                PyEdgeViewPropsFilterBuilder, PyPropertyExprBuilder, PyPropertyFilterBuilder,
            },
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyResult, Python};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;

/// Entry point for constructing **exploded edge** filter expressions.
///
/// An **exploded edge** represents an edge view where temporal events are treated
/// as individually addressable edge instances (i.e. “event-level” edges), rather
/// than a single aggregated edge across time.
///
/// This filter provides:
/// - property and metadata filters,
/// - view restrictions (time windows, snapshots, layers),
/// - and structural predicates over exploded edge state (active/valid/deleted/self-loop).
///
/// Examples:
///     ExplodedEdge.property("weight") > 0.5
///     ExplodedEdge.window(0, 10).is_active()
///     ExplodedEdge.layer("fire_nation").is_valid()
#[pyclass(frozen, name = "ExplodedEdge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeFilter;

#[pymethods]
impl PyExplodedEdgeFilter {
    /// Filters an exploded edge property by name.
    ///
    /// The property may be static or temporal depending on the query context.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyFilterOps
    #[staticmethod]
    fn property<'py>(
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b: PropertyFilterBuilder<ExplodedEdgeFilter> =
            PropertyFilterFactory::property(&ExplodedEdgeFilter, name);
        b.into_pyobject(py)
    }

    /// Filters an exploded edge metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of an exploded edge.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.FilterOps
    #[staticmethod]
    fn metadata<'py>(py: Python<'py>, name: String) -> PyResult<Bound<'py, PyPropertyExprBuilder>> {
        let b: MetadataFilterBuilder<ExplodedEdgeFilter> =
            PropertyFilterFactory::metadata(&ExplodedEdgeFilter, name);
        b.into_pyobject(py)
    }

    /// Restricts exploded edge evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (int): Start time.
    ///     end (int): End time.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.window(start, end)))
    }

    /// Restricts exploded edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn at(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.at(time)))
    }

    /// Restricts exploded edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn after(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.after(time)))
    }

    /// Restricts exploded edge evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn before(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.before(time)))
    }

    /// Evaluates exploded edge predicates against the latest available state.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn latest() -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.latest()))
    }

    /// Evaluates exploded edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.snapshot_at(time)))
    }

    /// Evaluates exploded edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn snapshot_latest() -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.snapshot_latest()))
    }

    /// Restricts evaluation to exploded edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn layer(layer: String) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.layer(layer)))
    }

    /// Restricts evaluation to exploded edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.layer(layers)))
    }

    /// Matches exploded edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr
    #[staticmethod]
    fn is_active() -> PyFilterExpr {
        PyFilterExpr(Arc::new(ExplodedEdgeFilter.is_active()))
    }

    /// Matches exploded edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr
    #[staticmethod]
    fn is_valid() -> PyFilterExpr {
        PyFilterExpr(Arc::new(ExplodedEdgeFilter.is_valid()))
    }

    /// Matches exploded edges that have been deleted.
    ///
    /// Returns:
    ///     filter.FilterExpr
    #[staticmethod]
    fn is_deleted() -> PyFilterExpr {
        PyFilterExpr(Arc::new(ExplodedEdgeFilter.is_deleted()))
    }

    /// Matches exploded edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.FilterExpr
    #[staticmethod]
    fn is_self_loop() -> PyFilterExpr {
        PyFilterExpr(Arc::new(ExplodedEdgeFilter.is_self_loop()))
    }
}
