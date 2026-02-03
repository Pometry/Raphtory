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
                PyEdgeViewPropsFilterBuilder, PyNodeViewPropsFilterBuilder, PyPropertyExprBuilder,
                PyPropertyFilterBuilder,
            },
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyResult, Python};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;

#[pyclass(frozen, name = "ExplodedEdge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeFilter;

#[pymethods]
impl PyExplodedEdgeFilter {
    #[staticmethod]
    fn property<'py>(
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b: PropertyFilterBuilder<ExplodedEdgeFilter> =
            PropertyFilterFactory::property(&ExplodedEdgeFilter, name);
        b.into_pyobject(py)
    }

    #[staticmethod]
    fn metadata<'py>(py: Python<'py>, name: String) -> PyResult<Bound<'py, PyPropertyExprBuilder>> {
        let b: MetadataFilterBuilder<ExplodedEdgeFilter> =
            PropertyFilterFactory::metadata(&ExplodedEdgeFilter, name);
        b.into_pyobject(py)
    }

    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.window(start, end)))
    }

    #[staticmethod]
    fn at(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.at(time)))
    }

    #[staticmethod]
    fn after(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.after(time)))
    }

    #[staticmethod]
    fn before(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.before(time)))
    }

    #[staticmethod]
    fn latest() -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.latest()))
    }

    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.snapshot_at(time)))
    }

    #[staticmethod]
    fn snapshot_latest() -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.snapshot_latest()))
    }

    #[staticmethod]
    fn layer(layer: String) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.layer(layer)))
    }

    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.layer(layers)))
    }

    #[staticmethod]
    fn is_active() -> PyFilterExpr {
        PyFilterExpr(Arc::new(ExplodedEdgeFilter.is_active()))
    }

    #[staticmethod]
    fn is_valid() -> PyFilterExpr {
        PyFilterExpr(Arc::new(ExplodedEdgeFilter.is_valid()))
    }

    #[staticmethod]
    fn is_deleted() -> PyFilterExpr {
        PyFilterExpr(Arc::new(ExplodedEdgeFilter.is_deleted()))
    }

    #[staticmethod]
    fn is_self_loop() -> PyFilterExpr {
        PyFilterExpr(Arc::new(ExplodedEdgeFilter.is_self_loop()))
    }
}
