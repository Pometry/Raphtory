use crate::{
    db::graph::views::filter::model::{
        exploded_edge_filter::ExplodedEdgeFilter,
        property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
        PropertyFilterFactory, ViewWrapOps,
    },
    python::{
        filter::property_filter_builders::{
            PyPropertyExprBuilder, PyPropertyFilterBuilder, PyViewPropsFilterBuilder,
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
    fn window(start: EventTime, end: EventTime) -> PyViewPropsFilterBuilder {
        PyViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.window(start, end)))
    }

    #[staticmethod]
    fn at(time: EventTime) -> PyViewPropsFilterBuilder {
        PyViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.at(time)))
    }

    #[staticmethod]
    fn after(time: EventTime) -> PyViewPropsFilterBuilder {
        PyViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.after(time)))
    }

    #[staticmethod]
    fn before(time: EventTime) -> PyViewPropsFilterBuilder {
        PyViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.before(time)))
    }

    #[staticmethod]
    fn latest() -> PyViewPropsFilterBuilder {
        PyViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.latest()))
    }

    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyViewPropsFilterBuilder {
        PyViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.snapshot_at(time)))
    }

    #[staticmethod]
    fn snapshot_latest() -> PyViewPropsFilterBuilder {
        PyViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.snapshot_latest()))
    }

    #[staticmethod]
    fn layer(layer: String) -> PyViewPropsFilterBuilder {
        PyViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.layer(layer)))
    }

    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyViewPropsFilterBuilder {
        PyViewPropsFilterBuilder(Arc::new(ExplodedEdgeFilter.layer(layers)))
    }
}
