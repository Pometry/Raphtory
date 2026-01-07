use crate::{
    db::graph::views::filter::model::{
        exploded_edge_filter::ExplodedEdgeFilter,
        property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
        PropertyFilterFactory,
    },
    python::{
        filter::property_filter_builders::{
            PyPropertyExprBuilder, PyPropertyFilterBuilder, PyPropertyFilterFactory,
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyResult, Python};
use raphtory_api::core::{entities::Layer, storage::timeindex::EventTime};

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
    fn window(start: EventTime, end: EventTime) -> PyPropertyFilterFactory {
        PyPropertyFilterFactory::wrap(ExplodedEdgeFilter::window(start, end))
    }

    #[staticmethod]
    fn layer(layer: String) -> PyPropertyFilterFactory {
        PyPropertyFilterFactory::wrap(ExplodedEdgeFilter::layer(layer))
    }

    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyPropertyFilterFactory {
        PyPropertyFilterFactory::wrap(ExplodedEdgeFilter::layer::<Layer>(layers.into()))
    }
}
