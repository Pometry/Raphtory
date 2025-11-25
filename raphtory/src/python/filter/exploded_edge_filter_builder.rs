use crate::{
    db::graph::views::filter::model::{
        exploded_edge_filter::ExplodedEdgeFilter,
        property_filter::{MetadataFilterBuilder, PropertyFilterBuilder},
        PropertyFilterFactory, Windowed,
    },
    python::{
        filter::{
            property_filter_builders::{PyFilterOps, PyPropertyFilterBuilder},
            window_filter::PyExplodedEdgeWindow,
        },
        utils::PyTime,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyResult, Python};

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
    fn metadata<'py>(py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b: MetadataFilterBuilder<ExplodedEdgeFilter> =
            PropertyFilterFactory::metadata(&ExplodedEdgeFilter, name);
        b.into_pyobject(py)
    }

    #[staticmethod]
    fn window(start: PyTime, end: PyTime) -> PyResult<PyExplodedEdgeWindow> {
        Ok(PyExplodedEdgeWindow(Windowed::from_times(
            start,
            end,
            ExplodedEdgeFilter,
        )))
    }
}
