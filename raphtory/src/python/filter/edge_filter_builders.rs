use crate::{
    db::graph::views::filter::model::{
        edge_filter::{EdgeEndpoint, EdgeFilter, EndpointWrapper},
        exploded_edge_filter::ExplodedEdgeFilter,
        node_filter::NodeFilter,
        property_filter::{MetadataFilterBuilder, PropertyFilterBuilder},
        PropertyFilterFactory, Windowed,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            node_filter_builders::{PyNodeFilterBuilder, PyNodeIdFilterBuilder},
            window_filter::{py_into_millis, PyEdgeWindow, PyExplodedEdgeWindow, PyNodeWindow},
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{exceptions::PyTypeError, pyclass, pymethods, Bound, PyAny, PyResult};
use raphtory_api::core::entities::GID;
use std::sync::Arc;

#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpoint(pub EdgeEndpoint);

#[pymethods]
impl PyEdgeEndpoint {
    fn id(&self) -> PyNodeIdFilterBuilder {
        self.0.id().into()
    }

    fn name(&self) -> PyNodeFilterBuilder {
        self.0.name().into()
    }

    fn node_type(&self) -> PyNodeFilterBuilder {
        self.0.node_type().into()
    }

    fn property(&self, name: String) -> EndpointWrapper<PropertyFilterBuilder<NodeFilter>> {
        self.0.property(name)
    }

    fn metadata(&self, name: String) -> EndpointWrapper<MetadataFilterBuilder<NodeFilter>> {
        self.0.metadata(name)
    }

    fn window(&self, py_start: Bound<PyAny>, py_end: Bound<PyAny>) -> PyResult<PyNodeWindow> {
        let s = py_into_millis(&py_start)?;
        let e = py_into_millis(&py_end)?;
        if s > e {
            return Err(PyTypeError::new_err("window.start must be <= window.end"));
        }
        Ok(PyNodeWindow(Arc::new(self.0.window(s, e))))
    }
}

#[pyclass(frozen, name = "Edge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilter;

#[pymethods]
impl PyEdgeFilter {
    #[staticmethod]
    fn src() -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::src())
    }

    #[staticmethod]
    fn dst() -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::dst())
    }

    #[staticmethod]
    fn property(name: String) -> PropertyFilterBuilder<EdgeFilter> {
        EdgeFilter::property(name)
    }

    #[staticmethod]
    fn metadata(name: String) -> MetadataFilterBuilder<EdgeFilter> {
        EdgeFilter::metadata(name)
    }

    #[staticmethod]
    fn window(py_start: Bound<PyAny>, py_end: Bound<PyAny>) -> PyResult<PyEdgeWindow> {
        let s = py_into_millis(&py_start)?;
        let e = py_into_millis(&py_end)?;
        if s > e {
            return Err(PyTypeError::new_err("window.start must be <= window.end"));
        }
        Ok(PyEdgeWindow(Windowed::<EdgeFilter>::from_times(s, e)))
    }
}

#[pyclass(frozen, name = "ExplodedEdge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeFilter;

#[pymethods]
impl PyExplodedEdgeFilter {
    #[staticmethod]
    fn src() -> PyEdgeEndpoint {
        PyEdgeEndpoint(ExplodedEdgeFilter::src())
    }

    #[staticmethod]
    fn dst() -> PyEdgeEndpoint {
        PyEdgeEndpoint(ExplodedEdgeFilter::dst())
    }

    #[staticmethod]
    fn property(name: String) -> PropertyFilterBuilder<ExplodedEdgeFilter> {
        ExplodedEdgeFilter::property(name)
    }

    #[staticmethod]
    fn metadata(name: String) -> MetadataFilterBuilder<ExplodedEdgeFilter> {
        ExplodedEdgeFilter::metadata(name)
    }

    #[staticmethod]
    fn window(py_start: Bound<PyAny>, py_end: Bound<PyAny>) -> PyResult<PyExplodedEdgeWindow> {
        let s = py_into_millis(&py_start)?;
        let e = py_into_millis(&py_end)?;
        if s > e {
            return Err(PyTypeError::new_err("window.start must be <= window.end"));
        }
        Ok(PyExplodedEdgeWindow(
            Windowed::<ExplodedEdgeFilter>::from_times(s, e),
        ))
    }
}
