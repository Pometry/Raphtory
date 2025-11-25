use crate::{
    db::graph::views::filter::{
        internal::CreateFilter,
        model::{
            edge_filter::EdgeFilter,
            exploded_edge_filter::ExplodedEdgeFilter,
            node_filter::NodeFilter,
            property_filter::{MetadataFilterBuilder, PropertyFilterBuilder},
            PropertyFilterFactory, TryAsCompositeFilter, Windowed,
        },
    },
    prelude::PropertyFilter,
    python::filter::property_filter_builders::{PyFilterOps, PyPropertyFilterBuilder},
};
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(frozen, name = "NodeWindow", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeWindow(pub Windowed<NodeFilter>);

#[pymethods]
impl PyNodeWindow {
    fn property<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b = self.0.property(name);
        b.into_pyobject(py)
    }

    fn metadata<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b = self.0.metadata(name);
        b.into_pyobject(py)
    }
}

#[pyclass(frozen, name = "EdgeWindow", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeWindow(pub Windowed<EdgeFilter>);

#[pymethods]
impl PyEdgeWindow {
    fn property<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b = self.0.property(name);
        b.into_pyobject(py)
    }

    fn metadata<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b = self.0.metadata(name);
        b.into_pyobject(py)
    }
}

#[pyclass(frozen, name = "ExplodedEdgeWindow", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeWindow(pub Windowed<ExplodedEdgeFilter>);

#[pymethods]
impl PyExplodedEdgeWindow {
    fn property<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b = self.0.property(name);
        b.into_pyobject(py)
    }

    fn metadata<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b = self.0.metadata(name);
        b.into_pyobject(py)
    }
}
