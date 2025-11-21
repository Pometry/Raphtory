use crate::{
    db::graph::views::filter::model::{
        edge_filter::EdgeFilter,
        exploded_edge_filter::ExplodedEdgeFilter,
        node_filter::NodeFilter,
        property_filter::{MetadataFilterBuilder, PropertyFilterBuilder},
        Windowed,
    },
    python::filter::property_filter_builders::{PyFilterOps, PyPropertyFilterBuilder},
};
use pyo3::prelude::*;

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
        let b: PropertyFilterBuilder<Windowed<NodeFilter>> =
            PropertyFilterBuilder::new(name, self.0.clone());
        b.into_pyobject(py)
    }

    fn metadata<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b: MetadataFilterBuilder<Windowed<NodeFilter>> =
            MetadataFilterBuilder::new(name, self.0.clone());
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
        let b: PropertyFilterBuilder<Windowed<EdgeFilter>> =
            PropertyFilterBuilder::new(name, self.0.clone());
        b.into_pyobject(py)
    }

    fn metadata<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b: MetadataFilterBuilder<Windowed<EdgeFilter>> =
            MetadataFilterBuilder::new(name, self.0.clone());
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
        let b: PropertyFilterBuilder<Windowed<ExplodedEdgeFilter>> =
            PropertyFilterBuilder::new(name, self.0.clone());
        b.into_pyobject(py)
    }

    fn metadata<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b: MetadataFilterBuilder<Windowed<ExplodedEdgeFilter>> =
            MetadataFilterBuilder::new(name, self.0.clone());
        b.into_pyobject(py)
    }
}
