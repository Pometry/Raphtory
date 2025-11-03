use crate::{
    db::graph::views::filter::model::{
        edge_filter::{EdgeFilter, ExplodedEdgeFilter},
        node_filter::NodeFilter,
        property_filter::{MetadataFilterBuilder, PropertyFilterBuilder},
        Windowed,
    },
    python::filter::property_filter_builders::PyPropertyFilterOps,
};
use pyo3::prelude::*;
use std::sync::Arc;

pub fn py_into_millis(obj: &Bound<PyAny>) -> PyResult<i64> {
    obj.extract::<i64>()
}

#[pyclass(frozen, name = "NodeWindow", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeWindow(pub Windowed<NodeFilter>);

#[pymethods]
impl PyNodeWindow {
    fn property(&self, name: String) -> PyPropertyFilterOps {
        let b: PropertyFilterBuilder<Windowed<NodeFilter>> =
            PropertyFilterBuilder::new(name, self.0.clone());
        PyPropertyFilterOps::from_arc(Arc::new(b))
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        let b: MetadataFilterBuilder<Windowed<NodeFilter>> =
            MetadataFilterBuilder::new(name, self.0.clone());
        PyPropertyFilterOps::from_arc(Arc::new(b))
    }
}

#[pyclass(frozen, name = "EdgeWindow", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeWindow(pub Windowed<EdgeFilter>);

#[pymethods]
impl PyEdgeWindow {
    fn property(&self, name: String) -> PyPropertyFilterOps {
        let b: PropertyFilterBuilder<Windowed<EdgeFilter>> =
            PropertyFilterBuilder::new(name, self.0.clone());
        PyPropertyFilterOps::from_arc(Arc::new(b))
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        let b: MetadataFilterBuilder<Windowed<EdgeFilter>> =
            MetadataFilterBuilder::new(name, self.0.clone());
        PyPropertyFilterOps::from_arc(Arc::new(b))
    }
}

#[pyclass(frozen, name = "ExplodedEdgeWindow", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeWindow(pub Windowed<ExplodedEdgeFilter>);

#[pymethods]
impl PyExplodedEdgeWindow {
    fn property(&self, name: String) -> PyPropertyFilterOps {
        let b: PropertyFilterBuilder<Windowed<ExplodedEdgeFilter>> =
            PropertyFilterBuilder::new(name, self.0.clone());
        PyPropertyFilterOps::from_arc(Arc::new(b))
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        let b: MetadataFilterBuilder<Windowed<ExplodedEdgeFilter>> =
            MetadataFilterBuilder::new(name, self.0.clone());
        PyPropertyFilterOps::from_arc(Arc::new(b))
    }
}
