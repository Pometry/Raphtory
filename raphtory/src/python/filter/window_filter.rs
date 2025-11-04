use crate::{
    db::graph::views::filter::model::{
        edge_filter::{EdgeFilter, EndpointWrapper},
        exploded_edge_filter::ExplodedEdgeFilter,
        node_filter::NodeFilter,
        property_filter::WindowedPropertyRef,
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
pub struct PyNodeWindow(pub Arc<dyn DynWindowedNodeFilter>);

#[pymethods]
impl PyNodeWindow {
    fn property(&self, name: String) -> PyPropertyFilterOps {
        self.0.property(name)
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        self.0.metadata(name)
    }
}

pub(crate) trait DynWindowedNodeFilter: Send + Sync {
    fn property(&self, name: String) -> PyPropertyFilterOps;
    fn metadata(&self, name: String) -> PyPropertyFilterOps;
}

impl DynWindowedNodeFilter for Windowed<NodeFilter> {
    fn property(&self, name: String) -> PyPropertyFilterOps {
        let wpr: WindowedPropertyRef<NodeFilter> = self.property(name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        let wpr: WindowedPropertyRef<NodeFilter> = self.metadata(name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }
}

impl DynWindowedNodeFilter for EndpointWrapper<Windowed<NodeFilter>> {
    fn property(&self, name: String) -> PyPropertyFilterOps {
        let wpr: EndpointWrapper<WindowedPropertyRef<NodeFilter>> =
            EndpointWrapper::property(self, name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        let wpr: EndpointWrapper<WindowedPropertyRef<NodeFilter>> =
            EndpointWrapper::metadata(self, name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }
}

#[pyclass(frozen, name = "EdgeWindow", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeWindow(pub Windowed<EdgeFilter>);

#[pymethods]
impl PyEdgeWindow {
    fn property(&self, name: String) -> PyPropertyFilterOps {
        let wpr: WindowedPropertyRef<EdgeFilter> = self.0.clone().property(name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        let wpr: WindowedPropertyRef<EdgeFilter> = self.0.clone().metadata(name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }
}

#[pyclass(frozen, name = "ExplodedEdgeWindow", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeWindow(pub Windowed<ExplodedEdgeFilter>);

#[pymethods]
impl PyExplodedEdgeWindow {
    fn property(&self, name: String) -> PyPropertyFilterOps {
        let wpr: WindowedPropertyRef<ExplodedEdgeFilter> = self.0.clone().property(name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        let wpr: WindowedPropertyRef<ExplodedEdgeFilter> = self.0.clone().metadata(name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }
}
