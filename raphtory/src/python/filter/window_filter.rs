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
use crate::db::graph::views::filter::internal::CreateFilter;
use crate::db::graph::views::filter::model::{PropertyFilterFactory, TryAsCompositeFilter};
use crate::db::graph::views::filter::model::property_filter::{MetadataFilterBuilder, PropertyFilterBuilder};
use crate::prelude::PropertyFilter;
use crate::python::filter::property_filter_builders::DynFilterOps;

pub fn py_into_millis(obj: &Bound<PyAny>) -> PyResult<i64> {
    obj.extract::<i64>()
}

#[pyclass(frozen, name = "NodeWindow", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeWindow(pub Arc<dyn DynNodePropertyFilterFactory>);

#[pymethods]
impl PyNodeWindow {
    fn property(&self, name: String) -> PyPropertyFilterOps {
        self.0.property(name)
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        self.0.metadata(name)
    }
}

pub(crate) trait DynNodePropertyFilterFactory: Send + Sync {
    fn property(&self, name: String) -> PyPropertyFilterOps;
    fn metadata(&self, name: String) -> PyPropertyFilterOps;
}

impl<M> DynNodePropertyFilterFactory for Windowed<M>
where
    M: Send + Sync + Clone + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter
{
    fn property(&self, name: String) -> PyPropertyFilterOps {
        let wpr: WindowedPropertyRef<M> = self.property(name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        let wpr: WindowedPropertyRef<M> = self.metadata(name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }
}

impl<M> DynNodePropertyFilterFactory for EndpointWrapper<Windowed<M>>
where
    M: Send + Sync + Clone + 'static,
    EndpointWrapper<WindowedPropertyRef<M>>: DynFilterOps
{
    fn property(&self, name: String) -> PyPropertyFilterOps {
        let wpr: EndpointWrapper<WindowedPropertyRef<M>> =
            EndpointWrapper::property(self, name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        let wpr: EndpointWrapper<WindowedPropertyRef<M>> =
            EndpointWrapper::metadata(self, name);
        PyPropertyFilterOps::from_arc(Arc::new(wpr))
    }
}

impl<M> DynNodePropertyFilterFactory for M
where
    M: PropertyFilterFactory<M> + Send + Sync + Clone + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn property(&self, name: String) -> PyPropertyFilterOps {
        let builder: PropertyFilterBuilder<M> = <M as PropertyFilterFactory<M>>::property(name);
        PyPropertyFilterOps::from_arc(Arc::new(builder))
    }

    fn metadata(&self, name: String) -> PyPropertyFilterOps {
        let builder: MetadataFilterBuilder<M> = <M as PropertyFilterFactory<M>>::metadata(name);
        PyPropertyFilterOps::from_arc(Arc::new(builder))
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
