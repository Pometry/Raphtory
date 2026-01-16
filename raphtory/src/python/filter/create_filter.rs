use crate::{
    db::{
        api::{
            state::ops::NodeOp,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{
                windowed_filter::Windowed, CombinedFilter, DynPropertyFilterFactory,
                InternalViewWrapOps, TryAsCompositeFilter, ViewWrapOps,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, NodeFilter, PropertyFilterFactory},
    python::{
        filter::property_filter_builders::{PyPropertyExprBuilder, PyPropertyFilterBuilder},
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::storage::timeindex::EventTime;
use std::{ops::Deref, sync::Arc};

pub trait DynCreateFilter: TryAsCompositeFilter + Send + Sync + 'static {
    fn create_dyn_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;

    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError>;
}

impl<T> DynCreateFilter for T
where
    T: CombinedFilter,
{
    fn create_dyn_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_filter(graph)?))
    }

    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_node_filter(graph)?))
    }
}

impl<T: DynCreateFilter + ?Sized + 'static> CreateFilter for Arc<T> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph> = Arc<dyn NodeOp<Output = bool> + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        self.deref().create_dyn_filter(Arc::new(graph))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        self.deref().create_dyn_node_filter(Arc::new(graph))
    }
}

// pub trait DynViewWrap: Send + Sync {
//     fn property(&self, name: String) -> PyPropertyFilterBuilder;
//
//     fn metadata(&self, name: String) -> PyPropertyExprBuilder;
// }
//
// impl<T> DynViewWrap for T
// where
//     T: ViewWrapOps,
// {
//     fn property(&self, name: String) -> PyPropertyFilterBuilder {
//         PyPropertyFilterBuilder::from_arc(Arc::new(PropertyFilterFactory::property(
//             &NodeFilter,
//             name,
//         )))
//     }
//
//     fn metadata(&self, name: String) -> PyPropertyExprBuilder {
//         PyPropertyExprBuilder::from_arc(Arc::new(PropertyFilterFactory::metadata(
//             &NodeFilter,
//             name,
//         )))
//     }
// }

// #[pyclass(name = "ViewFilterBuilder", module = "raphtory.filter", subclass)]
// pub struct PyViewFilterBuilder(pub Arc<dyn DynPropertyFilterFactory>);
//
// #[pymethods]
// impl PyViewFilterBuilder {
//     fn property(&self, name: String) -> PyPropertyFilterBuilder {
//         self.0.property(name)
//     }
//
//     fn metadata(&self, name: String) -> PyPropertyExprBuilder {
//         self.0.metadata(name)
//     }
//
//     fn window(&self, start: EventTime, end: EventTime) -> PyViewFilterBuilder {
//         PyViewFilterBuilder(Arc::new(self.0.clone().window(start, end)))
//     }
//
//     fn at(&self, time: EventTime) -> PyViewFilterBuilder {
//         PyViewFilterBuilder(Arc::new(self.0.clone().at(time)))
//     }
//
//     fn after(&self, time: EventTime) -> PyViewFilterBuilder {
//         PyViewFilterBuilder(Arc::new(self.0.clone().after(time)))
//     }
//
//     fn before(&self, time: EventTime) -> PyViewFilterBuilder {
//         PyViewFilterBuilder(Arc::new(self.0.clone().before(time)))
//     }
//
//     fn latest(&self) -> PyViewFilterBuilder {
//         PyViewFilterBuilder(Arc::new(self.0.clone().latest()))
//     }
//
//     fn snapshot_at(&self, time: EventTime) -> PyViewFilterBuilder {
//         PyViewFilterBuilder(Arc::new(self.0.clone().snapshot_at(time)))
//     }
//
//     fn snapshot_latest(&self) -> PyViewFilterBuilder {
//         PyViewFilterBuilder(Arc::new(self.0.clone().snapshot_latest()))
//     }
//
//     fn layer(&self, layer: String) -> PyViewFilterBuilder {
//         PyViewFilterBuilder(Arc::new(self.0.clone().layer(layer)))
//     }
//
//     fn layers(&self, layers: FromIterable<String>) -> PyViewFilterBuilder {
//         PyViewFilterBuilder(Arc::new(self.0.clone().layer(layers)))
//     }
// }
