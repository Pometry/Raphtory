use crate::{
    db::{
        api::{
            state::ops::NodeOp,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{model::TryAsCompositeFilter, CreateFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::{ops::Deref, sync::Arc};

pub trait DynInternalFilterOps: Send + Sync + TryAsCompositeFilter {
    fn create_dyn_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;

    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError>;
}

impl<T> DynInternalFilterOps for T
where
    T: CreateFilter + TryAsCompositeFilter + Clone + Send + Sync + 'static,
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

impl<T: DynInternalFilterOps + ?Sized + 'static> CreateFilter for Arc<T> {
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
