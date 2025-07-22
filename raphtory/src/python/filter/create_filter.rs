use crate::{
    db::{
        api::view::BoxableGraphView,
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::{ops::Deref, sync::Arc};
use crate::db::graph::views::filter::internal::CreateFilter;
use crate::db::graph::views::filter::model::{TryAsEdgeFilter, TryAsNodeFilter};

pub trait DynInternalFilterOps: Send + Sync + TryAsNodeFilter + TryAsEdgeFilter {
    fn create_dyn_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T> DynInternalFilterOps for T
where
    T: CreateFilter + TryAsNodeFilter + TryAsEdgeFilter + Clone + Send + Sync + 'static,
{
    fn create_dyn_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_filter(graph)?))
    }
}

impl<T: DynInternalFilterOps + ?Sized + 'static> CreateFilter for Arc<T> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
    = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        self.deref().create_dyn_filter(Arc::new(graph))
    }
}
