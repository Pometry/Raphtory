use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            internal::{CreateEdgeFilter, CreateExplodedEdgeFilter, CreateNodeFilter},
            model::{AsEdgeFilter, AsNodeFilter},
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::{ops::Deref, sync::Arc};

pub trait DynCreateNodeFilterOps: AsNodeFilter {
    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T: CreateNodeFilter + AsNodeFilter + Clone + 'static> DynCreateNodeFilterOps for T {
    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_node_filter(graph)?))
    }
}

pub trait DynCreateEdgeFilterOps: AsEdgeFilter {
    fn create_dyn_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T: CreateEdgeFilter + AsEdgeFilter + Clone + 'static> DynCreateEdgeFilterOps for T {
    fn create_dyn_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_edge_filter(graph)?))
    }
}

pub trait DynCreateExplodedEdgeFilter {
    fn create_dyn_exploded_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T: CreateExplodedEdgeFilter + Clone + 'static> DynCreateExplodedEdgeFilter for T {
    fn create_dyn_exploded_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_exploded_edge_filter(graph)?))
    }
}

impl<T: DynCreateNodeFilterOps + ?Sized + 'static> CreateNodeFilter for Arc<T> {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        self.deref().create_dyn_node_filter(Arc::new(graph))
    }
}

impl<T: DynCreateEdgeFilterOps + ?Sized + 'static> CreateEdgeFilter for Arc<T> {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        self.deref().create_dyn_edge_filter(Arc::new(graph))
    }
}

impl<T: DynCreateExplodedEdgeFilter + ?Sized + 'static> CreateExplodedEdgeFilter for Arc<T> {
    type ExplodedEdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError> {
        self.deref()
            .create_dyn_exploded_edge_filter(Arc::new(graph))
    }
}
