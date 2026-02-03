use crate::{
    db::{
        api::{
            state::ops::{filter::NodeExistsOp, GraphView},
            view::BoxableGraphView,
        },
        graph::views::{
            filter::{
                model::{
                    edge_filter::CompositeEdgeFilter, ComposableFilter,
                    CompositeExplodedEdgeFilter, CompositeNodeFilter, TryAsCompositeFilter,
                },
                CreateFilter,
            },
            is_active_graph::IsActiveGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::{fmt, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsActiveEdge;

impl fmt::Display for IsActiveEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_ACTIVE_EDGE")
    }
}

impl CreateFilter for IsActiveEdge {
    type EntityFiltered<'graph, G>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = NodeExistsOp<IsActiveGraph<G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(Arc::new(IsActiveGraph::new(graph)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(IsActiveGraph::new(graph)))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl ComposableFilter for IsActiveEdge {}

impl TryAsCompositeFilter for IsActiveEdge {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::IsActiveEdge(Box::new(IsActiveEdge)))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::IsActiveEdge(Box::new(
            IsActiveEdge,
        )))
    }
}
