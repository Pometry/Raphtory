use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::{
            filter::{
                model::{
                    edge_filter::CompositeEdgeFilter, ComposableFilter,
                    CompositeExplodedEdgeFilter, CompositeNodeFilter, TryAsCompositeFilter,
                },
                CreateFilter,
            },
            is_deleted_graph::IsDeletedGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsDeletedEdge;

impl fmt::Display for IsDeletedEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_DELETED_EDGE")
    }
}

impl CreateFilter for IsDeletedEdge {
    type EntityFiltered<'graph, G>
        = IsDeletedGraph<G>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = NodeExistsOp<IsDeletedGraph<G>>
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
        Ok(IsDeletedGraph::new(graph))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(IsDeletedGraph::new(graph)))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl ComposableFilter for IsDeletedEdge {}

impl TryAsCompositeFilter for IsDeletedEdge {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::IsDeletedEdge(IsDeletedEdge))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::IsDeletedEdge(IsDeletedEdge))
    }
}
