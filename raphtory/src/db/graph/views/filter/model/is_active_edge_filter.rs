use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::{
            filter::{
                edge_filtered_graph::EdgeFilteredGraph,
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
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsActiveEdge;

impl fmt::Display for IsActiveEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_ACTIVE_EDGE")
    }
}

impl CreateFilter for IsActiveEdge {
    type EntityFiltered<'graph, G, F>
        = EdgeFilteredGraph<G, IsActiveGraph<F>>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>
        = NodeExistsOp<IsActiveGraph<F>>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        Ok(EdgeFilteredGraph::new(graph, IsActiveGraph::new(filtered)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Ok(NodeExistsOp::new(IsActiveGraph::new(filtered)))
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
        Ok(CompositeEdgeFilter::IsActiveEdge(IsActiveEdge))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::IsActiveEdge(IsActiveEdge))
    }
}
