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
            valid_graph::ValidGraph,
        },
    },
    errors::GraphError,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsValidEdge;

impl fmt::Display for IsValidEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_VALID_EDGE")
    }
}

impl CreateFilter for IsValidEdge {
    type EntityFiltered<'graph, G, F>
        = EdgeFilteredGraph<G, ValidGraph<F>>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>
        = NodeExistsOp<ValidGraph<F>>
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
        Ok(EdgeFilteredGraph::new(graph, ValidGraph::new(filtered)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Ok(NodeExistsOp::new(ValidGraph::new(filtered)))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl ComposableFilter for IsValidEdge {}

impl TryAsCompositeFilter for IsValidEdge {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::IsValidEdge(IsValidEdge))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::IsValidEdge(IsValidEdge))
    }
}
