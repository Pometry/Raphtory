use crate::{
    db::{
        api::state::ops::{GraphView, HistoryOp, Map, NodeOp},
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter, ComposableFilter, CompositeExplodedEdgeFilter,
                CompositeNodeFilter, TryAsCompositeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsActiveNode;

impl fmt::Display for IsActiveNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_ACTIVE_NODE")
    }
}

impl CreateFilter for IsActiveNode {
    type EntityFiltered<'graph, G, F>
        = NodeFilteredGraph<G, Self::NodeFilter<'graph, G, F>>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>
        = Map<HistoryOp<'graph, F>, bool>
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
        let op = self.create_node_filter(graph.clone(), filtered)?;
        Ok(NodeFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        let op = HistoryOp::new(filtered).map(|h| !h.is_empty());
        Ok(op)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl ComposableFilter for IsActiveNode {}

impl TryAsCompositeFilter for IsActiveNode {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::IsActiveNode(IsActiveNode))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}
