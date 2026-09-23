use crate::{
    db::{
        api::{
            state::ops::{filter::AndOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::{
            and_filtered_graph::AndFilteredGraph,
            model::{edge_expr::ops::AndEdgeOp, ComposableFilter},
            CreateFilter,
        },
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AndFilter<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: Display, R: Display> Display for AndFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} AND {})", self.left, self.right)
    }
}

impl<L, R> ComposableFilter for AndFilter<L, R> {}

impl<L: CreateFilter, R: CreateFilter> CreateFilter for AndFilter<L, R> {
    type FilteredGraph<'graph, G>
        = AndFilteredGraph<G, L::FilteredGraph<'graph, G>, R::FilteredGraph<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = AndOp<L::NodeFilter<'graph, G>, R::NodeFilter<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = AndEdgeOp<L::EdgeFilter<'graph, G>, R::EdgeFilter<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        let left = self.left.create_graph_filter(graph.clone())?;
        let right = self.right.create_graph_filter(graph.clone())?;
        Ok(AndFilteredGraph::new(graph, left, right))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let left = self.left.create_node_filter(graph.clone())?;
        let right = self.right.create_node_filter(graph)?;
        Ok(left.and(right))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        let left = self.left.create_edge_filter(graph.clone())?;
        let right = self.right.create_edge_filter(graph)?;
        Ok(AndEdgeOp { left, right })
    }
}
