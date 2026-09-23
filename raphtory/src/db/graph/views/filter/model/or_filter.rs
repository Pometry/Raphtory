use crate::{
    db::{
        api::{
            state::ops::{filter::OrOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::{
            model::{edge_expr::ops::OrEdgeOp, ComposableFilter},
            or_filtered_graph::OrFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrFilter<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: Display, R: Display> Display for OrFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} OR {})", self.left, self.right)
    }
}

impl<L, R> ComposableFilter for OrFilter<L, R> {}

impl<L: CreateFilter, R: CreateFilter> CreateFilter for OrFilter<L, R> {
    type FilteredGraph<'graph, G>
        = OrFilteredGraph<G, L::FilteredGraph<'graph, G>, R::FilteredGraph<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = OrOp<L::NodeFilter<'graph, G>, R::NodeFilter<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = OrEdgeOp<L::EdgeFilter<'graph, G>, R::EdgeFilter<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        let left = self.left.create_graph_filter(graph.clone())?;
        let right = self.right.create_graph_filter(graph.clone())?;
        Ok(OrFilteredGraph { graph, left, right })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let left = self.left.create_node_filter(graph.clone())?;
        let right = self.right.create_node_filter(graph)?;
        Ok(left.or(right))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        let left = self.left.create_edge_filter(graph.clone())?;
        let right = self.right.create_edge_filter(graph)?;
        Ok(OrEdgeOp { left, right })
    }
}
