use crate::{
    db::{
        api::{
            state::ops::{filter::NotOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::{
            model::{edge_expr::ops::NotEdgeOp, ComposableFilter},
            not_filtered_graph::NotFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotFilter<T>(pub T);

impl<T: Display> Display for NotFilter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NOT({})", self.0)
    }
}

impl<T> ComposableFilter for NotFilter<T> {}

impl<T: CreateFilter> CreateFilter for NotFilter<T> {
    type FilteredGraph<'graph, G>
        = NotFilteredGraph<G, T::FilteredGraph<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = NotOp<T::NodeFilter<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = NotEdgeOp<T::EdgeFilter<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        let filter = self.0.create_graph_filter(graph.clone())?;
        Ok(NotFilteredGraph { graph, filter })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(self.0.create_node_filter(graph)?.not())
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        Ok(NotEdgeOp(self.0.create_edge_filter(graph)?))
    }
}
