use crate::{
    db::{
        api::{
            state::ops::{filter::AndOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::{
            and_filtered_graph::AndFilteredGraph, model::ComposableFilter, CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
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
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = AndFilteredGraph<
        G,
        L::EntityFiltered<'graph, G, L::FilteredGraph<'graph, F>>,
        R::EntityFiltered<'graph, G, R::FilteredGraph<'graph, F>>,
    >
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = AndOp<
        L::NodeFilter<'graph, G, L::FilteredGraph<'graph, F>>,
        R::NodeFilter<'graph, G, R::FilteredGraph<'graph, F>>,
    >
    where
        Self: 'graph;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let l = self.left.filter_graph_view(filtered.clone())?;
        let r = self.right.filter_graph_view(filtered)?;
        let left = self.left.create_filter(graph.clone(), l)?;
        let right = self.right.create_filter(graph.clone(), r)?;
        Ok(AndFilteredGraph::new(graph, left, right))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        let l = self.left.filter_graph_view(filtered.clone())?;
        let r = self.right.filter_graph_view(filtered)?;
        let left = self.left.create_node_filter(graph.clone(), l)?;
        let right = self.right.create_node_filter(graph, r)?;
        Ok(left.and(right))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError>
    where
        Self: 'graph,
    {
        Ok(graph)
    }
}
