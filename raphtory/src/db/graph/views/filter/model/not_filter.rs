use crate::{
    db::{
        api::{
            state::ops::{filter::NotOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                exploded_edge_filter::CompositeExplodedEdgeFilter,
                node_filter::CompositeNodeFilter, ComposableFilter, TryAsCompositeFilter,
            },
            not_filtered_graph::NotFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
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
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = NotFilteredGraph<G, T::EntityFiltered<'graph, T::FilteredGraph<'graph, G>>>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph>
        = NotOp<T::NodeFilter<'graph, T::FilteredGraph<'graph, G>>>
    where
        Self: 'graph;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let f = self.0.filter_graph_view(graph.clone())?;
        let filter = self.0.create_filter(f)?;
        Ok(NotFilteredGraph { graph, filter })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        Self: 'graph,
    {
        let f = self.0.filter_graph_view(graph.clone())?;
        Ok(self.0.create_node_filter(f)?.not())
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

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for NotFilter<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Not(Box::new(
            self.0.try_as_composite_node_filter()?,
        )))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Not(Box::new(
            self.0.try_as_composite_edge_filter()?,
        )))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::Not(Box::new(
            self.0.try_as_composite_exploded_edge_filter()?,
        )))
    }
}
