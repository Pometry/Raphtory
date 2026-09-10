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
                node_filter::CompositeNodeFilter, ComposableFilter, FilterTree,
                TryAsCompositeFilter,
            },
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
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = NotFilteredGraph<G, T::EntityFiltered<'graph, F, T::FilteredGraph<'graph, F>>>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = NotOp<T::NodeFilter<'graph, F, T::FilteredGraph<'graph, F>>>
    where
        Self: 'graph;

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
        let f = self.0.filter_graph_view(filtered.clone())?;
        let filter = self.0.create_filter(filtered, f)?;
        Ok(NotFilteredGraph { graph, filter })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        let f = self.0.filter_graph_view(filtered.clone())?;
        Ok(self.0.create_node_filter(filtered, f)?.not())
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
    fn try_as_filter_tree(&self) -> Result<FilterTree, GraphError> {
        // Same-kind combinations keep their composite form; mixed-kind trees
        // export structurally — the case the composite exports cannot
        // represent.
        if let Ok(f) = self.try_as_composite_node_filter() {
            return Ok(FilterTree::Node(f));
        }
        if let Ok(f) = self.try_as_composite_edge_filter() {
            return Ok(FilterTree::Edge(f));
        }
        if let Ok(f) = self.try_as_composite_exploded_edge_filter() {
            return Ok(FilterTree::ExplodedEdge(f));
        }
        Ok(FilterTree::Not(Box::new(self.0.try_as_filter_tree()?)))
    }

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
