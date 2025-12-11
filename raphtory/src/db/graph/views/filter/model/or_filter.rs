use crate::{
    db::{
        api::{
            state::ops::{filter::OrOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                exploded_edge_filter::CompositeExplodedEdgeFilter,
                node_filter::CompositeNodeFilter, ComposableFilter, TryAsCompositeFilter,
            },
            or_filtered_graph::OrFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
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
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = OrFilteredGraph<G, L::EntityFiltered<'graph, G>, R::EntityFiltered<'graph, G>>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph>
        = OrOp<L::NodeFilter<'graph, G>, R::NodeFilter<'graph, G>>
    where
        Self: 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let left = self.left.create_filter(graph.clone())?;
        let right = self.right.create_filter(graph.clone())?;
        Ok(OrFilteredGraph { graph, left, right })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let left = self.left.create_node_filter(graph.clone())?;
        let right = self.right.create_node_filter(graph.clone())?;
        Ok(left.or(right))
    }
}

impl<L: TryAsCompositeFilter, R: TryAsCompositeFilter> TryAsCompositeFilter for OrFilter<L, R> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Or(
            Box::new(self.left.try_as_composite_node_filter()?),
            Box::new(self.right.try_as_composite_node_filter()?),
        ))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Or(
            Box::new(self.left.try_as_composite_edge_filter()?),
            Box::new(self.right.try_as_composite_edge_filter()?),
        ))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::Or(
            Box::new(self.left.try_as_composite_exploded_edge_filter()?),
            Box::new(self.right.try_as_composite_exploded_edge_filter()?),
        ))
    }
}
