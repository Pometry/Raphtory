use crate::{
    db::{
        api::{
            state::ops::{filter::AndOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::{
            and_filtered_graph::AndFilteredGraph,
            model::{
                edge_filter::CompositeEdgeFilter,
                exploded_edge_filter::CompositeExplodedEdgeFilter,
                node_filter::CompositeNodeFilter, ComposableFilter, TryAsCompositeFilter,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_storage::layer_ops::InternalLayerOps;
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
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = AndFilteredGraph<G, L::EntityFiltered<'graph, G>, R::EntityFiltered<'graph, G>>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph>
        = AndOp<L::NodeFilter<'graph, G>, R::NodeFilter<'graph, G>>
    where
        Self: 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let left = self.left.create_filter(graph.clone())?;
        let right = self.right.create_filter(graph.clone())?;
        let layer_ids = left.layer_ids().intersect(right.layer_ids());
        Ok(AndFilteredGraph {
            graph,
            left,
            right,
            layer_ids,
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let left = self.left.create_node_filter(graph.clone())?;
        let right = self.right.create_node_filter(graph.clone())?;
        Ok(left.and(right))
    }
}

impl<L: TryAsCompositeFilter, R: TryAsCompositeFilter> TryAsCompositeFilter for AndFilter<L, R> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::And(
            Box::new(self.left.try_as_composite_node_filter()?),
            Box::new(self.right.try_as_composite_node_filter()?),
        ))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::And(
            Box::new(self.left.try_as_composite_edge_filter()?),
            Box::new(self.right.try_as_composite_edge_filter()?),
        ))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::And(
            Box::new(self.left.try_as_composite_exploded_edge_filter()?),
            Box::new(self.right.try_as_composite_exploded_edge_filter()?),
        ))
    }
}
