use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::{
            filter::{
                model::{ComposableFilter, CreateView},
                CreateFilter,
            },
            is_deleted_graph::IsDeletedGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsDeletedEdge<E> {
    pub(crate) view_expr: E,
}

impl<E> fmt::Display for IsDeletedEdge<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_DELETED_EDGE")
    }
}

impl<E: CreateView + 'static> CreateFilter for IsDeletedEdge<E> {
    type EntityFiltered<'graph, G>
        = IsDeletedGraph<G>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = NodeExistsOp<IsDeletedGraph<G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(IsDeletedGraph::new(graph))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(IsDeletedGraph::new(graph)))
    }
}

impl<E: CreateView> ComposableFilter for IsDeletedEdge<E> {}
