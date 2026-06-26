use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::{
            filter::{
                model::{ComposableFilter, CreateView},
                CreateFilter,
            },
            is_active_graph::IsActiveGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsActiveEdge<E> {
    pub(crate) view_expr: E,
}

impl<E> fmt::Display for IsActiveEdge<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_ACTIVE_EDGE")
    }
}

impl<E: CreateView + 'static> CreateFilter for IsActiveEdge<E> {
    type EntityFiltered<'graph, G>
        = IsActiveGraph<G>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = NodeExistsOp<IsActiveGraph<G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(IsActiveGraph::new(graph))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(IsActiveGraph::new(graph)))
    }
}

impl<E: CreateView> ComposableFilter for IsActiveEdge<E> {}
