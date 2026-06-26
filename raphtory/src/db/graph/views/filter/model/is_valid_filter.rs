use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::{
            filter::{model::ComposableFilter, CreateFilter},
            valid_graph::ValidGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::fmt;
use crate::db::graph::views::filter::model::CreateView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsValidEdge<E> {
    pub(crate) view_expr: E,
}

impl<E> fmt::Display for IsValidEdge<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_VALID_EDGE")
    }
}

impl<E: CreateView + 'static> CreateFilter for IsValidEdge<E> {
    type EntityFiltered<'graph, G>
        = ValidGraph<G>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = NodeExistsOp<ValidGraph<G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(ValidGraph::new(graph))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(ValidGraph::new(graph)))
    }
}

impl<E: CreateView> ComposableFilter for IsValidEdge<E> {}
