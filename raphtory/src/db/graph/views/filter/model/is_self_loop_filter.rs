use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::{
            filter::{model::ComposableFilter, CreateFilter},
            is_self_loop_graph::IsSelfLoopGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::fmt;
use crate::db::graph::views::filter::model::CreateView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsSelfLoopEdge<E> {
    pub(crate) view_expr: E,
}

impl<E> fmt::Display for IsSelfLoopEdge<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_SELF_LOOP_EDGE")
    }
}

impl<E: CreateView + 'static> CreateFilter for IsSelfLoopEdge<E> {
    type EntityFiltered<'graph, G>
        = IsSelfLoopGraph<G>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = NodeExistsOp<IsSelfLoopGraph<G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(IsSelfLoopGraph::new(graph))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(IsSelfLoopGraph::new(graph)))
    }
}

impl<E: CreateView> ComposableFilter for IsSelfLoopEdge<E> {}
