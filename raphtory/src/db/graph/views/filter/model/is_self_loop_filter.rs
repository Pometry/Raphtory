use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::{
            filter::{
                model::{
                    edge_expr::{ops::IsSelfLoopEdgePropOp, EdgeOp},
                    edge_filter::EdgeFilter,
                    node_expr::{CreateOp, EntityExpr},
                    ComposableFilter, CreateView,
                },
                CreateFilter,
            },
            is_self_loop_graph::IsSelfLoopGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::{fmt, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsSelfLoopEdge<E> {
    pub(crate) view_expr: E,
}

impl<E> IsSelfLoopEdge<E> {
    pub fn new(view_expr: E) -> Self {
        Self { view_expr }
    }
}

impl<E> fmt::Display for IsSelfLoopEdge<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_SELF_LOOP_EDGE")
    }
}

impl<E: Clone + Send + Sync + 'static> EntityExpr for IsSelfLoopEdge<E> {
    type Marker = EdgeFilter;

    fn entity(&self) -> Self::Marker {
        EdgeFilter
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl<E: CreateView + Clone> CreateOp for IsSelfLoopEdge<E> {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let view = self.view_expr.create_view(graph)?;
        Ok(Arc::new(IsSelfLoopEdgePropOp { graph: view }))
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
