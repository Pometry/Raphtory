use crate::{
    db::{
        api::state::ops::{GraphView, HistoryOp, Map, NodeOp},
        graph::views::filter::{
            model::{
                node_expr::{CreateOp, EntityExpr},
                ComposableFilter, CreateView,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, NodeFilter},
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::{fmt, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsActiveNode<E> {
    pub(crate) view_expr: E,
}

impl<E> fmt::Display for IsActiveNode<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_ACTIVE_NODE")
    }
}

impl<E: Clone + Send + Sync + 'static> EntityExpr for IsActiveNode<E> {
    type Marker = NodeFilter;

    fn entity(&self) -> Self::Marker {
        NodeFilter
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl<E: CreateView + Clone> CreateOp for IsActiveNode<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(
            HistoryOp::new(self.view_expr.create_view(graph)?)
                .map(|h| Some(Prop::Bool(!h.is_empty()))),
        ))
    }
}

impl<E: CreateView + 'static> CreateFilter for IsActiveNode<E> {
    type EntityFiltered<'graph, G>
        = NodeFilteredGraph<G, Self::NodeFilter<'graph, G>>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = Map<HistoryOp<'graph, E::View<'graph, G>>, bool>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let op = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(HistoryOp::new(self.view_expr.create_view(graph)?).map(|h| !h.is_empty()))
    }
}

impl<E: CreateView> ComposableFilter for IsActiveNode<E> {}
