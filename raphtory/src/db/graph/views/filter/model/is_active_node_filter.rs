use crate::{
    db::{
        api::state::ops::{GraphView, HistoryOp, Map, NodeOp},
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter, ComposableFilter, CompositeExplodedEdgeFilter,
                CompositeNodeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsActiveNode;

impl fmt::Display for IsActiveNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_ACTIVE_NODE")
    }
}

impl CreateFilter for IsActiveNode {
    type EntityFiltered<'graph, G, F>
        = NodeFilteredGraph<G, Self::NodeFilter<'graph, G, F>>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>
        = Map<HistoryOp<'graph, F>, bool>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

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
        let op = self.create_node_filter(graph.clone(), filtered)?;
        Ok(NodeFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        let op = HistoryOp::new(filtered).map(|h| !h.is_empty());
        Ok(op)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl ComposableFilter for IsActiveNode {}

// ── expr layer: the predicate as a boolean expression over the eval view ──

use crate::db::graph::views::filter::model::{
    node_expr::{CreateOp, EntityExpr},
    node_filter::NodeFilter as NodeFilterMarker,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;

impl EntityExpr for IsActiveNode {
    type Marker = NodeFilterMarker;

    fn entity(&self) -> NodeFilterMarker {
        NodeFilterMarker
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl CreateOp for IsActiveNode {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, crate::errors::GraphError> {
        Ok(Arc::new(
            HistoryOp::new(graph).map(|h| Some(Prop::Bool(!h.is_empty()))),
        ))
    }
}
