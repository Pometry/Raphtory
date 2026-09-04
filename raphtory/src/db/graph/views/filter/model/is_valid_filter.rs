use crate::{
    db::{
        api::state::ops::{filter::NodeExistsOp, GraphView},
        graph::views::{
            filter::{
                edge_filtered_graph::EdgeFilteredGraph, model::ComposableFilter, CreateFilter,
            },
            valid_graph::ValidGraph,
        },
    },
    errors::GraphError,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsValidEdge;

impl fmt::Display for IsValidEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IS_VALID_EDGE")
    }
}

impl CreateFilter for IsValidEdge {
    type EntityFiltered<'graph, G, F>
        = EdgeFilteredGraph<G, ValidGraph<F>>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>
        = NodeExistsOp<ValidGraph<F>>
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
        Ok(EdgeFilteredGraph::new(graph, ValidGraph::new(filtered)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Ok(NodeExistsOp::new(ValidGraph::new(filtered)))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl ComposableFilter for IsValidEdge {}

// ── expr layer: the predicate as a boolean expression over the eval view ──

use crate::db::graph::views::filter::model::{
    edge_expr::{ops::IsValidEdgePropOp, EdgeOp},
    edge_filter::EdgeFilter as EdgeFilterMarker,
    node_expr::{CreateOp, EntityExpr},
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;

impl EntityExpr for IsValidEdge {
    type Marker = EdgeFilterMarker;

    fn entity(&self) -> EdgeFilterMarker {
        EdgeFilterMarker
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl CreateOp for IsValidEdge {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, crate::errors::GraphError> {
        Ok(Arc::new(IsValidEdgePropOp { graph }))
    }
}
