//! One graph carrying what a lowered composite filter says about nodes and
//! about edges.
//!
//! `graph.filter(expr)` has to answer for nodes *and* edges, and a composite
//! decides each with its own boolean: nodes through a node op, edges through
//! the per-edge op in [`super::edge_op`]. Composing a wrapper graph per operand
//! instead is what loses a view operand's restriction, and cannot express `or`
//! or `not` at all.
//!
//! Either op may be absent. An expression that speaks only about nodes installs
//! no edge op, so this graph is indistinguishable from a single node filter:
//! edges follow from their endpoints through the endpoint check every filtered
//! graph performs, and walking from a node does not test the node being stood
//! on. One that speaks only about edges installs no node op and leaves nodes
//! alone.
//!
//! The endpoint check is deliberately left on even when an edge op is present.
//! It is what makes the result a graph rather than two unrelated sets: an edge
//! is kept only if both its endpoints are. Skipping it — on the grounds that the
//! edge op already accounts for node-kind operands — admits edges whose
//! endpoints the graph says do not exist.

use crate::db::{
    api::{
        properties::internal::{
            InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
        },
        state::ops::{node::NodeOp, GraphView},
        view::internal::{
            Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
            InheritExplodedEdgeFilterOps, InheritListOps, InheritMaterialize,
            InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics,
            InternalEdgeFilterOps, InternalNodeFilterOps, Static,
        },
    },
    graph::views::filter::edge_op::EdgeFilterOp,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{
        edges::edge_ref::EdgeEntryRef,
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
    layer_ops::InheritLayerOps,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct EntityOpFilteredGraph<'graph, G> {
    graph: G,
    /// `None` when the expression says nothing about nodes.
    node_op: Option<Arc<dyn NodeOp<Output = bool> + 'graph>>,
    /// `None` when the expression says nothing about edges beyond what its
    /// node op implies through their endpoints.
    edge_op: Option<Arc<dyn EdgeFilterOp + 'graph>>,
}

impl<'graph, G: GraphView> EntityOpFilteredGraph<'graph, G> {
    pub fn new(
        graph: G,
        node_op: Option<Arc<dyn NodeOp<Output = bool> + 'graph>>,
        edge_op: Option<Arc<dyn EdgeFilterOp + 'graph>>,
    ) -> Self {
        Self {
            graph,
            node_op,
            edge_op,
        }
    }
}

impl<'graph, G> Base for EntityOpFilteredGraph<'graph, G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G> Static for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G> Immutable for EntityOpFilteredGraph<'graph, G> {}

impl<'graph, G: GraphView> InheritCoreGraphOps for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritStorageOps for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritLayerOps for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritListOps for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritMaterialize for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritPropertiesOps for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritNodePropertySchemaOps for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritEdgePropertySchemaOps for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritTimeSemantics for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritNodeHistoryFilter for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritEdgeHistoryFilter for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritEdgeLayerFilterOps for EntityOpFilteredGraph<'graph, G> {}
impl<'graph, G: GraphView> InheritExplodedEdgeFilterOps for EntityOpFilteredGraph<'graph, G> {}

impl<'graph, G: GraphView> InternalNodeFilterOps for EntityOpFilteredGraph<'graph, G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.node_op.is_some() || self.graph.internal_nodes_filtered()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids)
            && self
                .node_op
                .as_ref()
                .is_none_or(|op| op.apply(self.graph.core_graph(), node.vid()))
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.node_op.is_none() && self.graph.internal_node_list_trusted()
    }
}

impl<'graph, G: GraphView> InternalEdgeFilterOps for EntityOpFilteredGraph<'graph, G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.edge_op.is_some() || self.graph.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.edge_op.is_none() && self.graph.internal_edge_list_trusted()
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_edge(edge, layer_ids)
            && self.edge_op.as_ref().is_none_or(|op| op.test(edge))
    }
}
