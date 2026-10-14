use crate::{
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                DynGraphArc, Immutable, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps,
                InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps, InternalNodeFilterOps,
                Static,
            },
        },
        graph::views::filter::operand::{
            keeps_edge, keeps_edge_layer, keeps_exploded_edge, keeps_node, restricts_edge_layers,
            restricts_edges, restricts_exploded_edges, restricts_nodes,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerId, LayerIds, ELID},
        storage::timeindex::EventTime,
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef},
};
use std::sync::Arc;

/// The graph keeping what any operand keeps. A node or an edge is filtered
/// only when every operand restricts it. Its node and edge lists are the base
/// graph's, not the union of the operands', so they are never trusted.
#[derive(Clone)]
pub struct OrFilteredGraph<'graph, G> {
    pub(crate) graph: G,
    pub(crate) operands: Arc<[DynGraphArc<'graph>]>,
}

impl<'graph, G> OrFilteredGraph<'graph, G> {
    pub fn new(graph: G, operands: Arc<[DynGraphArc<'graph>]>) -> Self {
        Self { graph, operands }
    }
}

impl<G> Base for OrFilteredGraph<'_, G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for OrFilteredGraph<'_, G> {}
impl<G> Immutable for OrFilteredGraph<'_, G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for OrFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for OrFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for OrFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for OrFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for OrFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for OrFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodePropertySchemaOps for OrFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgePropertySchemaOps for OrFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for OrFilteredGraph<'graph, G> {}

impl<G> InternalNodeFilterOps for OrFilteredGraph<'_, G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.operands.iter().all(restricts_nodes)
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        self.operands
            .iter()
            .any(|operand| keeps_node(operand, node))
    }
}

/// A layer or an update of an edge belongs to the union through the operands that
/// keep the edge, so any operand restricting layers or updates can drop one
/// that another operand's edge would otherwise let through.
impl<G> InternalEdgeLayerFilterOps for OrFilteredGraph<'_, G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        self.operands.iter().any(restricts_edge_layers)
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.operands
            .iter()
            .any(|operand| keeps_edge_layer(operand, edge, layer))
    }
}

impl<G> InternalExplodedEdgeFilterOps for OrFilteredGraph<'_, G> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.operands.iter().any(restricts_exploded_edges)
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: EventTime,
        _layer_ids: &LayerIds,
    ) -> bool {
        self.operands
            .iter()
            .any(|operand| keeps_exploded_edge(operand, eid, t))
    }
}

impl<G> InternalEdgeFilterOps for OrFilteredGraph<'_, G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.operands.iter().all(restricts_edges)
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, _layer_ids: &LayerIds) -> bool {
        self.operands
            .iter()
            .any(|operand| keeps_edge(operand, edge))
    }
}
