use crate::{
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                DynGraphArc, EdgeList, Immutable, InheritMaterialize, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, InternalEdgeLayerFilterOps,
                InternalExplodedEdgeFilterOps, InternalLayerOps, InternalNodeFilterOps, ListOps,
                NodeList, Static,
            },
        },
        graph::views::filter::operand::{
            keeps_edge, keeps_edge_layer, keeps_exploded_edge, keeps_node, lists_trusted,
            restricts_edge_layers, restricts_edges, restricts_exploded_edges, restricts_nodes,
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
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef},
};
use std::sync::Arc;

/// The graph keeping what every operand keeps. A level is filtered when any
/// operand restricts it.
#[derive(Clone)]
pub struct AndFilteredGraph<'graph, G> {
    pub(crate) graph: G,
    pub(crate) operands: Arc<[DynGraphArc<'graph>]>,
    pub(crate) layer_ids: LayerIds,
}

impl<'graph, G: GraphViewOps<'graph>> AndFilteredGraph<'graph, G> {
    /// `operands` is never empty: an `and` of nothing is refused before this
    /// graph is built. The fallbacks below only keep the folds total.
    pub fn new(graph: G, operands: Arc<[DynGraphArc<'graph>]>) -> Self {
        let layer_ids = operands
            .iter()
            .map(|operand| operand.layer_ids().clone())
            .reduce(|layer_ids, operand| layer_ids.intersect(&operand))
            .unwrap_or_else(|| graph.layer_ids().clone());
        Self {
            graph,
            operands,
            layer_ids,
        }
    }
}

impl<G> Base for AndFilteredGraph<'_, G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for AndFilteredGraph<'_, G> {}
impl<G> Immutable for AndFilteredGraph<'_, G> {}

impl<G> InheritCoreGraphOps for AndFilteredGraph<'_, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for AndFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for AndFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for AndFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodePropertySchemaOps for AndFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgePropertySchemaOps for AndFilteredGraph<'graph, G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for AndFilteredGraph<'graph, G> {}

impl<G: InternalLayerOps> InternalLayerOps for AndFilteredGraph<'_, G> {
    fn layer_ids(&self) -> &LayerIds {
        &self.layer_ids
    }
}

impl<G: CoreGraphOps + ListOps> ListOps for AndFilteredGraph<'_, G> {
    fn node_list(&self) -> NodeList {
        self.operands
            .iter()
            .map(|operand| operand.node_list())
            .reduce(|nodes, operand| nodes.intersection(&operand, self.graph.core_graph()))
            .unwrap_or_else(|| self.graph.node_list())
    }

    fn edge_list(&self) -> EdgeList {
        self.operands
            .iter()
            .map(|operand| operand.edge_list())
            .reduce(|edges, operand| edges.intersection(&operand, self.graph.core_graph()))
            .unwrap_or_else(|| self.graph.edge_list())
    }
}

impl<G> InternalNodeFilterOps for AndFilteredGraph<'_, G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.operands.iter().any(restricts_nodes)
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        lists_trusted(&self.operands, |operand| {
            operand.internal_node_list_trusted()
        })
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        self.operands
            .iter()
            .all(|operand| keeps_node(operand, node))
    }
}

impl<G> InternalEdgeLayerFilterOps for AndFilteredGraph<'_, G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        self.operands.iter().any(restricts_edge_layers)
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        lists_trusted(&self.operands, |operand| {
            operand.internal_layer_filter_edge_list_trusted()
        })
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.operands
            .iter()
            .all(|operand| keeps_edge_layer(operand, edge, layer))
    }
}

impl<G> InternalExplodedEdgeFilterOps for AndFilteredGraph<'_, G> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.operands.iter().any(restricts_exploded_edges)
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        lists_trusted(&self.operands, |operand| {
            operand.internal_exploded_filter_edge_list_trusted()
        })
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: EventTime,
        _layer_ids: &LayerIds,
    ) -> bool {
        self.operands
            .iter()
            .all(|operand| keeps_exploded_edge(operand, eid, t))
    }
}

impl<G> InternalEdgeFilterOps for AndFilteredGraph<'_, G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.operands.iter().any(restricts_edges)
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        lists_trusted(&self.operands, |operand| {
            operand.internal_edge_list_trusted()
        })
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, _layer_ids: &LayerIds) -> bool {
        self.operands
            .iter()
            .all(|operand| keeps_edge(operand, edge))
    }
}
