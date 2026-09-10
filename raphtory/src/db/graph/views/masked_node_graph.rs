use crate::db::api::{
    properties::internal::{
        InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
    },
    state::ops::GraphView,
    view::internal::{
        Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
        InheritExplodedEdgeFilterOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
        InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps, InternalNodeFilterOps,
        Static,
    },
};
use raphtory_api::{core::entities::LayerIds, inherit::Base, roaring::RoaringNodeMap};
use raphtory_core::entities::nodes::node_ref::AsNodeRef;
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{
        edges::edge_storage_ops::EdgeStorageOps,
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
    layer_ops::InheritLayerOps,
};
use std::{fmt::Debug, sync::Arc};
use storage::EdgeEntryRef;

#[derive(Clone, Debug)]
pub struct MaskedNodeGraph<G> {
    graph: G,
    mask: Arc<RoaringNodeMap>,
}

impl<G> Static for MaskedNodeGraph<G> {}

impl<G> Base for MaskedNodeGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: GraphView> Immutable for MaskedNodeGraph<G> {}

impl<G: GraphView> InheritCoreGraphOps for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritStorageOps for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritTimeSemantics for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritPropertiesOps for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritNodePropertySchemaOps for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritEdgePropertySchemaOps for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritMaterialize for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritLayerOps for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritNodeHistoryFilter for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritEdgeHistoryFilter for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritListOps for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritExplodedEdgeFilterOps for MaskedNodeGraph<G> {}
impl<G: GraphView> InheritEdgeLayerFilterOps for MaskedNodeGraph<G> {}

impl<G: GraphView> MaskedNodeGraph<G> {
    pub fn new(graph: G, nodes: impl IntoIterator<Item = impl AsNodeRef>) -> Self {
        // we keep all nodes that are in the mask, even if they are already filtered out of the view
        // as checking the mask should be much faster
        let mask = nodes
            .into_iter()
            .filter_map(|n| graph.internalise_node(n.as_node_ref()))
            .collect();
        MaskedNodeGraph {
            graph,
            mask: Arc::new(mask),
        }
    }
}

impl<G: GraphView> InternalEdgeFilterOps for MaskedNodeGraph<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    fn node_filter_includes_edge_filter(&self) -> bool {
        // removing one edge endpoint might lead to removing the other from the graph as well
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        !self.mask.contains(edge.src())
            && !self.mask.contains(edge.dst())
            && self.graph.internal_filter_edge(edge, layer_ids)
    }
}

impl<G: GraphView> InternalNodeFilterOps for MaskedNodeGraph<G> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        true
    }

    #[inline]
    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_filter_includes_node_filter()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        !self.mask.contains(node.vid()) && self.graph.internal_filter_node(node, layer_ids)
    }
}
