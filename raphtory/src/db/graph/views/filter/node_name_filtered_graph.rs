use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            state::NodeOp,
            view::internal::{
                GraphView, Immutable, InheritAllEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, InternalNodeFilterOps, Static,
            },
        },
        graph::views::filter::model::Filter,
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::entities::{LayerIds, VID},
    inherit::Base,
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{
        graph::GraphStorage,
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
};

#[derive(Debug, Clone)]
pub struct NodeNameFilteredGraph<G> {
    graph: G,
    filter: Filter,
}

impl<G> NodeNameFilteredGraph<G> {
    pub(crate) fn new(graph: G, filter: Filter) -> Self {
        Self { graph, filter }
    }
}

impl<G: GraphView> NodeOp for NodeNameFilteredGraph<G> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let layer_ids = self.graph.layer_ids();
        let nodes = storage.nodes();
        let node_ref = nodes.node(node);
        self.internal_filter_node(node_ref, layer_ids)
    }
}

impl<G> Base for NodeNameFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for NodeNameFilteredGraph<G> {}
impl<G> Immutable for NodeNameFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for NodeNameFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeNameFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeNameFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeNameFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeNameFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritAllEdgeFilterOps for NodeNameFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeNameFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeNameFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeNameFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeNameFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for NodeNameFilteredGraph<G> {
    fn internal_nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids)
            && self.filter.matches(Some(&node.id().to_str()))
    }
}
