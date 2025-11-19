use crate::{
    db::api::{
        properties::internal::InheritPropertiesOps,
        state::ops::NodeFilterOp,
        view::internal::{
            Immutable, InheritAllEdgeFilterOps, InheritEdgeHistoryFilter, InheritLayerOps,
            InheritListOps, InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
            InheritTimeSemantics, InternalNodeFilterOps, Static,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
};

#[derive(Debug, Clone)]
pub struct NodeFilteredGraph<G, F> {
    graph: G,
    filter: F,
}

impl<G, F> NodeFilteredGraph<G, F> {
    pub(crate) fn new(graph: G, filter: F) -> Self {
        Self { graph, filter }
    }
}

impl<G, F> Base for NodeFilteredGraph<G, F> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, F> Static for NodeFilteredGraph<G, F> {}
impl<G, F> Immutable for NodeFilteredGraph<G, F> {}

impl<'graph, G: GraphViewOps<'graph>, F> InheritCoreGraphOps for NodeFilteredGraph<G, F> {}
impl<'graph, G: GraphViewOps<'graph>, F> InheritStorageOps for NodeFilteredGraph<G, F> {}
impl<'graph, G: GraphViewOps<'graph>, F> InheritLayerOps for NodeFilteredGraph<G, F> {}
impl<'graph, G: GraphViewOps<'graph>, F> InheritListOps for NodeFilteredGraph<G, F> {}
impl<'graph, G: GraphViewOps<'graph>, F> InheritMaterialize for NodeFilteredGraph<G, F> {}
impl<'graph, G: GraphViewOps<'graph>, F> InheritAllEdgeFilterOps for NodeFilteredGraph<G, F> {}
impl<'graph, G: GraphViewOps<'graph>, F> InheritPropertiesOps for NodeFilteredGraph<G, F> {}
impl<'graph, G: GraphViewOps<'graph>, F> InheritTimeSemantics for NodeFilteredGraph<G, F> {}
impl<'graph, G: GraphViewOps<'graph>, F> InheritNodeHistoryFilter for NodeFilteredGraph<G, F> {}
impl<'graph, G: GraphViewOps<'graph>, F> InheritEdgeHistoryFilter for NodeFilteredGraph<G, F> {}

impl<'graph, G: GraphViewOps<'graph>, F: NodeFilterOp> InternalNodeFilterOps
    for NodeFilteredGraph<G, F>
{
    fn internal_nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids)
            && self.filter.apply(self.graph.core_graph(), node.vid())
    }
}
