use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritAllEdgeFilterOps, InheritEdgeHistoryFilter, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalNodeFilterOps, Static,
            },
        },
        graph::views::filter::{internal::CreateNodeFilter, model::Filter, NodeNameFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
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

impl CreateNodeFilter for NodeNameFilter {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>> = NodeNameFilteredGraph<G>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        Ok(NodeNameFilteredGraph::new(graph, self.0))
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
    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        if self.graph.internal_filter_node(node, layer_ids) {
            self.filter.matches(Some(&node.id().to_str()))
        } else {
            false
        }
    }
}
