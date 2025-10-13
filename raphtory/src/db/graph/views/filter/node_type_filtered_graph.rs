use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritAllEdgeFilterOps, InheritEdgeHistoryFilter, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalNodeFilterOps, Static,
            },
        },
        graph::views::filter::{internal::CreateNodeFilter, NodeTypeFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::inherit::Base;
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct NodeTypeFilteredGraph<G> {
    pub(crate) graph: G,
    pub(crate) node_types_filter: Arc<[bool]>,
}

impl<G> Static for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> Base for NodeTypeFilteredGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeTypeFilteredGraph<G> {
    pub fn new(graph: G, node_types_filter: Arc<[bool]>) -> Self {
        Self {
            graph,
            node_types_filter,
        }
    }
}

impl CreateNodeFilter for NodeTypeFilter {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>> = NodeTypeFilteredGraph<G>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        let node_types_filter = graph
            .node_meta()
            .node_type_meta()
            .all_keys()
            .iter()
            .map(|k| self.0.matches(Some(k))) // TODO: _default check
            .collect::<Vec<_>>();
        Ok(NodeTypeFilteredGraph::new(graph, node_types_filter.into()))
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritAllEdgeFilterOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for NodeTypeFilteredGraph<G> {
    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.node_types_filter
            .get(node.node_type_id())
            .copied()
            .unwrap_or(false)
            && self.graph.internal_filter_node(node, layer_ids)
    }
}

#[cfg(test)]
mod tests_node_type_filtered_subgraph {}
