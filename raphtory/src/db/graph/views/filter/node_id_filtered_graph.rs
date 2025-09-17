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
        graph::views::filter::{
            internal::CreateFilter,
            model::{
                node_filter::{NodeFilter, NodeIdFilter},
                Filter,
            },
        },
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
pub struct NodeIdFilteredGraph<G> {
    graph: G,
    filter: Filter,
}

impl<G> NodeIdFilteredGraph<G> {
    pub(crate) fn new(graph: G, filter: Filter) -> Self {
        Self { graph, filter }
    }
}

impl CreateFilter for NodeIdFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeIdFilteredGraph<G>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        NodeFilter::validate(graph.id_type(), &self.0)?;
        Ok(NodeIdFilteredGraph::new(graph, self.0))
    }
}

impl<G> Base for NodeIdFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for NodeIdFilteredGraph<G> {}
impl<G> Immutable for NodeIdFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for NodeIdFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeIdFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeIdFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeIdFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeIdFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritAllEdgeFilterOps for NodeIdFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeIdFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeIdFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeIdFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeIdFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for NodeIdFilteredGraph<G> {
    fn internal_nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids) && self.filter.id_matches(node.id())
    }
}
