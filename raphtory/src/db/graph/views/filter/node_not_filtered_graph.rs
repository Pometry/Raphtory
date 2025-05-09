use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::nodes::node_ref::NodeStorageRef,
            view::{
                internal::{
                    Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                    InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
                    InheritStorageOps, InheritTimeSemantics, NodeFilterOps, Static,
                },
                Base,
            },
        },
        graph::views::filter::{internal::InternalNodeFilterOps, model::NotFilter},
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::LayerIds;

#[derive(Debug, Clone)]
pub struct NodeNotFilteredGraph<G, T> {
    graph: G,
    filter: T,
}

impl<T: InternalNodeFilterOps> InternalNodeFilterOps for NotFilter<T> {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>>
        = NodeNotFilteredGraph<G, T::NodeFiltered<'graph, G>>
    where
        Self: 'graph;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        let filter = self.0.create_node_filter(graph.clone())?;
        Ok(NodeNotFilteredGraph { graph, filter })
    }
}

impl<G, T> Base for NodeNotFilteredGraph<G, T> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, T> Static for NodeNotFilteredGraph<G, T> {}
impl<G, T> Immutable for NodeNotFilteredGraph<G, T> {}

impl<'graph, G: GraphViewOps<'graph>, T> InheritCoreOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritStorageOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritLayerOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritListOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritMaterialize for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgeFilterOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritPropertiesOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritTimeSemantics for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodeHistoryFilter for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgeHistoryFilter for NodeNotFilteredGraph<G, T> {}

impl<G, T: NodeFilterOps> NodeFilterOps for NodeNotFilteredGraph<G, T> {
    #[inline]
    fn nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_node(node, layer_ids) && !self.filter.filter_node(node.clone(), layer_ids)
    }
}
