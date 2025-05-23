use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritEdgeFilterOps, InheritEdgeHistoryFilter, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalNodeFilterOps, Static,
            },
        },
        graph::views::filter::{internal::CreateNodeFilter, model::NotFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::nodes::node_ref::NodeStorageRef};

#[derive(Debug, Clone)]
pub struct NodeNotFilteredGraph<G, T> {
    graph: G,
    filter: T,
}

impl<T: CreateNodeFilter> CreateNodeFilter for NotFilter<T> {
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

impl<'graph, G: GraphViewOps<'graph>, T> InheritCoreGraphOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritStorageOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritLayerOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritListOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritMaterialize for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgeFilterOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritPropertiesOps for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritTimeSemantics for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodeHistoryFilter for NodeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgeHistoryFilter for NodeNotFilteredGraph<G, T> {}

impl<G: InternalNodeFilterOps, T: InternalNodeFilterOps> InternalNodeFilterOps
    for NodeNotFilteredGraph<G, T>
{
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_and_node_filter_independent(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.internal_filter_node(node, layer_ids)
            && !self.filter.internal_filter_node(node.clone(), layer_ids)
    }
}
