use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            view::{
                internal::{
                    Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                    InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
                    InheritStorageOps, InheritTimeSemantics, NodeFilterOps, Static,
                },
                node::NodeViewOps,
                Base,
            },
        },
        graph::views::filter::{internal::InternalNodeFilterOps, Filter},
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::{entities::LayerIds, storage::arc_str::OptionAsStr};

#[derive(Debug, Clone)]
pub struct NodeFieldFilteredGraph<G> {
    graph: G,
    filter: Filter,
}

impl<'graph, G> NodeFieldFilteredGraph<G> {
    pub(crate) fn new(graph: G, filter: Filter) -> Self {
        Self { graph, filter }
    }
}

impl InternalNodeFilterOps for Filter {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>> = NodeFieldFilteredGraph<G>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        Ok(NodeFieldFilteredGraph::new(graph, self))
    }
}

impl<'graph, G> Base for NodeFieldFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for NodeFieldFilteredGraph<G> {}
impl<G> Immutable for NodeFieldFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for NodeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for NodeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeFieldFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for NodeFieldFilteredGraph<G> {
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
        if self.graph.filter_node(node, layer_ids) {
            match self.filter.field_name.as_str() {
                "node_name" => self.filter.matches(node.name().as_str()),
                "node_type" => self
                    .filter
                    .matches(self.graph.node_type(node.vid()).as_deref()),
                _ => false,
            }
        } else {
            false
        }
    }
}
