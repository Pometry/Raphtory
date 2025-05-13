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
        graph::views::filter::{
            internal::InternalNodeFilterOps, model::property_filter::PropertyFilter,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::LayerIds;

#[derive(Debug, Clone)]
pub struct NodePropertyFilteredGraph<G> {
    graph: G,
    t_prop_id: Option<usize>,
    c_prop_id: Option<usize>,
    filter: PropertyFilter,
}

impl<'graph, G> NodePropertyFilteredGraph<G> {
    pub(crate) fn new(
        graph: G,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        filter: PropertyFilter,
    ) -> Self {
        Self {
            graph,
            t_prop_id,
            c_prop_id,
            filter,
        }
    }
}

impl InternalNodeFilterOps for PropertyFilter {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>> = NodePropertyFilteredGraph<G>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        let t_prop_id = self.resolve_temporal_prop_id(graph.node_meta())?;
        let c_prop_id = self.resolve_constant_prop_id(graph.node_meta())?;
        Ok(NodePropertyFilteredGraph::new(
            graph, t_prop_id, c_prop_id, self,
        ))
    }
}

impl<'graph, G> Base for NodePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for NodePropertyFilteredGraph<G> {}
impl<G> Immutable for NodePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for NodePropertyFilteredGraph<G> {
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
            self.filter
                .matches_node(&self.graph, self.t_prop_id, self.c_prop_id, node)
        } else {
            false
        }
    }
}
