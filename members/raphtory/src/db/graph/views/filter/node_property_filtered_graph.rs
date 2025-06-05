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
        graph::views::filter::{
            internal::CreateNodeFilter, model::property_filter::PropertyFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::nodes::node_ref::NodeStorageRef};

#[derive(Debug, Clone)]
pub struct NodePropertyFilteredGraph<G> {
    graph: G,
    t_prop_id: Option<usize>,
    c_prop_id: Option<usize>,
    filter: PropertyFilter,
}

impl<G> NodePropertyFilteredGraph<G> {
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

impl CreateNodeFilter for PropertyFilter {
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

impl<G> Base for NodePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for NodePropertyFilteredGraph<G> {}
impl<G> Immutable for NodePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for NodePropertyFilteredGraph<G> {
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
        if self.graph.internal_filter_node(node, layer_ids) {
            self.filter
                .matches_node(&self.graph, self.t_prop_id, self.c_prop_id, node)
        } else {
            false
        }
    }
}
