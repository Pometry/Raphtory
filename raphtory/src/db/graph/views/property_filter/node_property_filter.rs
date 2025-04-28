use crate::{
    core::{entities::LayerIds, utils::errors::GraphError},
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            view::{
                internal::{
                    Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                    InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
                    InheritTimeSemantics, InternalNodeFilterOps, Static,
                },
                node::NodeViewOps,
                Base,
            },
        },
        graph::{node::NodeView, views::property_filter::internal::InternalNodePropertyFilterOps},
    },
    prelude::{GraphViewOps, PropertyFilter},
};

use crate::db::api::view::internal::InheritStorageOps;

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

impl InternalNodePropertyFilterOps for PropertyFilter {
    type NodePropertyFiltered<'graph, G: GraphViewOps<'graph>> = NodePropertyFilteredGraph<G>;

    fn create_node_property_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodePropertyFiltered<'graph, G>, GraphError> {
        let t_prop_id = self.resolve_temporal_prop_ids(graph.node_meta())?;
        let c_prop_id = self.resolve_constant_prop_ids(graph.node_meta())?;
        Ok(NodePropertyFilteredGraph::new(
            graph, t_prop_id, c_prop_id, self,
        ))
    }
}

impl<G> Static for NodePropertyFilteredGraph<G> {}
impl<G> Immutable for NodePropertyFilteredGraph<G> {}

impl<'graph, G> Base for NodePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

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
    fn edge_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        if self.graph.internal_filter_node(node, layer_ids) {
            let props = NodeView::new_internal(&self.graph, node.vid()).properties();
            let prop_value = self
                .t_prop_id
                .and_then(|prop_id| {
                    props
                        .temporal()
                        .get_by_id(prop_id)
                        .and_then(|prop_view| prop_view.latest())
                })
                .or_else(|| {
                    self.c_prop_id
                        .and_then(|prop_id| props.constant().get_by_id(prop_id))
                });
            self.filter.matches(prop_value.as_ref())
        } else {
            false
        }
    }
}
