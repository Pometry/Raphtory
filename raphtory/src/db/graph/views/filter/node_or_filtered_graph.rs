use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::nodes::node_ref::NodeStorageRef,
            view::{
                internal::{
                    Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                    InheritLayerOps, InheritListOps, InheritMaterialize, InheritStorageOps,
                    InheritTimeSemantics, InternalLayerOps, NodeFilterOps, NodeHistoryFilter,
                    Static,
                },
                Base,
            },
        },
        graph::views::filter::{internal::InternalNodeFilterOps, model::OrFilter},
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{LayerIds, VID},
    storage::timeindex::TimeIndexEntry,
};
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct NodeOrFilteredGraph<G, L, R> {
    graph: G,
    left: L,
    right: R,
}

impl<L: InternalNodeFilterOps, R: InternalNodeFilterOps> InternalNodeFilterOps for OrFilter<L, R> {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>>
        = NodeOrFilteredGraph<G, L::NodeFiltered<'graph, G>, R::NodeFiltered<'graph, G>>
    where
        Self: 'graph;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        let left = self.left.create_node_filter(graph.clone())?;
        let right = self.right.create_node_filter(graph.clone())?;
        Ok(NodeOrFilteredGraph { graph, left, right })
    }
}

impl<G, L, R> Base for NodeOrFilteredGraph<G, L, R> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, L, R> Static for NodeOrFilteredGraph<G, L, R> {}
impl<G, L, R> Immutable for NodeOrFilteredGraph<G, L, R> {}

impl<'graph, G: GraphViewOps<'graph>, L, R> InheritCoreOps for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritStorageOps for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritLayerOps for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritListOps for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritMaterialize for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritEdgeFilterOps for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritPropertiesOps for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritTimeSemantics for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritEdgeHistoryFilter
    for NodeOrFilteredGraph<G, L, R>
{
}

impl<G, L, R> NodeHistoryFilter for NodeOrFilteredGraph<G, L, R>
where
    L: NodeHistoryFilter,
    R: NodeHistoryFilter,
{
    fn is_node_prop_update_available(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        self.left
            .is_node_prop_update_available(prop_id, node_id, time)
            || self
                .right
                .is_node_prop_update_available(prop_id, node_id, time)
    }

    fn is_node_prop_update_available_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.left
            .is_node_prop_update_available_window(prop_id, node_id, time, w.clone())
            || self
                .right
                .is_node_prop_update_available_window(prop_id, node_id, time, w)
    }

    fn is_node_prop_update_latest(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
    ) -> bool {
        self.left.is_node_prop_update_latest(prop_id, node_id, time)
            || self
                .right
                .is_node_prop_update_latest(prop_id, node_id, time)
    }

    fn is_node_prop_update_latest_window(
        &self,
        prop_id: usize,
        node_id: VID,
        time: TimeIndexEntry,
        w: Range<i64>,
    ) -> bool {
        self.left
            .is_node_prop_update_latest_window(prop_id, node_id, time, w.clone())
            || self
                .right
                .is_node_prop_update_latest_window(prop_id, node_id, time, w)
    }
}

impl<G, L: NodeFilterOps, R: NodeFilterOps> NodeFilterOps for NodeOrFilteredGraph<G, L, R> {
    #[inline]
    fn nodes_filtered(&self) -> bool {
        self.left.nodes_filtered() && self.right.nodes_filtered()
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        self.left.node_list_trusted() && self.right.node_list_trusted()
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.left.filter_node(node.clone(), layer_ids) || self.right.filter_node(node, layer_ids)
    }
}
