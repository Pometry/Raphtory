use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                EdgeList, Immutable, InheritAllEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritMaterialize, InheritStorageOps, InheritTimeSemantics, InternalLayerOps,
                InternalNodeFilterOps, ListOps, NodeHistoryFilter, NodeList, Static,
            },
        },
        graph::views::filter::{internal::CreateNodeFilter, model::AndFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerIds, VID},
        storage::timeindex::TimeIndexEntry,
    },
    inherit::Base,
};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::nodes::node_ref::NodeStorageRef};
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct NodeAndFilteredGraph<G, L, R> {
    graph: G,
    left: L,
    right: R,
    layer_ids: LayerIds,
}

impl<L: CreateNodeFilter, R: CreateNodeFilter> CreateNodeFilter for AndFilter<L, R> {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>>
        = NodeAndFilteredGraph<G, L::NodeFiltered<'graph, G>, R::NodeFiltered<'graph, G>>
    where
        Self: 'graph;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        let left = self.left.create_node_filter(graph.clone())?;
        let right = self.right.create_node_filter(graph.clone())?;
        let layer_ids = left.layer_ids().intersect(right.layer_ids());
        Ok(NodeAndFilteredGraph {
            graph,
            left,
            right,
            layer_ids,
        })
    }
}

impl<G, L, R> Base for NodeAndFilteredGraph<G, L, R> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, L, R> Static for NodeAndFilteredGraph<G, L, R> {}
impl<G, L, R> Immutable for NodeAndFilteredGraph<G, L, R> {}

impl<G, L, R> InheritCoreGraphOps for NodeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritStorageOps for NodeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritMaterialize for NodeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritAllEdgeFilterOps
    for NodeAndFilteredGraph<G, L, R>
{
}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritPropertiesOps for NodeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritTimeSemantics for NodeAndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritEdgeHistoryFilter
    for NodeAndFilteredGraph<G, L, R>
{
}

impl<G, L: Send + Sync, R: Send + Sync> InternalLayerOps for NodeAndFilteredGraph<G, L, R>
where
    G: InternalLayerOps,
{
    fn layer_ids(&self) -> &LayerIds {
        &self.layer_ids
    }
}

impl<G, L, R> ListOps for NodeAndFilteredGraph<G, L, R>
where
    L: ListOps,
    R: ListOps,
{
    fn node_list(&self) -> NodeList {
        let left = self.left.node_list();
        let right = self.right.node_list();
        left.intersection(&right)
    }

    fn edge_list(&self) -> EdgeList {
        let left = self.left.edge_list();
        let right = self.right.edge_list();
        left.intersection(&right)
    }
}

impl<G, L, R> NodeHistoryFilter for NodeAndFilteredGraph<G, L, R>
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
            && self
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
            && self
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
            && self
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
            && self
                .right
                .is_node_prop_update_latest_window(prop_id, node_id, time, w)
    }
}

impl<G, L: InternalNodeFilterOps, R: InternalNodeFilterOps> InternalNodeFilterOps
    for NodeAndFilteredGraph<G, L, R>
{
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.left.internal_nodes_filtered() || self.right.internal_nodes_filtered()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.left.internal_node_list_trusted() && self.right.internal_node_list_trusted()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.left.internal_filter_node(node, layer_ids)
            && self.right.internal_filter_node(node, layer_ids)
    }
}
