use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritAllEdgeFilterOps, InheritEdgeHistoryFilter, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritStorageOps, InheritTimeSemantics,
                InternalNodeFilterOps, NodeHistoryFilter, Static,
            },
        },
        graph::views::filter::{internal::CreateNodeFilter, model::OrFilter},
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
pub struct NodeOrFilteredGraph<G, L, R> {
    graph: G,
    left: L,
    right: R,
}

impl<L: CreateNodeFilter, R: CreateNodeFilter> CreateNodeFilter for OrFilter<L, R> {
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

impl<'graph, G: GraphViewOps<'graph>, L, R> InheritCoreGraphOps for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritStorageOps for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritLayerOps for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritListOps for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritMaterialize for NodeOrFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritAllEdgeFilterOps
    for NodeOrFilteredGraph<G, L, R>
{
}
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
        w: Range<TimeIndexEntry>,
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
        w: Range<TimeIndexEntry>,
    ) -> bool {
        self.left
            .is_node_prop_update_latest_window(prop_id, node_id, time, w.clone())
            || self
                .right
                .is_node_prop_update_latest_window(prop_id, node_id, time, w)
    }
}

impl<G, L: InternalNodeFilterOps, R: InternalNodeFilterOps> InternalNodeFilterOps
    for NodeOrFilteredGraph<G, L, R>
{
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.left.internal_nodes_filtered() && self.right.internal_nodes_filtered()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.left.internal_node_list_trusted() && self.right.internal_node_list_trusted()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.left.internal_filter_node(node, layer_ids)
            || self.right.internal_filter_node(node, layer_ids)
    }
}
