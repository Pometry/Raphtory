use crate::core::entities::LayerIds;
use enum_dispatch::enum_dispatch;
use raphtory_api::inherit::Base;
use raphtory_storage::graph::nodes::node_ref::NodeStorageRef;

#[enum_dispatch]
pub trait InternalNodeFilterOps {
    /// Check if GraphView filters nodes (i.e., there exists nodes in the underlying graph for which `filter_node` returns false
    fn internal_nodes_filtered(&self) -> bool;

    /// Check if node list can be trusted. (if false, nodes in `self.node_list` need further filtering,
    /// if true, the result of `self.node_list` can be trusted, in particular, its len is the number
    /// of nodes in the graph).
    fn internal_node_list_trusted(&self) -> bool;

    /// If true, do not need to check src and dst of the edge separately, even if nodes are filtered
    /// (i.e., edge filter already makes sure there are no edges between non-existent nodes)
    /// This should be `false` when implementing `NodeFilterOps` without overriding the edge filter.
    fn edge_and_node_filter_independent(&self) -> bool;

    /// If `true`, node is included in the graph
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool;
}

pub trait InheritNodeFilterOps: Base {}

impl<G: InheritNodeFilterOps> InternalNodeFilterOps for G
where
    G::Base: InternalNodeFilterOps,
{
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.base().internal_nodes_filtered()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.base().internal_node_list_trusted()
    }

    #[inline]
    fn edge_and_node_filter_independent(&self) -> bool {
        self.base().edge_and_node_filter_independent()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.base().internal_filter_node(node, layer_ids)
    }
}
