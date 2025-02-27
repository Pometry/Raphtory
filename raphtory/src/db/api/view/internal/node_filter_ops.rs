use crate::{
    core::entities::LayerIds,
    db::api::{
        storage::graph::nodes::node_ref::NodeStorageRef,
        view::{Base, MaterializedGraph},
    },
};
use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait NodeFilterOps {
    /// Check if GraphView filters nodes (i.e., there exists nodes in the underlying graph for which `filter_node` returns false
    fn nodes_filtered(&self) -> bool;

    /// Check if node list can be trusted. (if false, nodes in `self.node_list` need further filtering,
    /// if true, the result of `self.node_list` can be trusted, in particular, its len is the number
    /// of nodes in the graph)
    fn node_list_trusted(&self) -> bool;

    /// If true, do not need to check src and dst of the edge separately, even if nodes are filtered
    /// (i.e., edge filter already makes sure there are no edges between non-existent nodes)
    /// This should be `false` when implementing `NodeFilterOps` without overriding the edge filter.
    fn edge_filter_includes_node_filter(&self) -> bool;

    /// If `true`, node is included in the graph
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool;
}

pub trait InheritNodeFilterOps: Base {}

impl<G: InheritNodeFilterOps> NodeFilterOps for G
where
    G::Base: NodeFilterOps,
{
    #[inline]
    fn nodes_filtered(&self) -> bool {
        self.base().nodes_filtered()
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        self.base().node_list_trusted()
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        self.base().edge_filter_includes_node_filter()
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.base().filter_node(node, layer_ids)
    }
}
