use crate::{core::entities::LayerIds, db::api::view::MaterializedGraph};
use enum_dispatch::enum_dispatch;
use raphtory_memstorage::db::api::{storage::graph::nodes::node_ref::NodeStorageRef, view::internal::inherit::Base};

#[enum_dispatch]
pub trait NodeFilterOps {
    /// Check if GraphView filters nodes (i.e., there exists nodes in the underlying graph for which `filter_node` returns false
    fn nodes_filtered(&self) -> bool;

    /// Check if node list can be trusted. (if false, nodes in `self.node_list` need further filtering,
    /// if true, the result of `self.node_list` can be trusted, in particular, its len is the number
    /// of nodes in the graph)
    fn node_list_trusted(&self) -> bool;

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
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.base().filter_node(node, layer_ids)
    }
}
