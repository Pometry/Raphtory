use crate::{
    core::entities::{graph::tgraph::InternalGraph, LayerIds},
    db::api::{storage::nodes::node_ref::NodeStorageRef, view::internal::NodeFilterOps},
};

impl NodeFilterOps for InternalGraph {
    #[inline]
    fn node_list_trusted(&self) -> bool {
        true
    }
    #[inline]
    fn nodes_filtered(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, _node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        true
    }
}
