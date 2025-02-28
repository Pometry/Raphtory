use super::GraphStorage;
use crate::{
    core::entities::LayerIds,
    db::api::{storage::graph::nodes::node_ref::NodeStorageRef, view::internal::NodeFilterOps},
};

impl NodeFilterOps for GraphStorage {
    #[inline]
    fn node_list_trusted(&self) -> bool {
        true
    }
    #[inline]
    fn nodes_filtered(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        true
    }

    #[inline]
    fn filter_node(&self, _node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        true
    }
}
