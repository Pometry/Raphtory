use super::GraphStorage;
use crate::{core::entities::LayerIds, db::api::view::internal::InternalNodeFilterOps};
use raphtory_storage::graph::nodes::node_ref::NodeStorageRef;

impl InternalNodeFilterOps for GraphStorage {
    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        true
    }
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_node(&self, _node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        true
    }
}
