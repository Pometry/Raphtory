use crate::{
    core::entities::LayerIds,
    db::api::{storage::nodes::node_ref::NodeStorageRef, view::internal::NodeFilterOps},
};

use super::GraphStorage;

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
    fn filter_node(&self, _node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        true
    }
}
