use raphtory_memstorage::db::api::storage::graph::{nodes::node_ref::NodeStorageRef, GraphStorage};

use crate::{core::entities::LayerIds, db::api::view::internal::NodeFilterOps};

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
