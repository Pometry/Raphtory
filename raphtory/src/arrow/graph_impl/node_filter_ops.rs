use crate::{
    arrow::graph_impl::ArrowGraph,
    core::entities::LayerIds,
    db::api::{storage::nodes::node_ref::NodeStorageRef, view::internal::NodeFilterOps},
};

impl NodeFilterOps for ArrowGraph {
    fn nodes_filtered(&self) -> bool {
        false
    }

    fn node_list_trusted(&self) -> bool {
        true
    }

    fn filter_node(&self, _node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        true
    }
}
