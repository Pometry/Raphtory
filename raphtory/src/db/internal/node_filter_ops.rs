use crate::{
    core::entities::{graph::tgraph::InnerTemporalGraph, nodes::node_store::NodeStore, LayerIds},
    db::api::view::internal::NodeFilterOps,
};

impl<const N: usize> NodeFilterOps for InnerTemporalGraph<N> {
    #[inline]
    fn node_list_trusted(&self) -> bool {
        true
    }
    #[inline]
    fn nodes_filtered(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, _node: &NodeStore, _layer_ids: &LayerIds) -> bool {
        true
    }
}
