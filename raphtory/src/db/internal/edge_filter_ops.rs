use crate::{
    core::entities::{graph::tgraph::InternalGraph, LayerIds},
    db::api::{storage::edges::edge_ref::EdgeStorageRef, view::internal::EdgeFilterOps},
};

impl EdgeFilterOps for InternalGraph {
    fn edges_filtered(&self) -> bool {
        false
    }

    fn edge_list_trusted(&self) -> bool {
        true
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        true
    }

    fn filter_edge(&self, _edge: EdgeStorageRef, _layer_ids: &LayerIds) -> bool {
        true
    }
}
