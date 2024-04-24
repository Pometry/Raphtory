use crate::{
    core::entities::{edges::edge_store::EdgeStore, LayerIds},
    db::api::{
        storage::edges::edge_ref::EdgeStorageRef,
        view::internal::{EdgeFilter, EdgeFilterOps},
    },
};

use super::ArrowGraph;

impl EdgeFilterOps for ArrowGraph {
    fn edges_filtered(&self) -> bool {
        false
    }

    fn edge_list_trusted(&self) -> bool {
        true
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        true
    }

    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        true
    }
}
