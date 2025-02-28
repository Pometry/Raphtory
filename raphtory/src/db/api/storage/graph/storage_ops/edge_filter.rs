use super::GraphStorage;
use crate::{
    core::entities::LayerIds,
    db::api::{storage::graph::edges::edge_ref::EdgeStorageRef, view::internal::EdgeFilterOps},
};

impl EdgeFilterOps for GraphStorage {
    fn edges_filtered(&self) -> bool {
        false
    }

    fn edge_list_trusted(&self) -> bool {
        true
    }

    fn filter_edge(&self, _edge: EdgeStorageRef, _layer_ids: &LayerIds) -> bool {
        true
    }
}
