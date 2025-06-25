use super::GraphStorage;
use crate::{core::entities::LayerIds, db::api::view::internal::InternalEdgeFilterOps};
use raphtory_api::core::{entities::ELID, storage::timeindex::TimeIndexEntry};
use raphtory_storage::graph::edges::edge_ref::EdgeStorageRef;

impl InternalEdgeFilterOps for GraphStorage {
    fn internal_edges_filtered(&self) -> bool {
        false
    }

    fn edge_history_filtered(&self) -> bool {
        false
    }

    fn internal_edge_list_trusted(&self) -> bool {
        true
    }

    fn internal_filter_edge_history(
        &self,
        _eid: ELID,
        _t: TimeIndexEntry,
        _layer_ids: &LayerIds,
    ) -> bool {
        true
    }

    fn internal_filter_edge(&self, _edge: EdgeStorageRef, _layer_ids: &LayerIds) -> bool {
        true
    }
}
