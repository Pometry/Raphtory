use super::GraphStorage;
use crate::{
    core::entities::LayerIds,
    db::api::view::internal::{
        InternalEdgeFilterOps, InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps,
    },
};
use raphtory_api::core::{entities::ELID, storage::timeindex::TimeIndexEntry};
use raphtory_storage::graph::edges::edge_ref::EdgeStorageRef;

impl InternalEdgeFilterOps for GraphStorage {
    #[inline]
    fn internal_edges_filtered(&self) -> bool {
        false
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_edge(&self, _edge: EdgeStorageRef, _layer_ids: &LayerIds) -> bool {
        true
    }
}

impl InternalExplodedEdgeFilterOps for GraphStorage {
    #[inline]
    fn internal_exploded_edge_filtered(&self) -> bool {
        false
    }
    #[inline]
    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        true
    }
    #[inline]
    fn internal_filter_exploded_edge(
        &self,
        _eid: ELID,
        _t: TimeIndexEntry,
        _layer_ids: &LayerIds,
    ) -> bool {
        true
    }
}

impl InternalEdgeLayerFilterOps for GraphStorage {
    #[inline]
    fn internal_edge_layer_filtered(&self) -> bool {
        false
    }

    #[inline]
    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_edge_layer(&self, _edge: EdgeStorageRef, _layer: usize) -> bool {
        true
    }
}
