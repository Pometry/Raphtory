use super::GraphStorage;
use crate::{
    core::entities::LayerIds,
    db::api::view::internal::{
        InternalEdgeFilterOps, InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps,
    },
};
use raphtory_api::core::{
    entities::{LayerId, ELID},
    storage::timeindex::EventTime,
};
use storage::EdgeEntryRef;

impl InternalEdgeFilterOps for GraphStorage {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        false
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_edge(&self, _edge: EdgeEntryRef, _layer_ids: &LayerIds) -> bool {
        true
    }

    fn node_filter_includes_edge_filter(&self) -> bool {
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
        _t: EventTime,
        _layer_ids: &LayerIds,
    ) -> bool {
        true
    }

    #[inline]
    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
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
    fn internal_filter_edge_layer(&self, _edge: EdgeEntryRef, _layer: LayerId) -> bool {
        true
    }

    #[inline]
    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        true
    }
}
