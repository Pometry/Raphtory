use crate::core::entities::LayerIds;
use raphtory_api::{
    core::{entities::ELID, storage::timeindex::TimeIndexEntry},
    inherit::Base,
};
use raphtory_storage::graph::edges::edge_ref::EdgeStorageRef;

pub trait InternalEdgeFilterOps {
    /// If true, the edges from the underlying storage are filtered
    fn internal_edges_filtered(&self) -> bool;

    fn edge_history_filtered(&self) -> bool;

    /// If true, all edges returned by `self.edge_list()` exist, otherwise it needs further filtering
    fn internal_edge_list_trusted(&self) -> bool;

    fn internal_filter_edge_history(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool;

    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool;
}

pub trait InheritEdgeFilterOps: Base {}

impl<G: InheritEdgeFilterOps> DelegateEdgeFilterOps for G
where
    G::Base: InternalEdgeFilterOps,
{
    type Internal = G::Base;

    #[inline]
    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateEdgeFilterOps {
    type Internal: InternalEdgeFilterOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateEdgeFilterOps> InternalEdgeFilterOps for G {
    #[inline]
    fn internal_edges_filtered(&self) -> bool {
        self.graph().internal_edges_filtered()
    }

    #[inline]
    fn edge_history_filtered(&self) -> bool {
        self.graph().edge_history_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.graph().internal_edge_list_trusted()
    }

    fn internal_filter_edge_history(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph().internal_filter_edge_history(eid, t, layer_ids)
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph().internal_filter_edge(edge, layer_ids)
    }
}
