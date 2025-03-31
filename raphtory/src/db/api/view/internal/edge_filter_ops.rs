use crate::{
    core::entities::LayerIds,
    db::api::{storage::graph::edges::edge_ref::EdgeStorageRef, view::internal::Base},
};
use enum_dispatch::enum_dispatch;
use raphtory_api::core::{entities::ELID, storage::timeindex::TimeIndexEntry};

#[enum_dispatch]
pub trait EdgeFilterOps {
    /// If true, the edges from the underlying storage are filtered
    fn edges_filtered(&self) -> bool;

    fn edge_history_filtered(&self) -> bool;

    /// If true, all edges returned by `self.edge_list()` exist, otherwise it needs further filtering
    fn edge_list_trusted(&self) -> bool;

    fn filter_edge_history(&self, eid: ELID, t: TimeIndexEntry, layer_ids: &LayerIds) -> bool;

    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool;
}

pub trait InheritEdgeFilterOps: Base {}

impl<G: InheritEdgeFilterOps> DelegateEdgeFilterOps for G
where
    G::Base: EdgeFilterOps,
{
    type Internal = G::Base;

    #[inline]
    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateEdgeFilterOps {
    type Internal: EdgeFilterOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateEdgeFilterOps> EdgeFilterOps for G {
    #[inline]
    fn edges_filtered(&self) -> bool {
        self.graph().edges_filtered()
    }

    #[inline]
    fn edge_history_filtered(&self) -> bool {
        self.graph().edge_history_filtered()
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        self.graph().edge_list_trusted()
    }

    fn filter_edge_history(&self, eid: ELID, t: TimeIndexEntry, layer_ids: &LayerIds) -> bool {
        self.graph().filter_edge_history(eid, t, layer_ids)
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph().filter_edge(edge, layer_ids)
    }
}
