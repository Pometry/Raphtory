use crate::{
    core::{
        entities::{edges::edge_store::EdgeStore, LayerIds, VID},
        storage::timeindex::{TimeIndex, TimeIndexEntry},
    },
    db::api::view::internal::Base,
};
use enum_dispatch::enum_dispatch;
use std::{ops::Range, sync::Arc};

pub trait EdgeLike {
    fn active(&self, layer_ids: &LayerIds, w: Range<i64>) -> bool;
    fn has_layer(&self, layer_ids: &LayerIds) -> bool;
    fn src(&self) -> VID;
    fn dst(&self) -> VID;

    fn additions_iter(&self) -> Box<dyn Iterator<Item = &TimeIndex<TimeIndexEntry>> + '_>;
    fn deletions_iter(&self) -> Box<dyn Iterator<Item = &TimeIndex<TimeIndexEntry>> + '_>;

    fn additions(&self, layer_id: usize) -> Option<&TimeIndex<TimeIndexEntry>>;
    fn deletions(&self, layer_id: usize) -> Option<&TimeIndex<TimeIndexEntry>>;
}

impl EdgeLike for EdgeStore {
    fn active(&self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        self.active(layer_ids, w)
    }

    fn has_layer(&self, layer_ids: &LayerIds) -> bool {
        self.has_layer(layer_ids)
    }

    fn src(&self) -> VID {
        self.src()
    }

    fn dst(&self) -> VID {
        self.dst()
    }

    fn additions_iter(&self) -> Box<dyn Iterator<Item = &TimeIndex<TimeIndexEntry>> + '_> {
        Box::new(self.additions().into_iter())
    }

    fn deletions_iter(&self) -> Box<dyn Iterator<Item = &TimeIndex<TimeIndexEntry>> + '_> {
        Box::new(self.deletions().into_iter())
    }

    fn additions(&self, layer_id: usize) -> Option<&TimeIndex<TimeIndexEntry>> {
        self.additions().get(layer_id)
    }

    fn deletions(&self, layer_id: usize) -> Option<&TimeIndex<TimeIndexEntry>> {
        self.deletions().get(layer_id)
    }
}

pub type EdgeFilter = Arc<dyn Fn(&dyn EdgeLike, &LayerIds) -> bool + Send + Sync>;
pub type EdgeWindowFilter = Arc<dyn Fn(&dyn EdgeLike, &LayerIds, Range<i64>) -> bool + Send + Sync>;

#[enum_dispatch]
pub trait EdgeFilterOps {
    /// Return the optional edge filter for the graph
    fn edge_filter(&self) -> Option<&EdgeFilter>;

    /// Called by the windowed graph to get the edge filter (override if it should include more/different edges than a non-windowed graph)
    #[inline]
    fn edge_filter_window(&self) -> Option<&EdgeFilter> {
        self.edge_filter()
    }
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
    fn edge_filter(&self) -> Option<&EdgeFilter> {
        self.graph().edge_filter()
    }
}
