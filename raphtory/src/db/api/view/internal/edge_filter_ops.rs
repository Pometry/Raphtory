use crate::{
    core::{
        entities::{edges::edge_store::EdgeStore, LayerIds, VID},
        storage::timeindex::{TimeIndex, TimeIndexEntry, TimeIndexOps},
    },
    db::api::view::internal::Base,
};
use enum_dispatch::enum_dispatch;
use std::{ops::Range, sync::Arc};

pub enum TimeIndexLike<'a> {
    TimeIndex(&'a TimeIndex<TimeIndexEntry>),
    // External(Box<dyn BoxTimeIndexOps>),
}

impl <'a> TimeIndexOps for TimeIndexLike<'a> {
    type IterType<'b> = Box<dyn Iterator<Item = &'b i64> + Send + 'b> where Self: 'b;

    type WindowType<'b> = TimeIndexLike<'b> where Self: 'b;

    type IndexType = i64;

    fn active(&self, w: Range<i64>) -> bool {
        todo!()
    }

    fn range(&self, w: Range<i64>) -> Self::WindowType<'_> {
        todo!()
    }

    fn first(&self) -> Option<&Self::IndexType> {
        todo!()
    }

    fn last(&self) -> Option<&Self::IndexType> {
        todo!()
    }

    fn iter_t(&self) -> Self::IterType<'_> {
        todo!()
    }
}

pub trait EdgeLike {
    fn active(&self, layer_ids: &LayerIds, w: Range<i64>) -> bool;
    fn has_layer(&self, layer_ids: &LayerIds) -> bool;
    fn src(&self) -> VID;
    fn dst(&self) -> VID;

    fn additions_iter(&self) -> Box<dyn Iterator<Item = TimeIndexLike<'_>> + '_>;
    fn deletions_iter(&self) -> Box<dyn Iterator<Item = TimeIndexLike<'_>> + '_>;

    fn additions(&self, layer_id: usize) -> Option<TimeIndexLike<'_>>;
    fn deletions(&self, layer_id: usize) -> Option<TimeIndexLike<'_>>;
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

    fn additions_iter(&self) -> Box<dyn Iterator<Item = TimeIndexLike<'_>> + '_> {
        Box::new(self.additions().into_iter().map(TimeIndexLike::TimeIndex))
    }

    fn deletions_iter(&self) -> Box<dyn Iterator<Item = TimeIndexLike<'_>> + '_> {
        Box::new(self.deletions().into_iter().map(TimeIndexLike::TimeIndex))
    }

    fn additions(&self, layer_id: usize) -> Option<TimeIndexLike<'_>> {
        self.additions().get(layer_id).map(TimeIndexLike::TimeIndex)
    }

    fn deletions(&self, layer_id: usize) -> Option<TimeIndexLike<'_>> {
        self.deletions().get(layer_id).map(TimeIndexLike::TimeIndex)
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
