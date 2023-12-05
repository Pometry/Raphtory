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
    External(&'a dyn TimeIndexOps<IndexType = TimeIndexEntry>),
}

impl <'a> TimeIndexOps for TimeIndexLike<'a> {
    type IndexType = TimeIndexEntry;

    fn active(&self, w: Range<i64>) -> bool {
        match self {
            TimeIndexLike::TimeIndex(ref t) => t.active(w),
            TimeIndexLike::External(ref t) => t.active(w),
        }
    }

    fn range(&self, w: Range<i64>) -> Box<dyn TimeIndexOps<IndexType = Self::IndexType> + '_> {
        match self {
            TimeIndexLike::TimeIndex(ref t) => t.range(w),
            TimeIndexLike::External(ref t) => t.range(w),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexLike::TimeIndex(ref t) => t.first(),
            TimeIndexLike::External(ref t) => t.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexLike::TimeIndex(ref t) => t.last(),
            TimeIndexLike::External(ref t) => t.last(),
        }
    }

    fn iter_t(&self) -> Box<dyn Iterator<Item = &i64> + Send + '_> {
        match self {
            TimeIndexLike::TimeIndex(ref t) => t.iter_t(),
            TimeIndexLike::External(ref t) => t.iter_t(),
        }
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
        Box::new(
            self.additions()
                .into_iter()
                .map(|x| TimeIndexLike::TimeIndex(x)),
        )
    }

    fn deletions_iter(&self) -> Box<dyn Iterator<Item = TimeIndexLike<'_>> + '_> {
        Box::new(
            self.deletions()
                .into_iter()
                .map(|x| TimeIndexLike::TimeIndex(x)),
        )
    }

    fn additions(&self, layer_id: usize) -> Option<TimeIndexLike<'_>> {
        self.additions()
            .get(layer_id)
            .map(|x| TimeIndexLike::TimeIndex(x))
    }

    fn deletions(&self, layer_id: usize) -> Option<TimeIndexLike<'_>> {
        self.deletions()
            .get(layer_id)
            .map(|x| TimeIndexLike::TimeIndex(x))
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
