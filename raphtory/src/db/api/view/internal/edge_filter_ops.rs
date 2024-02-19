use crate::{
    arrow::timestamps::TimeStamps,
    core::{
        entities::{edges::edge_store::EdgeStore, LayerIds, VID},
        storage::timeindex::{TimeIndex, TimeIndexEntry, TimeIndexRefOps},
    },
    db::api::view::{internal::Base, IntoDynBoxed},
};
use enum_dispatch::enum_dispatch;
use std::{ops::Range, sync::Arc};

pub enum TimeIndexLike<'a> {
    TimeIndex(&'a TimeIndex<TimeIndexEntry>),
    ExternalRef(&'a dyn TimeIndexRefOps<IndexType = TimeIndexEntry>),
    External(TimeStamps<'a, TimeIndexEntry>),
}

impl<'a> TimeIndexRefOps for TimeIndexLike<'a> {
    type IndexType = TimeIndexEntry;

    fn active(&self, w: Range<i64>) -> bool {
        match self {
            TimeIndexLike::TimeIndex(ref t) => t.active(w),
            TimeIndexLike::ExternalRef(ref t) => t.active(w),
            TimeIndexLike::External(ref t) => t.active(w),
        }
    }

    fn range(&self, w: Range<i64>) -> Box<dyn TimeIndexRefOps<IndexType = Self::IndexType> + '_> {
        match self {
            TimeIndexLike::TimeIndex(ref t) => t.range(w),
            TimeIndexLike::ExternalRef(ref t) => t.range(w),
            TimeIndexLike::External(ref t) => t.range(w),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexLike::TimeIndex(ref t) => t.first(),
            TimeIndexLike::ExternalRef(ref t) => t.first(),
            TimeIndexLike::External(ref t) => t.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexLike::TimeIndex(ref t) => t.last(),
            TimeIndexLike::ExternalRef(ref t) => t.last(),
            TimeIndexLike::External(ref t) => t.last(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
        match self {
            TimeIndexLike::TimeIndex(ref t) => t.iter(),
            TimeIndexLike::ExternalRef(ref t) => t.iter(),
            TimeIndexLike::External(ref t) => t.iter(),
        }
    }
}

impl<'a> TimeIndexLike<'a> {
    pub fn into_iter(self) -> impl Iterator<Item = TimeIndexEntry> + Send + 'a {
        match self {
            TimeIndexLike::TimeIndex(t) => t.iter(),
            TimeIndexLike::ExternalRef(t) => t.iter(),
            TimeIndexLike::External(t) => t.into_iter().into_dyn_boxed(),
        }
    }

    pub fn into_range(
        self,
        w: Range<i64>,
    ) -> Box<dyn TimeIndexRefOps<IndexType = TimeIndexEntry> + 'a> {
        match self {
            TimeIndexLike::TimeIndex(t) => t.range(w),
            TimeIndexLike::ExternalRef(t) => t.range(w),
            TimeIndexLike::External(t) => Box::new(t.into_range(w)),
        }
    }
}

pub trait EdgeLike {
    fn active(&self, layer_ids: &LayerIds, w: Range<i64>) -> bool;
    fn has_layer(&self, layer_ids: &LayerIds) -> bool;
    fn src(&self) -> VID;
    fn dst(&self) -> VID;

    fn additions_iter<'a>(
        &'a self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a>;
    fn deletions_iter<'a>(
        &'a self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a>;

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

    fn additions_iter<'a>(
        &'a self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a> {
        Box::new(self.additions_iter(layer_ids).map(TimeIndexLike::TimeIndex))
    }

    fn deletions_iter<'a>(
        &'a self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a> {
        Box::new(self.deletions_iter(layer_ids).map(TimeIndexLike::TimeIndex))
    }

    fn additions(&self, layer_id: usize) -> Option<TimeIndexLike<'_>> {
        self.additions
            .get(layer_id)
            .map(|x| TimeIndexLike::TimeIndex(x))
    }

    fn deletions(&self, layer_id: usize) -> Option<TimeIndexLike<'_>> {
        self.deletions
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

    #[inline]
    fn edge_filter_window(&self) -> Option<&EdgeFilter> {
        self.graph().edge_filter_window()
    }
}
