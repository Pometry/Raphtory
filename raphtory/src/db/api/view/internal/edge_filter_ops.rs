use crate::{
    core::{
        entities::{edges::edge_store::EdgeStore, LayerIds, VID},
        storage::timeindex::{
            TimeIndex, TimeIndexEntry, TimeIndexIntoOps, TimeIndexOps, TimeIndexWindow,
        },
    },
    db::api::view::{internal::Base, IntoDynBoxed},
};
use enum_dispatch::enum_dispatch;
use std::{ops::Range, sync::Arc};

pub enum TimeIndexLike<'a> {
    Ref(&'a TimeIndex<TimeIndexEntry>),
    Range(TimeIndexWindow<'a, TimeIndexEntry>),
}

impl<'a> TimeIndexOps for TimeIndexLike<'a> {
    type IndexType = TimeIndexEntry;
    type RangeType<'b> = TimeIndexLike<'b> where Self: 'b;

    fn active(&self, w: Range<i64>) -> bool {
        match self {
            TimeIndexLike::Ref(t) => t.active(w),
            TimeIndexLike::Range(ref t) => t.active(w),
        }
    }

    fn range(&self, w: Range<i64>) -> Self::RangeType<'_> {
        match self {
            TimeIndexLike::Ref(t) => TimeIndexLike::Range(t.range(w)),
            TimeIndexLike::Range(ref t) => TimeIndexLike::Range(t.range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexLike::Ref(t) => t.first(),
            TimeIndexLike::Range(ref t) => t.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexLike::Ref(t) => t.last(),
            TimeIndexLike::Range(ref t) => t.last(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
        match self {
            TimeIndexLike::Ref(t) => t.iter(),
            TimeIndexLike::Range(t) => t.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            TimeIndexLike::Ref(ts) => ts.len(),
            TimeIndexLike::Range(ts) => ts.len(),
        }
    }
}

impl<'a> TimeIndexIntoOps for TimeIndexLike<'a> {
    type IndexType = TimeIndexEntry;

    type RangeType = Self;

    fn into_range(self, w: Range<i64>) -> TimeIndexLike<'a> {
        match self {
            TimeIndexLike::Ref(t) => TimeIndexLike::Range(t.range_inner(w)),
            TimeIndexLike::Range(t) => TimeIndexLike::Range(t.into_range(w)),
        }
    }
    fn into_iter(self) -> impl Iterator<Item = TimeIndexEntry> + Send + 'a {
        match self {
            TimeIndexLike::Ref(t) => t.iter(),
            TimeIndexLike::Range(t) => t.into_iter().into_dyn_boxed(),
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
        Box::new(self.additions_iter(layer_ids).map(TimeIndexLike::Ref))
    }

    fn deletions_iter<'a>(
        &'a self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a> {
        Box::new(self.deletions_iter(layer_ids).map(TimeIndexLike::Ref))
    }

    fn additions(&self, layer_id: usize) -> Option<TimeIndexLike<'_>> {
        self.additions.get(layer_id).map(|x| TimeIndexLike::Ref(x))
    }

    fn deletions(&self, layer_id: usize) -> Option<TimeIndexLike<'_>> {
        self.deletions.get(layer_id).map(|x| TimeIndexLike::Ref(x))
    }
}

pub type EdgeFilter = Arc<dyn Fn(&dyn EdgeLike, &LayerIds) -> bool + Send + Sync>;
pub type EdgeWindowFilter = Arc<dyn Fn(&dyn EdgeLike, &LayerIds, Range<i64>) -> bool + Send + Sync>;

#[enum_dispatch]
pub trait EdgeFilterOps {
    /// If true, the edges from the underlying storage are filtered
    fn edges_filtered(&self) -> bool;

    /// If true, all edges returned by `self.edge_list()` exist, otherwise it needs further filtering
    fn edge_list_trusted(&self) -> bool;

    fn filter_edge(&self, edge: &EdgeStore, layer_ids: &LayerIds) -> bool;
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
    fn edge_list_trusted(&self) -> bool {
        self.graph().edge_list_trusted()
    }

    #[inline]
    fn filter_edge(&self, edge: &EdgeStore, layer_ids: &LayerIds) -> bool {
        self.graph().filter_edge(edge, layer_ids)
    }
}
