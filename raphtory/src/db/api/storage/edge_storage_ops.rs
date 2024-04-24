use crate::{
    arrow::timestamps::TimeStamps,
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            properties::tprop::{LockedLayeredTProp, TPropOps},
            LayerIds, VID,
        },
        storage::timeindex::{TimeIndex, TimeIndexIntoOps, TimeIndexOps, TimeIndexWindow},
    },
    db::api::{storage::layer_variants::LayerVariants, view::IntoDynBoxed},
    prelude::TimeIndexEntry,
};
use rayon::prelude::*;
use std::ops::Range;

pub enum TimeIndexLike<'a> {
    Ref(&'a TimeIndex<TimeIndexEntry>),
    Range(TimeIndexWindow<'a, TimeIndexEntry>),
    #[cfg(feature = "arrow")]
    External(TimeStamps<'a, TimeIndexEntry>),
}

impl<'a> TimeIndexLike<'a> {
    pub fn len(&self) -> usize {
        match self {
            TimeIndexLike::Ref(ts) => ts.len(),
            TimeIndexLike::Range(ts) => ts.len(),
            #[cfg(feature = "arrow")]
            TimeIndexLike::External(ts) => ts.len(),
        }
    }
}

impl<'a> TimeIndexOps for TimeIndexLike<'a> {
    type IndexType = TimeIndexEntry;
    type RangeType<'b> = TimeIndexLike<'b> where Self: 'b;

    fn active(&self, w: Range<i64>) -> bool {
        match self {
            TimeIndexLike::Ref(ref t) => t.active(w),
            TimeIndexLike::Range(ref t) => t.active(w),
            #[cfg(feature = "arrow")]
            TimeIndexLike::External(ref t) => t.active(w),
        }
    }

    fn range(&self, w: Range<i64>) -> Self::RangeType<'_> {
        match self {
            TimeIndexLike::Ref(t) => TimeIndexLike::Range(t.range(w)),
            TimeIndexLike::Range(ref t) => TimeIndexLike::Range(t.range(w)),
            #[cfg(feature = "arrow")]
            TimeIndexLike::External(ref t) => TimeIndexLike::External(t.range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexLike::Ref(t) => t.first(),
            TimeIndexLike::Range(ref t) => t.first(),
            #[cfg(feature = "arrow")]
            TimeIndexLike::External(ref t) => t.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexLike::Ref(t) => t.last(),
            TimeIndexLike::Range(ref t) => t.last(),
            #[cfg(feature = "arrow")]
            TimeIndexLike::External(ref t) => t.last(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
        match self {
            TimeIndexLike::Ref(t) => t.iter(),
            TimeIndexLike::Range(t) => t.iter(),
            #[cfg(feature = "arrow")]
            TimeIndexLike::External(ref t) => t.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            TimeIndexLike::Ref(ts) => ts.len(),
            TimeIndexLike::Range(ts) => ts.len(),
            #[cfg(feature = "arrow")]
            TimeIndexLike::External(ref t) => t.len(),
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
            #[cfg(feature = "arrow")]
            TimeIndexLike::External(t) => TimeIndexLike::External(t.into_range(w)),
        }
    }
    fn into_iter(self) -> impl Iterator<Item = TimeIndexEntry> + Send + 'a {
        match self {
            TimeIndexLike::Ref(t) => t.iter(),
            TimeIndexLike::Range(t) => t.into_iter().into_dyn_boxed(),
            #[cfg(feature = "arrow")]
            TimeIndexLike::External(t) => t.into_iter().into_dyn_boxed(),
        }
    }
}

pub trait EdgeStorageOps<'a> {
    fn in_ref(self) -> EdgeRef;

    fn out_ref(self) -> EdgeRef;
    fn active(self, layer_ids: &LayerIds, w: Range<i64>) -> bool;
    fn has_layer(self, layer_ids: &LayerIds) -> bool;
    fn src(self) -> VID;
    fn dst(self) -> VID;

    fn additions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a>;

    fn additions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = TimeIndexLike<'a>> + 'a;
    fn deletions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a>;

    fn deletions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = TimeIndexLike<'a>> + 'a;

    fn updates_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexLike<'a>, TimeIndexLike<'a>)> + 'a;

    fn updates_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexLike<'a>, TimeIndexLike<'a>)> + 'a;

    fn additions(self, layer_id: usize) -> TimeIndexLike<'a>;
    fn deletions(self, layer_id: usize) -> TimeIndexLike<'a>;

    fn has_temporal_prop(self, layer_ids: &LayerIds, prop_id: usize) -> bool;

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize)
        -> Option<Box<dyn TPropOps + 'a>>;
}

impl<'a> EdgeStorageOps<'a> for &'a EdgeStore {
    fn in_ref(self) -> EdgeRef {
        EdgeRef::new_incoming(self.eid, self.src, self.dst)
    }

    fn out_ref(self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid, self.src, self.dst)
    }

    fn active(self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        self.active(layer_ids, w)
    }

    fn has_layer(self, layer_ids: &LayerIds) -> bool {
        self.has_layer(layer_ids)
    }

    fn src(self) -> VID {
        self.src()
    }

    fn dst(self) -> VID {
        self.dst()
    }

    fn additions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a> {
        Box::new(self.additions_iter_inner(layer_ids).map(TimeIndexLike::Ref))
    }

    fn additions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = TimeIndexLike<'a>> + 'a {
        let iter = match layer_ids {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::All => LayerVariants::All(self.additions.par_iter()),
            LayerIds::One(layer_id) => {
                LayerVariants::One(self.additions.get(*layer_id).into_par_iter())
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.par_iter()
                    .flat_map(|layer_id| self.additions.get(*layer_id)),
            ),
        };
        iter.map(TimeIndexLike::Ref)
    }

    fn deletions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = TimeIndexLike<'a>> + 'a> {
        Box::new(self.deletions_iter_inner(layer_ids).map(TimeIndexLike::Ref))
    }

    fn deletions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = TimeIndexLike<'a>> + 'a {
        let iter = match layer_ids {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::All => LayerVariants::All(self.deletions.par_iter()),
            LayerIds::One(layer_id) => {
                LayerVariants::One(self.deletions.get(*layer_id).into_par_iter())
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.par_iter()
                    .flat_map(|layer_id| self.deletions.get(*layer_id)),
            ),
        };
        iter.map(TimeIndexLike::Ref)
    }

    fn updates_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexLike<'a>, TimeIndexLike<'a>)> + 'a {
        self.updates_iter_inner(layer_ids)
            .map(|(layer, additions, deletions)| {
                (
                    layer,
                    TimeIndexLike::Ref(additions),
                    TimeIndexLike::Ref(deletions),
                )
            })
    }

    fn updates_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexLike<'a>, TimeIndexLike<'a>)> + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::All => {
                let len = self.additions.len().max(self.deletions.len());
                LayerVariants::All(
                    (0..len).into_par_iter().map(|layer_id| {
                        (layer_id, self.additions(layer_id), self.deletions(layer_id))
                    }),
                )
            }
            LayerIds::One(layer_id) => LayerVariants::One(rayon::iter::once((
                *layer_id,
                self.additions(*layer_id),
                self.deletions(*layer_id),
            ))),
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(ids.par_iter().map(|&layer_id| {
                    (layer_id, self.additions(layer_id), self.deletions(layer_id))
                }))
            }
        }
    }

    fn additions(self, layer_id: usize) -> TimeIndexLike<'a> {
        TimeIndexLike::Ref(self.additions.get(layer_id).unwrap_or_default())
    }

    fn deletions(self, layer_id: usize) -> TimeIndexLike<'a> {
        TimeIndexLike::Ref(self.deletions.get(layer_id).unwrap_or_default())
    }

    fn has_temporal_prop(self, layer_ids: &LayerIds, prop_id: usize) -> bool {
        self.has_temporal_prop(layer_ids, prop_id)
    }

    fn temporal_prop_layer(
        self,
        layer_id: usize,
        prop_id: usize,
    ) -> Option<Box<dyn TPropOps + 'a>> {
        self.temporal_prop_layer(layer_id, prop_id)
            .map(|props| Box::new(LockedLayeredTProp::One(props)) as Box<dyn TPropOps>)
    }
}
