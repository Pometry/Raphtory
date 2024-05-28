use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            LayerIds, VID,
        },
        storage::timeindex::{TimeIndex, TimeIndexIntoOps, TimeIndexOps, TimeIndexWindow},
    },
    db::api::view::IntoDynBoxed,
};

#[cfg(feature = "arrow")]
use raphtory_arrow::timestamps::TimeStamps;

use crate::{
    core::entities::properties::tprop::TProp,
    db::api::storage::{tprop_storage_ops::TPropOps, variants::layer_variants::LayerVariants},
};
use rayon::prelude::*;
use std::ops::Range;
use tantivy::HasLen;
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use crate::prelude::EdgeViewOps;

pub enum TimeIndexRef<'a> {
    Ref(&'a TimeIndex<TimeIndexEntry>),
    Range(TimeIndexWindow<'a, TimeIndexEntry>),
    #[cfg(feature = "arrow")]
    External(TimeStamps<'a, TimeIndexEntry>),
}

impl<'a> TimeIndexRef<'a> {
    pub fn len(&self) -> usize {
        match self {
            TimeIndexRef::Ref(ts) => ts.len(),
            TimeIndexRef::Range(ts) => ts.len(),
            #[cfg(feature = "arrow")]
            TimeIndexRef::External(ts) => ts.len(),
        }
    }
}

impl<'a> TimeIndexOps for TimeIndexRef<'a> {
    type IndexType = TimeIndexEntry;
    type RangeType<'b> = TimeIndexRef<'b> where Self: 'b;

    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        match self {
            TimeIndexRef::Ref(t) => t.active(w),
            TimeIndexRef::Range(ref t) => t.active(w),
            #[cfg(feature = "arrow")]
            TimeIndexRef::External(ref t) => t.active(w),
        }
    }

    fn range(&self, w: Range<TimeIndexEntry>) -> Self::RangeType<'_> {
        match self {
            TimeIndexRef::Ref(t) => TimeIndexRef::Range(t.range(w)),
            TimeIndexRef::Range(ref t) => TimeIndexRef::Range(t.range(w)),
            #[cfg(feature = "arrow")]
            TimeIndexRef::External(ref t) => TimeIndexRef::External(t.range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexRef::Ref(t) => t.first(),
            TimeIndexRef::Range(ref t) => t.first(),
            #[cfg(feature = "arrow")]
            TimeIndexRef::External(ref t) => t.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexRef::Ref(t) => t.last(),
            TimeIndexRef::Range(ref t) => t.last(),
            #[cfg(feature = "arrow")]
            TimeIndexRef::External(ref t) => t.last(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
        match self {
            TimeIndexRef::Ref(t) => t.iter(),
            TimeIndexRef::Range(t) => t.iter(),
            #[cfg(feature = "arrow")]
            TimeIndexRef::External(ref t) => t.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            TimeIndexRef::Ref(ts) => ts.len(),
            TimeIndexRef::Range(ts) => ts.len(),
            #[cfg(feature = "arrow")]
            TimeIndexRef::External(ref t) => t.len(),
        }
    }
}

impl<'a> TimeIndexIntoOps for TimeIndexRef<'a> {
    type IndexType = TimeIndexEntry;

    type RangeType = Self;

    fn into_range(self, w: Range<TimeIndexEntry>) -> TimeIndexRef<'a> {
        match self {
            TimeIndexRef::Ref(t) => TimeIndexRef::Range(t.range_inner(w)),
            TimeIndexRef::Range(t) => TimeIndexRef::Range(t.into_range(w)),
            #[cfg(feature = "arrow")]
            TimeIndexRef::External(t) => TimeIndexRef::External(t.into_range(w)),
        }
    }
    fn into_iter(self) -> impl Iterator<Item = TimeIndexEntry> + Send + 'a {
        match self {
            TimeIndexRef::Ref(t) => t.iter(),
            TimeIndexRef::Range(t) => t.into_iter().into_dyn_boxed(),
            #[cfg(feature = "arrow")]
            TimeIndexRef::External(t) => t.into_iter().into_dyn_boxed(),
        }
    }
}

pub trait EdgeStorageIntoOps {
    fn into_layers(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send;

    fn into_exploded(
        self,
        layer_ids: LayerIds,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send;

    fn into_exploded_window(
        self,
        layer_ids: LayerIds,
        w: Range<TimeIndexEntry>,
        eref: EdgeRef,
    ) -> impl Iterator<Item = EdgeRef> + Send;
}

pub trait EdgeStorageOps<'a>: Copy + Sized + Send + Sync + 'a {
    fn in_ref(self) -> EdgeRef;

    fn out_ref(self) -> EdgeRef;
    fn active(self, layer_ids: &LayerIds, w: Range<i64>) -> bool;
    fn has_layer(self, layer_ids: &LayerIds) -> bool;
    fn src(self) -> VID;
    fn dst(self) -> VID;

    fn layer_ids_iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = usize> + 'a;

    fn layer_ids_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = usize> + 'a;

    fn additions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.additions(id)))
    }

    fn additions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.additions(id)))
    }
    fn deletions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.deletions(id)))
    }

    fn deletions_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.deletions(id)))
    }

    fn updates_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.additions(id), self.deletions(id)))
    }

    fn updates_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.additions(id), self.deletions(id)))
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a>;
    fn deletions(self, layer_id: usize) -> TimeIndexRef<'a>;

    fn has_temporal_prop(self, layer_ids: &'a LayerIds, prop_id: usize) -> bool {
        self.layer_ids_par_iter(layer_ids)
            .any(move |id| !self.temporal_prop_layer(id, prop_id).is_empty())
    }

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + Sync + 'a;

    fn temporal_prop_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.temporal_prop_layer(id, prop_id)))
    }

    fn temporal_prop_par_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl ParallelIterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.temporal_prop_layer(id, prop_id)))
    }
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
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => true,
            LayerIds::One(id) => self.has_layer_inner(*id),
            LayerIds::Multiple(ids) => ids.par_iter().any(|id| self.has_layer_inner(*id)),
        }
    }

    fn src(self) -> VID {
        self.src
    }

    fn dst(self) -> VID {
        self.dst
    }

    fn layer_ids_iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = usize> + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(std::iter::empty()),
            LayerIds::All => LayerVariants::All(
                (0..self.internal_num_layers()).filter(|&l| self.has_layer_inner(l)),
            ),
            LayerIds::One(id) => {
                LayerVariants::One(self.has_layer_inner(*id).then_some(*id).into_iter())
            }
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(ids.iter().copied().filter(|&id| self.has_layer_inner(id)))
            }
        }
    }

    fn layer_ids_par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = usize> + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::All => LayerVariants::All(
                (0..self.internal_num_layers())
                    .into_par_iter()
                    .filter(|&l| self.has_layer_inner(l)),
            ),
            LayerIds::One(id) => {
                LayerVariants::One(self.has_layer_inner(*id).then_some(*id).into_par_iter())
            }
            LayerIds::Multiple(ids) => LayerVariants::Multiple(
                ids.par_iter()
                    .copied()
                    .filter(|&id| self.has_layer_inner(id)),
            ),
        }
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a> {
        TimeIndexRef::Ref(self.additions.get(layer_id).unwrap_or(&TimeIndex::Empty))
    }

    fn deletions(self, layer_id: usize) -> TimeIndexRef<'a> {
        TimeIndexRef::Ref(self.deletions.get(layer_id).unwrap_or(&TimeIndex::Empty))
    }

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + 'a {
        self.temporal_prop_layer_inner(layer_id, prop_id)
            .unwrap_or(&TProp::Empty)
    }
}
