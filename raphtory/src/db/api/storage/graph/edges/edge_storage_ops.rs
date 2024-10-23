use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            properties::{props::Props, tprop::TProp},
            LayerIds, VID,
        },
        storage::{
            raw_edges::EdgeShard,
            timeindex::{TimeIndex, TimeIndexIntoOps, TimeIndexOps, TimeIndexWindow},
        },
        utils::iter::GenLockedIter,
        Prop,
    },
    db::api::{
        storage::graph::{
            tprop_storage_ops::TPropOps,
            variants::{layer_variants::LayerVariants, storage_variants3::StorageVariants},
        },
        view::IntoDynBoxed,
    },
};
use raphtory_api::core::{entities::EID, storage::timeindex::TimeIndexEntry};
use rayon::prelude::*;
use std::ops::Range;

#[cfg(feature = "storage")]
use pometry_storage::timestamps::TimeStamps;

pub enum TimeIndexRef<'a> {
    Ref(&'a TimeIndex<TimeIndexEntry>),
    Range(TimeIndexWindow<'a, TimeIndexEntry>),
    #[cfg(feature = "storage")]
    External(TimeStamps<'a, TimeIndexEntry>),
}

impl<'a> TimeIndexRef<'a> {
    pub fn len(&self) -> usize {
        match self {
            TimeIndexRef::Ref(ts) => ts.len(),
            TimeIndexRef::Range(ts) => ts.len(),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ts) => ts.len(),
        }
    }

    pub fn iter(self) -> impl Iterator<Item = TimeIndexEntry> + 'a {
        match self {
            TimeIndexRef::Ref(t) => StorageVariants::Mem(t.iter()),
            TimeIndexRef::Range(t) => {
                StorageVariants::Unlocked(GenLockedIter::from(t, |t| t.iter()))
            }
            #[cfg(feature = "storage")]
            TimeIndexRef::External(t) => {
                StorageVariants::Disk(GenLockedIter::from(t, |t| t.iter()))
            }
        }
    }
}

impl<'a> TimeIndexOps for TimeIndexRef<'a> {
    type IndexType = TimeIndexEntry;
    type RangeType<'b>
        = TimeIndexRef<'b>
    where
        Self: 'b;

    #[inline(always)]
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        match self {
            TimeIndexRef::Ref(t) => t.active(w),
            TimeIndexRef::Range(ref t) => t.active(w),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ref t) => t.active(w),
        }
    }

    fn range(&self, w: Range<TimeIndexEntry>) -> Self::RangeType<'_> {
        match self {
            TimeIndexRef::Ref(t) => TimeIndexRef::Range(t.range(w)),
            TimeIndexRef::Range(ref t) => TimeIndexRef::Range(t.range(w)),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ref t) => TimeIndexRef::External(t.range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexRef::Ref(t) => t.first(),
            TimeIndexRef::Range(ref t) => t.first(),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ref t) => t.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexRef::Ref(t) => t.last(),
            TimeIndexRef::Range(ref t) => t.last(),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ref t) => t.last(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
        match self {
            TimeIndexRef::Ref(t) => t.iter(),
            TimeIndexRef::Range(t) => t.iter(),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(ref t) => t.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            TimeIndexRef::Ref(ts) => ts.len(),
            TimeIndexRef::Range(ts) => ts.len(),
            #[cfg(feature = "storage")]
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
            #[cfg(feature = "storage")]
            TimeIndexRef::External(t) => TimeIndexRef::External(t.into_range(w)),
        }
    }
    fn into_iter(self) -> impl Iterator<Item = TimeIndexEntry> + Send {
        match self {
            TimeIndexRef::Ref(t) => t.iter(),
            TimeIndexRef::Range(t) => t.into_iter().into_dyn_boxed(),
            #[cfg(feature = "storage")]
            TimeIndexRef::External(t) => t.into_iter().into_dyn_boxed(),
        }
    }
}

pub trait EdgeStorageOps<'a>: Copy + Sized + Send + Sync + 'a {
    fn out_ref(self) -> EdgeRef;

    fn active(self, layer_ids: LayerIds, w: Range<i64>) -> bool;

    fn has_layer(self, layer_ids: LayerIds) -> bool;
    fn src(self) -> VID;
    fn dst(self) -> VID;

    fn layer_ids_iter(self, layer_ids: LayerIds) -> impl Iterator<Item = usize> + 'a;

    fn layer_ids_par_iter(self, layer_ids: LayerIds) -> impl ParallelIterator<Item = usize> + 'a;

    fn additions_iter(
        self,
        layer_ids: LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.additions(id)))
    }

    fn additions_par_iter(
        self,
        layer_ids: LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.additions(id)))
    }
    fn deletions_iter(
        self,
        layer_ids: LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.deletions(id)))
    }

    fn deletions_par_iter(
        self,
        layer_ids: LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.deletions(id)))
    }

    fn updates_iter(
        self,
        layer_ids: LayerIds,
    ) -> impl Iterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.additions(id), self.deletions(id)))
    }

    fn updates_par_iter(
        self,
        layer_ids: LayerIds,
    ) -> impl ParallelIterator<Item = (usize, TimeIndexRef<'a>, TimeIndexRef<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.additions(id), self.deletions(id)))
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a>;
    fn deletions(self, layer_id: usize) -> TimeIndexRef<'a>;

    fn has_temporal_prop(self, layer_ids: LayerIds, prop_id: usize) -> bool {
        self.layer_ids_par_iter(layer_ids)
            .any(move |id| !self.temporal_prop_layer(id, prop_id).is_empty())
    }

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + Sync + 'a;

    fn temporal_prop_iter(
        self,
        layer_ids: LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.temporal_prop_layer(id, prop_id)))
    }

    fn temporal_prop_par_iter(
        self,
        layer_ids: LayerIds,
        prop_id: usize,
    ) -> impl ParallelIterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.layer_ids_par_iter(layer_ids)
            .map(move |id| (id, self.temporal_prop_layer(id, prop_id)))
    }

    fn constant_prop_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop>;
}

#[derive(Clone, Copy, Debug)]
pub struct MemEdge<'a> {
    edges: &'a EdgeShard,
    offset: usize,
}

impl<'a> MemEdge<'a> {
    pub fn new(edges: &'a EdgeShard, offset: usize) -> Self {
        MemEdge { edges, offset }
    }

    pub fn edge_store(&self) -> &EdgeStore {
        self.edges.edge_store(self.offset)
    }

    #[inline]
    pub fn props(&self, layer_id: usize) -> Option<&Props> {
        self.edges
            .props(self.offset, layer_id)
            .and_then(|el| el.props())
    }

    pub fn eid(self) -> EID {
        self.edge_store().eid
    }

    pub fn as_edge_ref(&self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid(), self.src(), self.dst())
    }

    pub fn internal_num_layers(self) -> usize {
        self.edges.internal_num_layers()
    }

    fn get_additions(self, layer_id: usize) -> Option<&'a TimeIndex<TimeIndexEntry>> {
        self.edges.additions(self.offset, layer_id)
    }

    fn get_deletions(self, layer_id: usize) -> Option<&'a TimeIndex<TimeIndexEntry>> {
        self.edges.deletions(self.offset, layer_id)
    }

    pub fn has_layer_inner(self, layer_id: usize) -> bool {
        self.get_additions(layer_id)
            .filter(|t_index| !t_index.is_empty())
            .is_some()
            || self
                .get_deletions(layer_id)
                .filter(|t_index| !t_index.is_empty())
                .is_some()
    }

    pub fn temporal_prop_layer_inner(self, layer_id: usize, prop_id: usize) -> Option<&'a TProp> {
        let layer = self.edges.props(self.offset, layer_id)?;
        layer.temporal_property(prop_id)
    }
}

impl<'a> EdgeStorageOps<'a> for MemEdge<'a> {
    fn active(self, layer_ids: LayerIds, w: Range<i64>) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self
                .additions_iter(layer_ids)
                .any(|(_, t_index)| t_index.active_t(w.clone())),
            LayerIds::One(l_id) => self
                .get_additions(l_id)
                .filter(|a| a.active_t(w))
                .is_some(),
            LayerIds::Multiple(layers) => layers
                .iter()
                .any(|l_id| self.active(LayerIds::One(l_id), w.clone())),
        }
    }

    fn has_layer(self, layer_ids: LayerIds) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => true,
            LayerIds::One(id) => self.has_layer_inner(id),
            LayerIds::Multiple(ids) => ids.iter().any(|id| self.has_layer_inner(id)),
        }
    }

    fn src(self) -> VID {
        self.edge_store().src
    }

    fn dst(self) -> VID {
        self.edge_store().dst
    }

    fn out_ref(self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid(), self.src(), self.dst())
    }

    fn layer_ids_iter(self, layer_ids: LayerIds) -> impl Iterator<Item = usize> + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(std::iter::empty()),
            LayerIds::All => LayerVariants::All(
                (0..self.internal_num_layers()).filter(move |&l| self.has_layer_inner(l)),
            ),
            LayerIds::One(id) => {
                LayerVariants::One(self.has_layer_inner(id).then_some(id).into_iter())
            }
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(ids.iter().filter(move |&id| self.has_layer_inner(id)))
            }
        }
    }

    fn layer_ids_par_iter(self, layer_ids: LayerIds) -> impl ParallelIterator<Item = usize> + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(rayon::iter::empty()),
            LayerIds::All => LayerVariants::All(
                (0..self.internal_num_layers())
                    .into_par_iter()
                    .filter(move |&l| self.has_layer_inner(l)),
            ),
            LayerIds::One(id) => {
                LayerVariants::One(self.has_layer_inner(id).then_some(id).into_par_iter())
            }
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(ids.par_iter().filter(move |&id| self.has_layer_inner(id)))
            }
        }
    }

    fn additions(self, layer_id: usize) -> TimeIndexRef<'a> {
        TimeIndexRef::Ref(self.get_additions(layer_id).unwrap_or(&TimeIndex::Empty))
    }

    fn deletions(self, layer_id: usize) -> TimeIndexRef<'a> {
        TimeIndexRef::Ref(self.get_deletions(layer_id).unwrap_or(&TimeIndex::Empty))
    }

    #[inline(always)]
    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + 'a {
        self.temporal_prop_layer_inner(layer_id, prop_id)
            .unwrap_or(&TProp::Empty)
    }

    fn constant_prop_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.props(layer_id)
            .and_then(|props| props.const_prop(prop_id).cloned())
    }
}
