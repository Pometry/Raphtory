use iter_enum::{DoubleEndedIterator, ExactSizeIterator, FusedIterator, Iterator};
use raphtory_api::core::{
    entities::{
        edges::edge_ref::{Dir, EdgeRef},
        properties::{prop::Prop, tprop::TPropOps},
        LayerIds, LayerVariants, EID, VID,
    },
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use raphtory_core::storage::timeindex::{TimeIndex, TimeIndexWindow};
use std::ops::Range;
use storage::api::edges::EdgeRefOps;

#[derive(Clone)]
pub enum TimeIndexRef<'a> {
    Ref(&'a TimeIndex<TimeIndexEntry>),
    Range(TimeIndexWindow<'a, TimeIndexEntry, TimeIndex<TimeIndexEntry>>),
}

#[derive(Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator, Debug, Clone)]
pub enum TimeIndexRefVariants<Ref, Range> {
    Ref(Ref),
    Range(Range),
}

impl<'a> TimeIndexOps<'a> for TimeIndexRef<'a> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    #[inline(always)]
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        match self {
            TimeIndexRef::Ref(t) => t.active(w),
            TimeIndexRef::Range(t) => t.active(w),
        }
    }

    fn range(&self, w: Range<TimeIndexEntry>) -> Self {
        match self {
            TimeIndexRef::Ref(t) => TimeIndexRef::Range(t.range(w)),
            TimeIndexRef::Range(t) => TimeIndexRef::Range(t.range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexRef::Ref(t) => t.first(),
            TimeIndexRef::Range(t) => t.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            TimeIndexRef::Ref(t) => t.last(),
            TimeIndexRef::Range(t) => t.last(),
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self {
            TimeIndexRef::Ref(t) => TimeIndexRefVariants::Ref(t.iter()),
            TimeIndexRef::Range(t) => TimeIndexRefVariants::Range(t.iter()),
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self {
            TimeIndexRef::Ref(t) => TimeIndexRefVariants::Ref(t.iter_rev()),
            TimeIndexRef::Range(t) => TimeIndexRefVariants::Range(t.iter_rev()),
        }
    }

    fn len(&self) -> usize {
        match self {
            TimeIndexRef::Ref(ts) => ts.len(),
            TimeIndexRef::Range(ts) => ts.len(),
        }
    }
}

pub trait EdgeStorageOps<'a>: Copy + Sized + Send + Sync + 'a {
    fn edge_ref(self, dir: Dir) -> EdgeRef {
        EdgeRef::new(self.eid(), self.src(), self.dst(), dir)
    }

    fn out_ref(self) -> EdgeRef {
        self.edge_ref(Dir::Out)
    }

    /// Check if the edge was added in any of the layers during the time interval
    fn added(self, layer_ids: &LayerIds, w: Range<i64>) -> bool;

    /// Check if the edge was deleted in any of the layers during the time interval
    fn deleted(self, layer_ids: &'a LayerIds, w: Range<i64>) -> bool {
        self.deletions_iter(layer_ids)
            .any(|(_, deletions)| deletions.active_t(w.clone()))
    }

    fn has_layer(self, layer_ids: &LayerIds) -> bool;
    fn src(self) -> VID;
    fn dst(self) -> VID;
    fn eid(self) -> EID;

    fn layer_ids_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = usize> + Send + Sync + 'a;

    fn additions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, storage::EdgeAdditions<'a>)> + Send + Sync + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.additions(id)))
    }

    fn deletions_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = (usize, storage::EdgeDeletions<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.deletions(id)))
    }

    fn updates_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<
        Item = (
            usize,
            storage::EdgeAdditions<'a>,
            storage::EdgeDeletions<'a>,
        ),
    > + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.additions(id), self.deletions(id)))
    }

    fn additions(self, layer_id: usize) -> storage::EdgeAdditions<'a>;

    fn deletions(self, layer_id: usize) -> storage::EdgeDeletions<'a>;

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + 'a;

    fn temporal_prop_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> + 'a {
        self.layer_ids_iter(layer_ids)
            .map(move |id| (id, self.temporal_prop_layer(id, prop_id)))
    }

    fn metadata_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop>;

    fn metadata_iter(
        self,
        layer_ids: &'a LayerIds,
        prop_id: usize,
    ) -> impl Iterator<Item = (usize, Prop)> + 'a {
        self.layer_ids_iter(layer_ids)
            .filter_map(move |id| Some((id, self.metadata_layer(id, prop_id)?)))
    }
}

impl<'a> EdgeStorageOps<'a> for storage::EdgeEntryRef<'a> {
    fn added(self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self
                .additions_iter(&LayerIds::All)
                .any(|(_, t_index)| t_index.active_t(w.clone())),
            LayerIds::One(l_id) => self.layer_additions(*l_id).active_t(w),
            LayerIds::Multiple(layers) => layers
                .iter()
                .any(|l_id| self.added(&LayerIds::One(l_id), w.clone())),
        }
    }

    fn has_layer(self, layer_ids: &LayerIds) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self.edge(0).is_some(),
            LayerIds::One(id) => self.edge(*id).is_some(),
            LayerIds::Multiple(ids) => self.has_layers(ids),
        }
    }

    fn src(self) -> VID {
        EdgeRefOps::src(&self).expect("EdgeRefOps::src should not return None")
    }

    fn dst(self) -> VID {
        EdgeRefOps::dst(&self).expect("EdgeRefOps::dst should not return None")
    }

    fn eid(self) -> EID {
        EdgeRefOps::edge_id(&self)
    }

    fn layer_ids_iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = usize> + 'a {
        match layer_ids {
            LayerIds::None => LayerVariants::None(std::iter::empty()),
            LayerIds::All => LayerVariants::All(
                (1..self.internal_num_layers()).filter(move |&l| self.has_layer_inner(l)),
            ),
            LayerIds::One(id) => {
                LayerVariants::One(self.has_layer_inner(*id).then_some(*id).into_iter())
            }
            LayerIds::Multiple(ids) => {
                LayerVariants::Multiple(ids.iter().filter(move |&id| self.has_layer_inner(id)))
            }
        }
    }

    fn additions(self, layer_id: usize) -> storage::EdgeAdditions<'a> {
        EdgeRefOps::layer_additions(self, layer_id)
    }

    fn deletions(self, layer_id: usize) -> storage::EdgeDeletions<'a> {
        EdgeRefOps::layer_deletions(self, layer_id)
    }

    #[inline(always)]
    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> impl TPropOps<'a> + 'a {
        EdgeRefOps::layer_t_prop(self, layer_id, prop_id)
    }

    fn metadata_layer(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        EdgeRefOps::c_prop(self, layer_id, prop_id)
    }
}
