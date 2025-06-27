use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use parking_lot::{RwLockReadGuard, RwLockWriteGuard, lock_api::ArcRwLockReadGuard};
use raphtory_api::core::entities::properties::{meta::Meta, prop::Prop, tprop::TPropOps};
use raphtory_core::{
    entities::{LayerIds, EID, VID},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use rayon::iter::ParallelIterator;

use crate::{LocalPOS, error::DBV4Error, segments::edge::MemEdgeSegment};

pub trait EdgeSegmentOps: Send + Sync + std::fmt::Debug {
    type Extension;

    type Entry<'a>: EdgeEntryOps<'a>
    where
        Self: 'a;

    type ArcLockedSegment: LockedESegment;

    fn latest(&self) -> Option<TimeIndexEntry>;
    fn earliest(&self) -> Option<TimeIndexEntry>;

    fn t_len(&self) -> usize;
    fn num_layers(&self) -> usize;
    fn layer_count(&self, layer_id: usize) -> usize;

    fn load(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Result<Self, DBV4Error>
    where
        Self: Sized;

    fn new(
        page_id: usize,
        max_page_len: usize,
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Self;

    fn segment_id(&self) -> usize;

    fn num_edges(&self) -> usize;

    fn head(&self) -> RwLockReadGuard<MemEdgeSegment>;

    fn head_arc(&self) -> ArcRwLockReadGuard<parking_lot::RawRwLock, MemEdgeSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<MemEdgeSegment>;

    fn try_head_mut(&self) -> Option<RwLockWriteGuard<MemEdgeSegment>>;

    fn notify_write(
        &self,
        head_lock: impl DerefMut<Target = MemEdgeSegment>,
    ) -> Result<(), DBV4Error>;

    fn increment_num_edges(&self) -> usize;

    fn contains_edge(
        &self,
        edge_pos: LocalPOS,
        layer_id: usize,
        locked_head: impl Deref<Target = MemEdgeSegment>,
    ) -> bool;

    fn get_edge(
        &self,
        edge_pos: LocalPOS,
        layer_id: usize,
        locked_head: impl Deref<Target = MemEdgeSegment>,
    ) -> Option<(VID, VID)>;

    fn entry<'a, LP: Into<LocalPOS>>(&'a self, edge_pos: LP) -> Self::Entry<'a>;

    fn layer_entry<'a, LP: Into<LocalPOS>>(
        &'a self,
        edge_pos: LP,
        layer_id: usize,
    ) -> Option<Self::Entry<'a>> {
        let edge_pos = edge_pos.into();
        if self.head().contains_edge(edge_pos, layer_id) {
            Some(self.entry(edge_pos))
        } else {
            None
        }
    }

    fn locked(self: &Arc<Self>) -> Self::ArcLockedSegment;
}

pub trait LockedESegment: Send + Sync + std::fmt::Debug {
    type EntryRef<'a>: EdgeRefOps<'a>
    where
        Self: 'a;

    fn entry_ref<'a>(&'a self, edge_pos: impl Into<LocalPOS>) -> Self::EntryRef<'a>
    where
        Self: 'a;

    fn edge_iter<'a, 'b: 'a>(
        &'a self,
        layer_ids: &'b LayerIds,
    ) -> impl Iterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a;

    fn edge_par_iter<'a, 'b: 'a>(
        &'a self,
        layer_ids: &'b LayerIds,
    ) -> impl ParallelIterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a;
}

#[derive(Debug)]
pub struct ReadLockedES<ES: EdgeSegmentOps> {
    es: Arc<ES>,
    head: ES::ArcLockedSegment,
}

pub trait EdgeEntryOps<'a>: Send + Sync {
    type Ref<'b>: EdgeRefOps<'b>
    where
        'a: 'b,
        Self: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b;
}

pub trait EdgeRefOps<'a>: Copy + Clone + Send + Sync {
    type Additions: TimeIndexOps<'a>;
    type TProps: TPropOps<'a>;

    fn edge(self, layer_id: usize) -> Option<(VID, VID)>;

    fn has_layer_inner(self, layer_id: usize) -> bool{
        self.edge(layer_id).is_some()
    }

    fn internal_num_layers(self) -> usize;

    fn additions(self, layer_ids: &'a LayerIds) -> Self::Additions;

    fn layer_additions(self, layer_id: usize) -> Self::Additions;

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop>;

    fn t_prop(self, layer_id: &'a LayerIds, prop_id: usize) -> Self::TProps;
    fn layer_t_prop(self, layer_id: usize, prop_id: usize) -> Self::TProps;

    fn src(&self) -> VID;

    fn dst(&self) -> VID;

    fn edge_id(&self) -> EID;
}
