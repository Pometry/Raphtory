use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use parking_lot::{RwLockReadGuard, RwLockWriteGuard, lock_api::ArcRwLockReadGuard};
use raphtory_api::core::entities::properties::{meta::Meta, prop::Prop, tprop::TPropOps};
use raphtory_core::{
    entities::{LayerIds, VID},
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

pub trait EdgeEntryOps<'a> {
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

    fn additions(self, layer_ids: &'a LayerIds) -> Self::Additions;

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop>;

    fn t_prop(self, layer_id: &'a LayerIds, prop_id: usize) -> Self::TProps;
}
