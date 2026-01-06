use parking_lot::{RwLockReadGuard, RwLockWriteGuard, lock_api::ArcRwLockReadGuard};
use raphtory_api::core::entities::properties::{meta::Meta, prop::Prop, tprop::TPropOps};
use raphtory_core::{
    entities::{EID, LayerIds, VID},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use rayon::iter::ParallelIterator;
use std::{
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicU32},
};

use crate::{LocalPOS, error::StorageError, segments::edge::segment::MemEdgeSegment, wal::LSN};

pub trait EdgeSegmentOps: Send + Sync + std::fmt::Debug + 'static {
    type Extension;

    type Entry<'a>: EdgeEntryOps<'a>
    where
        Self: 'a;

    type ArcLockedSegment: LockedESegment;

    fn latest(&self) -> Option<TimeIndexEntry>;
    fn earliest(&self) -> Option<TimeIndexEntry>;

    fn t_len(&self) -> usize;
    fn num_layers(&self) -> usize;
    fn layer_count(&self, layer_id: usize) -> u32;

    fn load(
        page_id: usize,
        max_page_len: u32,
        meta: Arc<Meta>,
        path: impl AsRef<Path>,
        ext: Self::Extension,
    ) -> Result<Self, StorageError>
    where
        Self: Sized;

    fn new(page_id: usize, meta: Arc<Meta>, path: Option<PathBuf>, ext: Self::Extension) -> Self;

    fn segment_id(&self) -> usize;

    fn edges_counter(&self) -> &AtomicU32;

    fn num_edges(&self) -> u32 {
        self.edges_counter()
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn head(&self) -> RwLockReadGuard<'_, MemEdgeSegment>;

    fn head_arc(&self) -> ArcRwLockReadGuard<parking_lot::RawRwLock, MemEdgeSegment>;

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemEdgeSegment>;

    fn try_head_mut(&self) -> Option<RwLockWriteGuard<'_, MemEdgeSegment>>;

    fn set_dirty(&self, dirty: bool);

    /// notify that an edge was added (might need to write to disk)
    fn notify_write(
        &self,
        head_lock: impl DerefMut<Target = MemEdgeSegment>,
    ) -> Result<(), StorageError>;

    fn increment_num_edges(&self) -> u32;

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

    fn entry<'a>(&'a self, edge_pos: LocalPOS) -> Self::Entry<'a>;

    fn layer_entry<'a>(
        &'a self,
        edge_pos: LocalPOS,
        layer_id: usize,
        locked_head: Option<parking_lot::RwLockReadGuard<'a, MemEdgeSegment>>,
    ) -> Option<Self::Entry<'a>>;

    fn locked(self: &Arc<Self>) -> Self::ArcLockedSegment;

    fn vacuum(
        &self,
        locked_head: impl DerefMut<Target = MemEdgeSegment>,
    ) -> Result<(), StorageError>;

    /// Returns the latest lsn for the immutable part of this segment.
    fn immut_lsn(&self) -> LSN;
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
    ) -> impl ParallelIterator<Item = Self::EntryRef<'a>> + Sync + 'a;
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
    type Additions: TimeIndexOps<'a, IndexType = TimeIndexEntry>;
    type Deletions: TimeIndexOps<'a, IndexType = TimeIndexEntry>;
    type TProps: TPropOps<'a>;

    fn edge(self, layer_id: usize) -> Option<(VID, VID)>;

    fn has_layer_inner(self, layer_id: usize) -> bool {
        self.edge(layer_id).is_some()
    }

    fn internal_num_layers(self) -> usize;

    fn layer_additions(self, layer_id: usize) -> Self::Additions;
    fn layer_deletions(self, layer_id: usize) -> Self::Deletions;

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop>;

    fn layer_t_prop(self, layer_id: usize, prop_id: usize) -> Self::TProps;

    fn src(&self) -> Option<VID>;

    fn dst(&self) -> Option<VID>;

    fn edge_id(&self) -> EID;
}
