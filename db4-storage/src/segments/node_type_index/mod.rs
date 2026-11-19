mod index;

use crate::{
    api::node_type_index::NodeTypeIndexOps, error::StorageError,
    pages::locked::node_type_index::WriteLockedNodeTypeIndex,
    persist::strategy::PersistenceStrategy,
};
use parking_lot::{RwLock, RwLockWriteGuard};
use raphtory_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard};
use std::{
    ops::DerefMut,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

pub use index::{FrozenNodeTypeEntry, MemNodeTypeEntry, MemNodeTypeIndex};

/// Fully in-memory node type index.
#[derive(Debug)]
pub struct NodeTypeIndexView<P: PersistenceStrategy> {
    head: Arc<RwLock<MemNodeTypeIndex>>,

    /// Caches `head.est_size`.
    /// Used to get memory usage without acquiring the `head` lock.
    est_size_cached: AtomicUsize,
    is_dirty: AtomicBool,
    _persistence: P,
}

impl<P: PersistenceStrategy> NodeTypeIndexOps for NodeTypeIndexView<P> {
    type Extension = P;
    type Entry = MemNodeTypeEntry;

    fn new(_path: Option<&Path>, ext: Self::Extension) -> Result<Self, StorageError> {
        Ok(Self {
            head: Arc::new(RwLock::new(MemNodeTypeIndex::new())),
            est_size_cached: AtomicUsize::new(0),
            is_dirty: AtomicBool::new(false),
            _persistence: ext,
        })
    }

    fn load(_path: impl AsRef<Path>, _ext: Self::Extension) -> Result<Self, StorageError> {
        Err(StorageError::GenericFailure(
            "load not supported".to_string(),
        ))
    }

    fn head_shared_arc(&self) -> ArcRwLockReadGuard<MemNodeTypeIndex> {
        self.head.read_arc_recursive()
    }

    fn head_exclusive(&self) -> RwLockWriteGuard<'_, MemNodeTypeIndex> {
        self.head.write()
    }

    fn head_exclusive_arc(&self) -> ArcRwLockWriteGuard<MemNodeTypeIndex> {
        self.head.write_arc()
    }

    fn entry(&self, type_ids: &[usize]) -> Self::Entry {
        MemNodeTypeEntry::with_types(self.head_shared_arc(), type_ids)
    }

    fn is_empty(&self) -> bool {
        self.head_shared_arc().is_empty()
    }

    /// The cached memory size of `head` in bytes.
    fn est_size_cached(&self) -> usize {
        self.est_size_cached.load(Ordering::Relaxed)
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Relaxed)
    }

    fn set_dirty(&self, dirty: bool) {
        self.is_dirty.store(dirty, Ordering::Release);
    }

    fn notify_write(&self, head: Option<impl DerefMut<Target = MemNodeTypeIndex>>) {
        let est_size = match &head {
            Some(head) => head.est_size(),
            None => self.head_shared_arc().est_size(),
        };

        self.est_size_cached.store(est_size, Ordering::Relaxed);
    }

    fn write_locked(self: &Arc<Self>) -> WriteLockedNodeTypeIndex<Self> {
        let head = self.head_exclusive_arc();
        let index = self.clone();

        WriteLockedNodeTypeIndex::new(head, index)
    }

    fn flush(&self, _head: impl DerefMut<Target = MemNodeTypeIndex>) -> Result<(), StorageError> {
        Ok(())
    }

    fn copy_to(&self, _dst: &Path) -> Result<(), StorageError> {
        Ok(())
    }
}
