mod index;

use crate::{
    api::node_type_index::NodeTypeIndexOps, error::StorageError, loop_lock_write,
    pages::locked::node_type_index::WriteLockedNodeTypeIndex,
    persist::strategy::PersistenceStrategy,
};
use ahash::RandomState;
use indexmap::IndexSet;
use parking_lot::{
    RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard, lock_api::ArcRwLockWriteGuard,
};
use raphtory_core::entities::VID;
use std::{
    ops::DerefMut,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

pub use index::MemNodeTypeIndex;

/// Fully in-memory node type index.
#[derive(Debug)]
pub struct NodeTypeIndexView<P: PersistenceStrategy> {
    head: Arc<RwLock<MemNodeTypeIndex>>,
    est_size: AtomicUsize,
    is_dirty: AtomicBool,
    _persistence: P,
}

impl<P: PersistenceStrategy> NodeTypeIndexOps for NodeTypeIndexView<P> {
    type Extension = P;

    fn new(_path: Option<&Path>, ext: Self::Extension) -> Self {
        Self {
            head: Arc::new(RwLock::new(MemNodeTypeIndex::new())),
            est_size: AtomicUsize::new(0),
            is_dirty: AtomicBool::new(false),
            _persistence: ext,
        }
    }

    fn load(_path: impl AsRef<Path>, _ext: Self::Extension) -> Result<Self, StorageError> {
        Err(StorageError::GenericFailure(
            "load not supported".to_string(),
        ))
    }

    fn head_shared(&self) -> RwLockReadGuard<'_, MemNodeTypeIndex> {
        self.head.read_recursive()
    }

    fn head_exclusive(&self) -> RwLockWriteGuard<'_, MemNodeTypeIndex> {
        self.head.write()
    }

    fn head_exclusive_arc(&self) -> ArcRwLockWriteGuard<RawRwLock, MemNodeTypeIndex> {
        self.head.write_arc()
    }

    fn nodes_of_type(&self, type_ids: &[usize]) -> IndexSet<VID, RandomState> {
        self.head_shared()
            .nodes_of_type(type_ids)
            .into_iter()
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.head_shared().is_empty()
    }

    fn est_size(&self) -> usize {
        self.est_size.load(Ordering::Relaxed)
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Relaxed)
    }

    fn set_dirty(&self, dirty: bool) {
        self.is_dirty.store(dirty, Ordering::Release);
    }

    fn notify_write(&self) {
        self.est_size
            .store(self.head_shared().est_size(), Ordering::Relaxed);
    }

    fn write_locked(self: &Arc<Self>) -> WriteLockedNodeTypeIndex<Self> {
        let head = self.head.write_arc();
        let index = self.clone();

        WriteLockedNodeTypeIndex::new(head, index)
    }

    fn flush(
        &self,
        _head_exclusive: impl DerefMut<Target = MemNodeTypeIndex>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn copy_to(&self, _dst: &Path) -> Result<(), StorageError> {
        Ok(())
    }
}
