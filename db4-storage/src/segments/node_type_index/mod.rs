pub mod index;

use crate::{
    api::node_type_index::NodeTypeIndexOps, error::StorageError,
    persist::strategy::PersistenceStrategy, segments::node_type_index::index::MemNodeTypeIndex,
};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use raphtory_core::entities::VID;
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

/// Fully in-memory node type index.
#[derive(Debug)]
pub struct NodeTypeIndexView<P: PersistenceStrategy> {
    head: Arc<RwLock<MemNodeTypeIndex>>,
    est_size: AtomicUsize,
    is_dirty: AtomicBool,
    _persistent: P,
}

impl<P: PersistenceStrategy> NodeTypeIndexOps for NodeTypeIndexView<P> {
    type Extension = P;

    fn new(_path: Option<&Path>, ext: Self::Extension) -> Self {
        Self {
            head: Arc::new(RwLock::new(MemNodeTypeIndex::new())),
            est_size: AtomicUsize::new(0),
            is_dirty: AtomicBool::new(false),
            _persistent: ext,
        }
    }

    fn load(_path: impl AsRef<Path>, _ext: Self::Extension) -> Result<Self, StorageError> {
        Err(StorageError::GenericFailure(
            "load not supported".to_string(),
        ))
    }

    fn head(&self) -> RwLockReadGuard<'_, MemNodeTypeIndex> {
        self.head.read()
    }

    fn head_mut(&self) -> RwLockWriteGuard<'_, MemNodeTypeIndex> {
        self.head.write()
    }

    fn nodes_of_type(&self, type_ids: &[usize]) -> Vec<VID> {
        self.head().get(type_ids)
    }

    fn is_empty(&self) -> bool {
        self.head().is_empty()
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
            .store(self.head().est_size(), Ordering::Relaxed);
    }

    fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }
}
