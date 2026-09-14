use crate::{
    api::node_type_index::NodeTypeIndexOps, error::StorageError,
    segments::node_type_index::MemNodeTypeIndex,
};
use parking_lot::{ArcRwLockWriteGuard, RawRwLock};
use std::{ops::DerefMut, path::Path, sync::Arc};

pub struct WriteLockedNodeTypeIndex<NTI> {
    head: ArcRwLockWriteGuard<RawRwLock, MemNodeTypeIndex>,
    index: Arc<NTI>,
}

impl<NTI: NodeTypeIndexOps> WriteLockedNodeTypeIndex<NTI> {
    pub fn new(head: ArcRwLockWriteGuard<RawRwLock, MemNodeTypeIndex>, index: Arc<NTI>) -> Self {
        Self { head, index }
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        let head_lock = self.head.deref_mut();
        self.index.flush_locked(head_lock)
    }

    pub fn copy_to(&self, dst: &Path) -> Result<(), StorageError> {
        std::fs::create_dir_all(dst)?;

        self.index.copy_to(dst)
    }
}
