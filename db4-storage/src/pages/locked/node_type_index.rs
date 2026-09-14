use crate::{
    api::node_type_index::NodeTypeIndexOps, error::StorageError,
    segments::node_type_index::MemNodeTypeIndex,
};
use parking_lot::{ArcRwLockWriteGuard, RawRwLock};
use std::sync::Arc;

pub struct WriteLockedNodeTypeIndex<NTI> {
    head: ArcRwLockWriteGuard<RawRwLock, MemNodeTypeIndex>,
    index: Arc<NTI>,
}

impl<NTI: NodeTypeIndexOps> WriteLockedNodeTypeIndex<NTI> {
    pub fn new(head: ArcRwLockWriteGuard<RawRwLock, MemNodeTypeIndex>, index: Arc<NTI>) -> Self {
        Self { head, index }
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        Ok(())
    }
}
