use crate::{
    api::node_type_index::NodeTypeIndexOps, error::StorageError,
    pages::node_type_index::writer::NodeTypeIndexWriter,
    segments::node_type_index::MemNodeTypeIndex,
};
use raphtory_api::ArcRwLockWriteGuard;
use std::{ops::DerefMut, sync::Arc};

pub struct WriteLockedNodeTypeIndex<NTI> {
    head: ArcRwLockWriteGuard<MemNodeTypeIndex>,
    index: Arc<NTI>,
}

impl<NTI: NodeTypeIndexOps> WriteLockedNodeTypeIndex<NTI> {
    pub fn new(head: ArcRwLockWriteGuard<MemNodeTypeIndex>, index: Arc<NTI>) -> Self {
        Self { head, index }
    }

    pub fn immut_lsn(&self) -> u64 {
        self.index.immut_lsn()
    }

    /// **Deadlock safety:** node type index must always be locked before node segments.
    pub fn writer(&mut self) -> NodeTypeIndexWriter<'_, &mut MemNodeTypeIndex, NTI> {
        NodeTypeIndexWriter::new_exclusive(self.head.deref_mut(), self.index.as_ref())
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        let head = self.head.deref_mut();
        self.index.flush(head)
    }
}
