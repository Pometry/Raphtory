use crate::{api::node_type_index::NodeTypeIndexOps, segments::node_type_index::MemNodeTypeIndex};
use parking_lot::RwLockWriteGuard;
use raphtory_api::{ArcRwLockReadGuard, core::entities::VID};
use std::ops::Deref;

#[derive(Debug)]
pub struct NodeTypeIndexWriter<'a, M: Deref<Target = MemNodeTypeIndex>, NTI: NodeTypeIndexOps> {
    head: M,
    index: &'a NTI,
    initial_est_size: usize,
}

// NOTE:
// * Unlike node and edge writers which always acquire a write lock on head, the index writer
// may hold a read lock.
// * Calling notify_write on drop can then cause deadlocks since notify_write acquires a
// write lock on flush.
// * Hence why the index writer calls notify_write on creation before any locks are taken.
impl<'a, NTI: NodeTypeIndexOps> NodeTypeIndexWriter<'a, ArcRwLockReadGuard<MemNodeTypeIndex>, NTI> {
    /// Create a new index writer with a shared read lock on the head.
    ///
    /// May flush dirty data to disk before returning a writer.
    pub fn new_shared(index: &'a NTI) -> Self {
        index.notify_write(None::<RwLockWriteGuard<'_, MemNodeTypeIndex>>);
        let head = index.head_shared_arc();
        Self::from_head(head, index)
    }
}

impl<'a, NTI: NodeTypeIndexOps> NodeTypeIndexWriter<'a, &'a mut MemNodeTypeIndex, NTI> {
    /// Create a new index writer with an exclusive write lock on the head.
    ///
    /// May flush dirty data to disk before returning a writer.
    pub(crate) fn new_exclusive(head: &'a mut MemNodeTypeIndex, index: &'a NTI) -> Self {
        index.notify_write(Some(&mut *head));
        Self::from_head(head, index)
    }
}

impl<'a, M: Deref<Target = MemNodeTypeIndex>, NTI: NodeTypeIndexOps>
    NodeTypeIndexWriter<'a, M, NTI>
{
    pub fn insert(&self, type_id: usize, vid: VID) {
        self.head.insert(type_id, vid);
    }

    pub fn insert_batch(&self, type_id: usize, vids: impl IntoIterator<Item = VID>) {
        self.head.insert_batch(type_id, vids);
    }

    fn from_head(head: M, index: &'a NTI) -> Self {
        let initial_est_size = head.est_size();

        Self {
            head,
            index,
            initial_est_size,
        }
    }
}

impl<M: Deref<Target = MemNodeTypeIndex>, NTI: NodeTypeIndexOps> Drop
    for NodeTypeIndexWriter<'_, M, NTI>
{
    fn drop(&mut self) {
        let delta = self.head.est_size() - self.initial_est_size;
        self.head.increment_global_memory_tracker(delta);
        self.index.set_est_size_cached(self.head.est_size());
        self.index.set_dirty(true);
    }
}
