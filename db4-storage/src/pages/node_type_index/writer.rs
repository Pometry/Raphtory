use crate::{
    api::node_type_index::NodeTypeIndexOps, error::StorageError,
    segments::node_type_index::MemNodeTypeIndex,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::{ArcRwLockReadGuard, core::entities::VID};
use std::{
    ops::Deref,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
pub struct NodeTypeIndexWriter<'a, M: Deref<Target = MemNodeTypeIndex>, NTI: NodeTypeIndexOps> {
    head: M,
    index: &'a NTI,
    /// Bytes added to `head` by this writer.
    est_size_delta: AtomicUsize,
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
    pub fn new_shared(index: &'a NTI) -> Result<Self, StorageError> {
        index.notify_write(None::<RwLockWriteGuard<'_, MemNodeTypeIndex>>)?;
        let head = index.head_shared_arc();
        Ok(Self::from_head(head, index))
    }
}

impl<'a, NTI: NodeTypeIndexOps> NodeTypeIndexWriter<'a, &'a mut MemNodeTypeIndex, NTI> {
    /// Create a new index writer with an exclusive write lock on the head.
    ///
    /// May flush dirty data to disk before returning a writer.
    pub(crate) fn new_exclusive(
        head: &'a mut MemNodeTypeIndex,
        index: &'a NTI,
    ) -> Result<Self, StorageError> {
        index.notify_write(Some(&mut *head))?;
        Ok(Self::from_head(head, index))
    }
}

impl<'a, M: Deref<Target = MemNodeTypeIndex>, NTI: NodeTypeIndexOps>
    NodeTypeIndexWriter<'a, M, NTI>
{
    pub fn insert(&self, type_id: usize, vid: VID) {
        let delta = self.head.insert(type_id, vid);
        self.est_size_delta.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn insert_batch(&self, type_id: usize, vids: impl IntoIterator<Item = VID>) {
        let delta = self.head.insert_batch(type_id, vids);
        self.est_size_delta.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn set_lsn(&self, lsn: u64) {
        self.head.set_lsn(lsn);
    }

    fn from_head(head: M, index: &'a NTI) -> Self {
        Self {
            head,
            index,
            est_size_delta: AtomicUsize::new(0),
        }
    }
}

impl<M: Deref<Target = MemNodeTypeIndex>, NTI: NodeTypeIndexOps> Drop
    for NodeTypeIndexWriter<'_, M, NTI>
{
    fn drop(&mut self) {
        let delta = self.est_size_delta.load(Ordering::Relaxed);
        self.head.increment_global_memory_tracker(delta);
        self.index.increment_est_size_cached(delta);
        self.index.set_dirty(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        persist::{
            config::BaseConfig,
            strategy::{NoOpStrategy, PersistenceStrategy},
        },
        segments::node_type_index::NodeTypeIndexView,
    };
    use std::sync::{Arc, atomic::Ordering};

    #[test]
    fn concurrent_shared_writers_update_est_size() {
        let ext = NoOpStrategy::new(None, BaseConfig::default()).unwrap();
        let tracker = Arc::clone(ext.memory_tracker());
        let index = Arc::new(NodeTypeIndexView::new(None, ext).unwrap());
        const N: usize = 10;

        std::thread::scope(|scope| {
            for i in 0..N {
                let index = Arc::clone(&index);
                scope.spawn(move || {
                    let writer = NodeTypeIndexWriter::new_shared(index.as_ref()).unwrap();
                    writer.insert(1, VID(i));
                });
            }
        });

        let expected = N * MemNodeTypeIndex::ESTIMATED_ENTRY_SIZE;
        assert_eq!(index.head_shared_arc().num_entries(), N);
        assert_eq!(index.head_shared_arc().est_size(), expected);
        assert_eq!(index.est_size_cached(), expected);
        assert_eq!(tracker.load(Ordering::Relaxed), expected);
    }
}
