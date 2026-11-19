use crate::{api::node_type_index::NodeTypeIndexOps, segments::node_type_index::MemNodeTypeIndex};
use parking_lot::RwLockWriteGuard;
use raphtory_api::{ArcRwLockReadGuard, core::entities::VID};

#[derive(Debug)]
pub struct NodeTypeIndexWriter<'a, NTI: NodeTypeIndexOps> {
    /// The in-memory head of the node type index.
    ///
    /// Holds a read instead of a write guard since `MemNodeTypeIndex` has
    /// interior mutability.
    head: ArcRwLockReadGuard<MemNodeTypeIndex>,
    index: &'a NTI,
    initial_est_size: usize,
}

impl<'a, NTI: NodeTypeIndexOps> NodeTypeIndexWriter<'a, NTI> {
    pub fn new(head: ArcRwLockReadGuard<MemNodeTypeIndex>, index: &'a NTI) -> Self {
        let initial_est_size = head.est_size();

        Self {
            head,
            index,
            initial_est_size,
        }
    }

    pub fn insert(&self, type_id: usize, vid: VID) {
        self.head.insert(type_id, vid);
    }
}

impl<'a, NTI: NodeTypeIndexOps> Drop for NodeTypeIndexWriter<'a, NTI> {
    fn drop(&mut self) {
        let delta = self.head.est_size() - self.initial_est_size;
        self.head.increment_global_memory_tracker(delta);

        self.index.set_dirty(true);
        self.index
            .notify_write(None::<RwLockWriteGuard<'_, MemNodeTypeIndex>>);
    }
}
