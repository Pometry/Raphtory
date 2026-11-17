use crate::{api::node_type_index::NodeTypeIndexOps, segments::node_type_index::MemNodeTypeIndex};
use raphtory_api::{ArcRwLockReadGuard, core::entities::VID};
use std::sync::Arc;

#[derive(Debug)]
pub struct NodeTypeIndexWriter<NTI: NodeTypeIndexOps> {
    /// The in-memory head of the node type index.
    ///
    /// Uses a read instead of a write guard since `MemNodeTypeIndex` has
    /// interior mutability.
    head: ArcRwLockReadGuard<MemNodeTypeIndex>,
    index: Arc<NTI>,
    initial_est_size: usize,
}

impl<NTI: NodeTypeIndexOps> NodeTypeIndexWriter<NTI> {
    pub fn new(head: ArcRwLockReadGuard<MemNodeTypeIndex>, index: Arc<NTI>) -> Self {
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

impl<NTI: NodeTypeIndexOps> Drop for NodeTypeIndexWriter<NTI> {
    fn drop(&mut self) {
        if self.head.est_size() != self.initial_est_size {
            self.index.set_dirty(true);
            self.index.notify_write();
        }
    }
}
