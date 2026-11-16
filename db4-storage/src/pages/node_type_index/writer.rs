use crate::{api::node_type_index::NodeTypeIndexOps, segments::node_type_index::MemNodeTypeIndex};
use raphtory_api::{ArcRwLockWriteGuard, core::entities::VID};

pub struct NodeTypeIndexWriter<NTI: NodeTypeIndexOps> {
    head: ArcRwLockWriteGuard<MemNodeTypeIndex>,
    index: NTI,
    initial_est_size: usize,
}

impl<NTI: NodeTypeIndexOps> NodeTypeIndexWriter<NTI> {
    pub fn new(head: ArcRwLockWriteGuard<MemNodeTypeIndex>, index: NTI) -> Self {
        let initial_est_size = index.est_size();
        Self {
            head,
            index,
            initial_est_size,
        }
    }

    pub fn insert(&mut self, type_id: usize, vid: VID) {
        self.head.insert(type_id, vid);
    }
}
