use crate::{api::node_type_index::NodeTypeIndexOps, segments::node_type_index::MemNodeTypeIndex};
use raphtory_api::ArcRwLockWriteGuard;

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
}
