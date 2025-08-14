use std::sync::Arc;

use super::node_ref::NodeStorageRef;
use crate::graph::nodes::nodes_ref::NodesStorageEntry;
use raphtory_api::core::entities::VID;
use storage::{Extension, ReadLockedNodes};

#[cfg(feature = "storage")]
use crate::disk::storage_interface::nodes::DiskNodesOwned;

#[repr(transparent)]
pub struct NodesStorage {
    storage: Arc<ReadLockedNodes<Extension>>,
}

impl NodesStorage {
    pub fn new(storage: Arc<ReadLockedNodes<Extension>>) -> Self {
        Self { storage }
    }

    #[inline]
    pub fn as_ref(&self) -> NodesStorageEntry {
        NodesStorageEntry::Mem(self.storage.as_ref())
    }

    #[inline]
    pub fn node_entry(&self, vid: VID) -> NodeStorageRef {
        self.storage.node_ref(vid)
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }
}
