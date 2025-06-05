use super::node_ref::NodeStorageRef;
use crate::graph::nodes::nodes_ref::NodesStorageEntry;
use raphtory_api::core::entities::VID;
use raphtory_core::storage::ReadLockedStorage;
use std::sync::Arc;

#[cfg(feature = "storage")]
use crate::disk::storage_interface::nodes::DiskNodesOwned;

pub enum NodesStorage {
    Mem(Arc<ReadLockedStorage>),
    #[cfg(feature = "storage")]
    Disk(DiskNodesOwned),
}

impl NodesStorage {
    #[inline]
    pub fn as_ref(&self) -> NodesStorageEntry {
        match self {
            NodesStorage::Mem(storage) => NodesStorageEntry::Mem(storage),
            #[cfg(feature = "storage")]
            NodesStorage::Disk(storage) => NodesStorageEntry::Disk(storage.as_ref()),
        }
    }

    #[inline]
    pub fn node_entry(&self, vid: VID) -> NodeStorageRef {
        match self {
            NodesStorage::Mem(storage) => NodeStorageRef::Mem(storage.get_entry(vid)),
            #[cfg(feature = "storage")]
            NodesStorage::Disk(storage) => NodeStorageRef::Disk(storage.node(vid)),
        }
    }

    pub fn len(&self) -> usize {
        self.as_ref().len()
    }
}
