use super::node_ref::NodeStorageRef;
use crate::{
    core::{
        entities::{nodes::node_store::NodeStore, VID},
        storage::ReadLockedStorage,
    },
    db::api::storage::graph::nodes::nodes_ref::NodesStorageEntry,
};
use std::sync::Arc;

#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::nodes::DiskNodesOwned;

pub enum NodesStorage {
    Mem(Arc<ReadLockedStorage<NodeStore, VID>>),
    #[cfg(feature = "storage")]
    Disk(DiskNodesOwned),
}

impl NodesStorage {
    pub fn as_ref(&self) -> NodesStorageEntry {
        match self {
            NodesStorage::Mem(storage) => NodesStorageEntry::Mem(storage),
            #[cfg(feature = "storage")]
            NodesStorage::Disk(storage) => NodesStorageEntry::Disk(storage.as_ref()),
        }
    }

    pub fn node_entry(&self, vid: VID) -> NodeStorageRef {
        match self {
            NodesStorage::Mem(storage) => NodeStorageRef::Mem(storage.get(vid)),
            #[cfg(feature = "storage")]
            NodesStorage::Disk(storage) => NodeStorageRef::Disk(storage.node(vid)),
        }
    }

    pub fn len(&self) -> usize {
        self.as_ref().len()
    }
}
