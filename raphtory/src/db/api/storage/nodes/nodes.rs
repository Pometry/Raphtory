#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::nodes::DiskNodesOwned;
use crate::{
    core::{
        entities::{nodes::node_store::NodeStore, VID},
        storage::ReadLockedStorage,
    },
    db::api::storage::nodes::{node_ref::NodeStorageRef, nodes_ref::NodesStorageRef},
};
use std::sync::Arc;

pub enum NodesStorage {
    Mem(Arc<ReadLockedStorage<NodeStore, VID>>),
    #[cfg(feature = "storage")]
    Disk(DiskNodesOwned),
}

impl NodesStorage {
    pub fn as_ref(&self) -> NodesStorageRef {
        match self {
            NodesStorage::Mem(storage) => NodesStorageRef::Mem(storage),
            #[cfg(feature = "storage")]
            NodesStorage::Disk(storage) => NodesStorageRef::Disk(storage.as_ref()),
        }
    }

    pub fn node_ref(&self, vid: VID) -> NodeStorageRef {
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
