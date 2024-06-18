#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::nodes::DiskNodesOwned;
use crate::{
    core::{
        entities::{graph::tgraph::InternalGraph, nodes::node_store::NodeStore, VID},
        storage::ReadLockedStorage,
    },
    db::api::storage::nodes::nodes_ref::NodesStorageRef,
};
use std::sync::Arc;

use super::{node_entry::NodeStorageEntry, unlocked::UnlockedNodes};

pub enum NodesStorage {
    Mem(Arc<ReadLockedStorage<NodeStore, VID>>),
    Unlocked(InternalGraph),
    #[cfg(feature = "storage")]
    Disk(DiskNodesOwned),
}

impl NodesStorage {
    pub fn as_ref(&self) -> NodesStorageRef {
        match self {
            NodesStorage::Mem(storage) => NodesStorageRef::Mem(storage),
            NodesStorage::Unlocked(storage) => NodesStorageRef::Unlocked(UnlockedNodes(storage)),
            #[cfg(feature = "storage")]
            NodesStorage::Disk(storage) => NodesStorageRef::Disk(storage.as_ref()),
        }
    }

    pub fn node_entry(&self, vid: VID) -> NodeStorageEntry {
        match self {
            NodesStorage::Mem(storage) => NodeStorageEntry::Mem(storage.get(vid)),
            NodesStorage::Unlocked(storage) => {
                NodeStorageEntry::Unlocked(UnlockedNodes(storage).node(vid))
            }
            #[cfg(feature = "storage")]
            NodesStorage::Disk(storage) => NodeStorageEntry::Disk(storage.node(vid)),
        }
    }
}
