#[cfg(feature = "storage")]
use crate::db::api::storage::variants::storage_variants3::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::nodes_ref::DiskNodesRef;

#[cfg(not(feature = "storage"))]
use either::Either;

use crate::{
    core::{
        entities::{nodes::node_store::NodeStore, VID},
        storage::ReadLockedStorage,
    },
    db::api::storage::nodes::node_ref::NodeStorageRef,
};
use rayon::iter::ParallelIterator;

use super::{node_entry::NodeStorageEntry, unlocked::UnlockedNodes};

#[derive(Debug)]
pub enum NodesStorageRef<'a> {
    Mem(&'a ReadLockedStorage<NodeStore, VID>),
    Unlocked(UnlockedNodes<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskNodesRef<'a>),
}

#[cfg(feature = "storage")]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorageRef::Mem($pattern) => StorageVariants::Mem($result),
            NodesStorageRef::Unlocked($pattern) => StorageVariants::Unlocked($result),
            NodesStorageRef::Disk($pattern) => StorageVariants::Disk($result),
        }
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorageRef::Mem($pattern) => Either::Left($result),
            NodesStorageRef::Unlocked($pattern) => Either::Right($result),
        }
    };
}

impl<'a> NodesStorageRef<'a> {
    pub fn node(&self, vid: VID) -> NodeStorageEntry<'a> {
        match self {
            NodesStorageRef::Mem(store) => NodeStorageEntry::Mem(store.get(vid)),
            NodesStorageRef::Unlocked(store) => NodeStorageEntry::Unlocked(store.node(vid)),
            #[cfg(feature = "storage")]
            NodesStorageRef::Disk(store) => NodeStorageEntry::Disk(store.node(vid)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NodesStorageRef::Mem(store) => store.len(),
            NodesStorageRef::Unlocked(store) => store.len(),
            #[cfg(feature = "storage")]
            NodesStorageRef::Disk(store) => store.len(),
        }
    }
    pub fn par_iter(self) -> impl ParallelIterator<Item = NodeStorageEntry<'a>> {
        for_all_variants!(self, nodes => nodes.par_iter().map(|n| n.into()))
    }

    pub fn iter(self) -> impl Iterator<Item = NodeStorageEntry<'a>> {
        for_all_variants!(self, nodes => nodes.iter().map(|n| n.into()))
    }
}
