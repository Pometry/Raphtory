use super::node_ref::NodeStorageRef;
use crate::core::{entities::nodes::node_store::NodeStore, storage::ReadLockedStorage};
use raphtory_api::core::entities::VID;
use rayon::iter::ParallelIterator;

#[cfg(feature = "storage")]
use crate::db::api::storage::graph::variants::storage_variants3::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::nodes_ref::DiskNodesRef;

#[cfg(not(feature = "storage"))]
use either::Either;

#[derive(Debug)]
pub enum NodesStorageEntry<'a> {
    Mem(&'a ReadLockedStorage<NodeStore, VID>),
    Unlocked(ReadLockedStorage<NodeStore, VID>),
    #[cfg(feature = "storage")]
    Disk(DiskNodesRef<'a>),
}

#[cfg(feature = "storage")]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorageEntry::Mem($pattern) => StorageVariants::Mem($result),
            NodesStorageEntry::Unlocked($pattern) => StorageVariants::Unlocked($result),
            NodesStorageEntry::Disk($pattern) => StorageVariants::Disk($result),
        }
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorageEntry::Mem($pattern) => Either::Left($result),
            NodesStorageEntry::Unlocked($pattern) => Either::Right($result),
        }
    };
}

impl<'a> NodesStorageEntry<'a> {
    pub fn node(&self, vid: VID) -> NodeStorageRef<'_> {
        match self {
            NodesStorageEntry::Mem(store) => NodeStorageRef::Mem(store.get(vid)),
            NodesStorageEntry::Unlocked(store) => NodeStorageRef::Mem(store.get(vid)),
            #[cfg(feature = "storage")]
            NodesStorageEntry::Disk(store) => NodeStorageRef::Disk(store.node(vid)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NodesStorageEntry::Mem(store) => store.len(),
            NodesStorageEntry::Unlocked(store) => store.len(),
            #[cfg(feature = "storage")]
            NodesStorageEntry::Disk(store) => store.len(),
        }
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = NodeStorageRef<'_>> {
        for_all_variants!(self, nodes => nodes.par_iter().map(|n| n.into()))
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeStorageRef<'_>> {
        for_all_variants!(self, nodes => nodes.iter().map(|n| n.into()))
    }
}
