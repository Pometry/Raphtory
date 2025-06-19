use super::node_ref::NodeStorageRef;
use crate::graph::variants::storage_variants3::StorageVariants3;
use raphtory_api::core::entities::VID;
use rayon::iter::ParallelIterator;
use storage::{Extension, ReadLockedNodes};

#[cfg(feature = "storage")]
use crate::disk::storage_interface::nodes_ref::DiskNodesRef;

#[derive(Debug)]
pub enum NodesStorageEntry<'a> {
    Mem(&'a ReadLockedNodes<Extension>),
    Unlocked(ReadLockedNodes<Extension>),
    #[cfg(feature = "storage")]
    Disk(DiskNodesRef<'a>),
}

macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorageEntry::Mem($pattern) => StorageVariants3::Mem($result),
            NodesStorageEntry::Unlocked($pattern) => StorageVariants3::Unlocked($result),
            #[cfg(feature = "storage")]
            NodesStorageEntry::Disk($pattern) => StorageVariants3::Disk($result),
        }
    };
}

impl<'a> NodesStorageEntry<'a> {
    pub fn node(&self, vid: VID) -> NodeStorageRef<'_> {
        match self {
            NodesStorageEntry::Mem(store) => NodeStorageRef::Mem(store.node_ref(vid)),
            NodesStorageEntry::Unlocked(store) => NodeStorageRef::Mem(store.node_ref(vid)),
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
