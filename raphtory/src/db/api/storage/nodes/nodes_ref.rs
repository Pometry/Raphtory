#[cfg(feature = "storage")]
use crate::db::api::storage::variants::storage_variants::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::nodes_ref::DiskNodesRef;
use crate::{
    core::{
        entities::{nodes::node_store::NodeStore, VID},
        storage::ReadLockedStorage,
    },
    db::api::storage::nodes::node_ref::NodeStorageRef,
};
use rayon::iter::ParallelIterator;

#[derive(Copy, Clone, Debug)]
pub enum NodesStorageRef<'a> {
    Mem(&'a ReadLockedStorage<NodeStore, VID>),
    #[cfg(feature = "storage")]
    Disk(DiskNodesRef<'a>),
}

#[cfg(feature = "storage")]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorageRef::Mem($pattern) => StorageVariants::Mem($result),
            NodesStorageRef::Disk($pattern) => StorageVariants::Disk($result),
        }
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorageRef::Mem($pattern) => $result,
        }
    };
}

impl<'a> NodesStorageRef<'a> {
    pub fn node(self, vid: VID) -> NodeStorageRef<'a> {
        match self {
            NodesStorageRef::Mem(store) => NodeStorageRef::Mem(store.get(vid)),
            #[cfg(feature = "storage")]
            NodesStorageRef::Disk(store) => NodeStorageRef::Disk(store.node(vid)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NodesStorageRef::Mem(store) => store.len(),
            #[cfg(feature = "storage")]
            NodesStorageRef::Disk(store) => store.len(),
        }
    }
    pub fn par_iter(self) -> impl ParallelIterator<Item = NodeStorageRef<'a>> {
        for_all_variants!(self, nodes => nodes.par_iter().map(|n| n.into()))
    }

    pub fn iter(self) -> impl Iterator<Item = NodeStorageRef<'a>> {
        for_all_variants!(self, nodes => nodes.iter().map(|n| n.into()))
    }
}
