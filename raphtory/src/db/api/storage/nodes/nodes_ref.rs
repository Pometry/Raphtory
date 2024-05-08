#[cfg(feature = "arrow")]
use crate::arrow::storage_interface::nodes_ref::ArrowNodesRef;
#[cfg(feature = "arrow")]
use crate::db::api::storage::variants::storage_variants::StorageVariants;
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
    #[cfg(feature = "arrow")]
    Arrow(ArrowNodesRef<'a>),
}

#[cfg(feature = "arrow")]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorageRef::Mem($pattern) => StorageVariants::Mem($result),
            NodesStorageRef::Arrow($pattern) => StorageVariants::Arrow($result),
        }
    };
}

#[cfg(not(feature = "arrow"))]
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
            #[cfg(feature = "arrow")]
            NodesStorageRef::Arrow(store) => NodeStorageRef::Arrow(store.node(vid)),
        }
    }

    pub fn par_iter(self) -> impl ParallelIterator<Item = NodeStorageRef<'a>> {
        for_all_variants!(self, nodes => nodes.par_iter().map(|n| n.into()))
    }

    pub fn iter(self) -> impl Iterator<Item = NodeStorageRef<'a>> {
        for_all_variants!(self, nodes => nodes.iter().map(|n| n.into()))
    }
}
