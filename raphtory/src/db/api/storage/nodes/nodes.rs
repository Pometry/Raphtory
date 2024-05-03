#[cfg(feature = "arrow")]
use crate::arrow::storage_interface::nodes::ArrowNodesOwned;
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
    #[cfg(feature = "arrow")]
    Arrow(ArrowNodesOwned),
}

#[cfg(feature = "arrow")]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorage::Mem($pattern) => StorageVariants::Mem($result),
            NodesStorage::Arrow($pattern) => StorageVariants::Arrow($result),
        }
    };
}

#[cfg(not(feature = "arrow"))]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodesStorage::Mem($pattern) => $result,
        }
    };
}

impl NodesStorage {
    pub fn as_ref(&self) -> NodesStorageRef {
        match self {
            NodesStorage::Mem(storage) => NodesStorageRef::Mem(storage),
            #[cfg(feature = "arrow")]
            NodesStorage::Arrow(storage) => NodesStorageRef::Arrow(storage.as_ref()),
        }
    }

    pub fn node_ref(&self, vid: VID) -> NodeStorageRef {
        match self {
            NodesStorage::Mem(storage) => NodeStorageRef::Mem(storage.get(vid)),
            #[cfg(feature = "arrow")]
            NodesStorage::Arrow(storage) => NodeStorageRef::Arrow(storage.node(vid)),
        }
    }
}
