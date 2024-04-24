use crate::{
    core::{
        entities::{nodes::node_store::NodeStore, VID},
        storage::ReadLockedStorage,
    },
    db::api::storage::{
        arrow::nodes::{ArrowNodesOwned, ArrowNodesRef},
        nodes::node_ref::NodeStorageRef,
    },
};
use arrow2::Either;
use rayon::iter::ParallelIterator;
use std::sync::Arc;

pub enum NodesStorage {
    Mem(Arc<ReadLockedStorage<NodeStore, VID>>),
    Arrow(ArrowNodesOwned),
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

#[derive(Copy, Clone, Debug)]
pub enum NodesStorageRef<'a> {
    Mem(&'a ReadLockedStorage<NodeStore, VID>),
    #[cfg(feature = "arrow")]
    Arrow(ArrowNodesRef<'a>),
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
        match self {
            NodesStorageRef::Mem(store) => Either::Left(store.par_iter().map(NodeStorageRef::Mem)),
            #[cfg(feature = "arrow")]
            NodesStorageRef::Arrow(store) => {
                Either::Right(store.par_iter().map(NodeStorageRef::Arrow))
            }
        }
    }

    pub fn iter(self) -> impl Iterator<Item = NodeStorageRef<'a>> {
        match self {
            NodesStorageRef::Mem(store) => Either::Left(store.iter().map(NodeStorageRef::Mem)),
            #[cfg(feature = "arrow")]
            NodesStorageRef::Arrow(store) => Either::Right(store.iter().map(NodeStorageRef::Arrow)),
        }
    }
}
