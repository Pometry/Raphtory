use crate::core::entities::nodes::node_store::NodeStore;
use raphtory_api::core::entities::{GidRef, LayerIds};
use raphtory_api::core::Direction;
use std::any::Any;
use std::borrow::Cow;

#[cfg(feature = "storage")]
use crate::db::api::storage::graph::variants::storage_variants::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskNode;

#[derive(Copy, Clone, Debug)]
pub enum NodeStorageRef<'a> {
    Mem(&'a NodeStore),
    #[cfg(feature = "storage")]
    Disk(DiskNode<'a>),
}


impl<'a> From<&'a NodeStore> for NodeStorageRef<'a> {
    fn from(value: &'a NodeStore) -> Self {
        NodeStorageRef::Mem(value)
    }
}

#[cfg(feature = "storage")]
impl<'a> From<DiskNode<'a>> for NodeStorageRef<'a> {
    fn from(value: DiskNode<'a>) -> Self {
        NodeStorageRef::Disk(value)
    }
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodeStorageRef::Mem($pattern) => $result,
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk($pattern) => $result,
        }
    };
}

impl <'a> NodeStorageRef<'a> {
    pub fn node_type_id(self) -> usize {
        match self {
            NodeStorageRef::Mem(node) => node.node_type,
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk(node) => node.node_type_id(),
        }
    }

    pub fn degree(self, layer_ids: &LayerIds, direction: Direction) -> usize {
        for_all!(self, node => node.degree(layer_ids, direction))
    }
}

#[cfg(feature = "storage")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeStorageRef::Mem($pattern) => StorageVariants::Mem($result),
            NodeStorageRef::Disk($pattern) => StorageVariants::Disk($result),
        }
    }};
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeStorageRef::Mem($pattern) => $result,
        }
    }};
}
