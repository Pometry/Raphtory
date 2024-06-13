use std::{borrow::Cow, ops::Deref};

#[cfg(feature = "storage")]
use crate::db::api::storage::variants::storage_variants3::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskOwnedNode;
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, LayerIds, VID},
        storage::ArcEntry,
        Direction,
    },
    db::api::{
        storage::{
            nodes::{
                node_ref::NodeStorageRef,
                node_storage_ops::{NodeStorageIntoOps, NodeStorageOps},
            },
            tprop_storage_ops::TPropOps,
        },
        view::internal::NodeAdditions,
    },
};

use super::unlocked::UnlockedOwnedNode;

pub enum NodeOwnedEntry {
    Mem(ArcEntry<NodeStore>),
    Unlocked(UnlockedOwnedNode),
    #[cfg(feature = "storage")]
    Disk(DiskOwnedNode),
}

impl NodeOwnedEntry {
    pub fn as_ref(&self) -> NodeStorageRef {
        match self {
            NodeOwnedEntry::Mem(entry) => NodeStorageRef::Mem(entry),
            NodeOwnedEntry::Unlocked(entry) => NodeStorageRef::Unlocked(entry.node()),
            #[cfg(feature = "storage")]
            NodeOwnedEntry::Disk(entry) => NodeStorageRef::Disk(entry.as_ref()),
        }
    }
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodeOwnedEntry::Mem($pattern) => $result,
            NodeOwnedEntry::Unlocked($pattern) => $result,
            #[cfg(feature = "storage")]
            NodeOwnedEntry::Disk($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeOwnedEntry::Mem($pattern) => StorageVariants::Mem($result),
            NodeOwnedEntry::Unlocked($pattern) => StorageVariants::Unlocked($result),
            NodeOwnedEntry::Disk($pattern) => StorageVariants::Disk($result),
        }
    }};
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeOwnedEntry::Mem($pattern) => $result,
        }
    }};
}

impl NodeStorageIntoOps for NodeOwnedEntry {
    fn into_edges_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> {
        for_all_iter!(self, node => node.into_edges_iter(layers, dir))
    }
}
