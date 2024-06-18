#[cfg(not(feature = "storage"))]
use either::Either;

#[cfg(feature = "storage")]
use crate::db::api::storage::variants::storage_variants3::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskOwnedNode;
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, LayerIds},
        storage::ArcEntry,
        Direction,
    },
    db::api::storage::nodes::node_storage_ops::NodeStorageIntoOps,
};

use super::unlocked::UnlockedOwnedNode;

pub enum NodeOwnedEntry {
    Mem(ArcEntry<NodeStore>),
    Unlocked(UnlockedOwnedNode),
    #[cfg(feature = "storage")]
    Disk(DiskOwnedNode),
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
            NodeOwnedEntry::Mem($pattern) => Either::Left($result),
            NodeOwnedEntry::Unlocked($pattern) => Either::Right($result),
        }
    }};
}

impl NodeStorageIntoOps for NodeOwnedEntry {
    fn into_edges_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> {
        for_all_iter!(self, node => node.into_edges_iter(layers, dir))
    }
}
