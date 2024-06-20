#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskOwnedNode;

#[cfg(feature = "storage")]
use either::Either;

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, LayerIds},
        storage::ArcEntry,
        Direction,
    },
    db::api::storage::nodes::node_storage_ops::NodeStorageIntoOps,
};

pub enum NodeOwnedEntry {
    Mem(ArcEntry<NodeStore>),
    #[cfg(feature = "storage")]
    Disk(DiskOwnedNode),
}

#[cfg(feature = "storage")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeOwnedEntry::Mem($pattern) => Either::Left($result),
            NodeOwnedEntry::Disk($pattern) => Either::Right($result),
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
