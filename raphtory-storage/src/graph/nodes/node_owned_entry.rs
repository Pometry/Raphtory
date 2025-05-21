use crate::graph::nodes::node_storage_ops::NodeStorageIntoOps;
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, LayerIds},
    Direction,
};
use raphtory_core::storage::ArcNodeEntry;

#[cfg(feature = "storage")]
use {crate::disk::storage_interface::node::DiskOwnedNode, either::Either};

pub enum NodeOwnedEntry {
    Mem(ArcNodeEntry),
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
