#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskOwnedNode;

#[cfg(feature = "storage")]
use either::Either;
use raphtory_api::core::{entities::{edges::edge_ref::EdgeRef, LayerIds}, Direction};

use crate::core::{
        entities::nodes::node_store::NodeStore, storage::ArcEntry
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

impl NodeOwnedEntry {
    pub fn into_edges_iter(self, layers: &LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> {
        for_all_iter!(self, node => node.into_edges(layers, dir))
    }
}
