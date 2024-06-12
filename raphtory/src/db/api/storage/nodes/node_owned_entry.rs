use std::ops::Deref;

#[cfg(feature = "storage")]
use crate::db::api::storage::variants::storage_variants::StorageVariants;
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

pub enum NodeOwnedEntry {
    Mem(ArcEntry<NodeStore>),
    #[cfg(feature = "storage")]
    Disk(DiskOwnedNode),
}

impl NodeOwnedEntry {
    pub fn as_ref(&self) -> NodeStorageRef {
        match self {
            NodeOwnedEntry::Mem(entry) => NodeStorageRef::Mem(entry),
            #[cfg(feature = "storage")]
            NodeOwnedEntry::Disk(entry) => NodeStorageRef::Disk(entry.as_ref()),
        }
    }
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodeOwnedEntry::Mem($pattern) => $result,
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

impl<'a> NodeStorageOps<'a> for &'a NodeOwnedEntry {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        for_all!(self, node => node.degree(layers, dir))
    }

    fn additions(&self) -> NodeAdditions<'a> {
        match self {
            NodeOwnedEntry::Mem(entry) => entry.deref().additions(),
            #[cfg(feature = "storage")]
            NodeOwnedEntry::Disk(entry) => entry.additions(),
        }
    }

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        for_all_iter!(self, node => node.tprop(prop_id))
    }

    fn edges_iter(
        self,
        layers: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        for_all_iter!(self, node => node.edges_iter(layers, dir))
    }

    fn node_type_id(&self) -> usize {
        for_all!(self, node => node.deref().node_type_id())
    }

    fn vid(&self) -> VID {
        for_all!(self, node => node.deref().vid())
    }

    fn id(self) -> u64 {
        for_all!(self, node => node.id())
    }

    fn name(self) -> Option<&'a str> {
        for_all!(self, node => node.name())
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        for_all!(self, node => node.find_edge(dst, layer_ids))
    }
}

impl NodeStorageIntoOps for NodeOwnedEntry {
    fn into_edges_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> {
        for_all_iter!(self, node => node.into_edges_iter(layers, dir))
    }

    fn into_neighbours_iter(self, layers: LayerIds, dir: Direction) -> impl Iterator<Item = VID> {
        for_all_iter!(self, node => node.into_neighbours_iter(layers, dir))
    }
}
