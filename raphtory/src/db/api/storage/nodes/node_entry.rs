use std::{borrow::Cow, ops::Deref};

// #[cfg(feature = "storage")]
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskNode;
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, LayerIds, VID},
        storage::Entry,
        Direction,
    },
    db::api::{
        storage::{
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
            variants::storage_variants::StorageVariants,
        },
        view::internal::NodeAdditions,
    },
};

pub enum NodeStorageEntry<'a> {
    Mem(Entry<'a, NodeStore>),
    #[cfg(feature = "storage")]
    Disk(DiskNode<'a>),
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodeStorageEntry::Mem($pattern) => $result,
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeStorageEntry::Mem($pattern) => StorageVariants::Mem($result),
            NodeStorageEntry::Disk($pattern) => StorageVariants::Disk($result),
        }
    }};
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeStorageEntry::Mem($pattern) => $result,
        }
    }};
}

impl<'a> NodeStorageEntry<'a> {
    pub fn as_ref(&self) -> NodeStorageRef {
        match self {
            NodeStorageEntry::Mem(entry) => NodeStorageRef::Mem(entry),
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk(node) => NodeStorageRef::Disk(*node),
        }
    }
}

impl<'a, 'b: 'a> From<&'a NodeStorageEntry<'b>> for NodeStorageRef<'a> {
    fn from(value: &'a NodeStorageEntry<'b>) -> Self {
        value.as_ref()
    }
}

impl<'a> NodeStorageOps<'a> for NodeStorageEntry<'a> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        match self {
            NodeStorageEntry::Mem(node) => node.deref().degree(layers, dir),
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk(node) => node.degree(layers, dir),
        }
    }

    fn additions(&self) -> NodeAdditions<'a> {
        for_all!(self, node => node.additions())
    }

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        {
            match self {
                NodeStorageEntry::Mem(node) => StorageVariants::Mem(node.tprop(prop_id)),
                #[cfg(feature = "storage")]
                NodeStorageEntry::Disk(node) => StorageVariants::Disk(node.tprop(prop_id)),
            }
        }
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

    fn name(self) -> Option<Cow<'a, str>> {
        for_all!(self, node => node.name())
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        for_all!(self, node => node.find_edge(dst, layer_ids))
    }
}
