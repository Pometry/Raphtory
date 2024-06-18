#[cfg(feature = "storage")]
use crate::db::api::storage::variants::storage_variants::StorageVariants;
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

impl<'a, 'b: 'a> NodeStorageOps<'a> for &'a NodeStorageEntry<'b> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        for_all!(self, node => node.degree(layers, dir))
    }

    fn additions(self) -> NodeAdditions<'a> {
        for_all!(self, node => node.additions())
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

    fn node_type_id(self) -> usize {
        for_all!(self, node => node.node_type_id())
    }

    fn vid(self) -> VID {
        for_all!(self, node => node.vid())
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
