use std::borrow::Cow;

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
            variants::storage_variants3::StorageVariants,
        },
        view::internal::NodeAdditions,
    },
};

pub enum NodeStorageEntry<'a> {
    Mem(&'a NodeStore),
    Unlocked(Entry<'a, NodeStore>),
    #[cfg(feature = "storage")]
    Disk(DiskNode<'a>),
}

impl<'a> From<&'a NodeStore> for NodeStorageEntry<'a> {
    fn from(value: &'a NodeStore) -> Self {
        NodeStorageEntry::Mem(value)
    }
}

impl<'a> From<Entry<'a, NodeStore>> for NodeStorageEntry<'a> {
    fn from(value: Entry<'a, NodeStore>) -> Self {
        NodeStorageEntry::Unlocked(value)
    }
}

#[cfg(feature = "storage")]
impl<'a> From<DiskNode<'a>> for NodeStorageEntry<'a> {
    fn from(value: DiskNode<'a>) -> Self {
        NodeStorageEntry::Disk(value)
    }
}

impl<'a> NodeStorageEntry<'a> {
    #[inline]
    pub fn as_ref(&self) -> NodeStorageRef {
        match self {
            NodeStorageEntry::Mem(entry) => NodeStorageRef::Mem(entry),
            NodeStorageEntry::Unlocked(entry) => NodeStorageRef::Mem(entry),
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

impl<'b> NodeStorageEntry<'b> {
    pub fn into_edges_iter(
        self,
        layers: &'b LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + '_ {
        match self {
            NodeStorageEntry::Mem(entry) => StorageVariants::Mem(entry.edges_iter(layers, dir)),
            NodeStorageEntry::Unlocked(entry) => {
                StorageVariants::Unlocked(entry.into_edges_iter(layers, dir))
            }
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk(node) => StorageVariants::Disk(node.edges_iter(layers, dir)),
        }
    }
}

impl<'a, 'b: 'a> NodeStorageOps<'a> for &'a NodeStorageEntry<'b> {
    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        self.as_ref().degree(layers, dir)
    }

    fn additions(self) -> NodeAdditions<'a> {
        self.as_ref().additions()
    }

    fn tprop(self, prop_id: usize) -> impl TPropOps<'a> {
        self.as_ref().tprop(prop_id)
    }

    fn edges_iter(
        self,
        layers: &'a LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        self.as_ref().edges_iter(layers, dir)
    }

    fn node_type_id(self) -> usize {
        self.as_ref().node_type_id()
    }

    fn vid(self) -> VID {
        self.as_ref().vid()
    }

    fn id(self) -> u64 {
        self.as_ref().id()
    }

    fn name(self) -> Option<Cow<'a, str>> {
        self.as_ref().name()
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        self.as_ref().find_edge(dst, layer_ids)
    }
}
