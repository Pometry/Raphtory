use std::borrow::Cow;

#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskNode;
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, GidRef, LayerIds, VID},
        storage::Entry,
        utils::iter::GenLockedIter,
        Direction,
    },
    db::api::{
        storage::graph::{
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
            variants::storage_variants3::StorageVariants,
        },
        view::internal::NodeAdditions,
    },
    prelude::Prop,
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

    pub fn prop_ids(self) -> Box<dyn Iterator<Item = usize> + 'b> {
        match self {
            NodeStorageEntry::Mem(entry) => Box::new(entry.const_prop_ids()),
            NodeStorageEntry::Unlocked(entry) => {
                Box::new(GenLockedIter::from(entry, |e| Box::new(e.const_prop_ids())))
            }
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk(node) => Box::new(node.constant_node_prop_ids()),
        }
    }

    pub fn temporal_prop_ids(self) -> Box<dyn Iterator<Item = usize> + 'b> {
        match self {
            NodeStorageEntry::Mem(entry) => Box::new(entry.temporal_prop_ids()),
            NodeStorageEntry::Unlocked(entry) => Box::new(GenLockedIter::from(entry, |e| {
                Box::new(e.temporal_prop_ids())
            })),
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk(node) => Box::new(node.temporal_node_prop_ids()),
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

    fn id(self) -> GidRef<'a> {
        self.as_ref().id()
    }

    fn name(self) -> Option<Cow<'a, str>> {
        self.as_ref().name()
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        self.as_ref().find_edge(dst, layer_ids)
    }

    fn prop(self, prop_id: usize) -> Option<Prop> {
        self.as_ref().prop(prop_id)
    }
}
