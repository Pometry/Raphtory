use crate::graph::{
    nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    variants::storage_variants3::StorageVariants3,
};
use raphtory_api::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            properties::{prop::Prop, tprop::TPropOps},
            GidRef, LayerIds, VID,
        },
        Direction,
    },
    iter::BoxedLIter,
};
use raphtory_core::{
    storage::{node_entry::NodePtr, NodeEntry},
    utils::iter::GenLockedIter,
};
use std::borrow::Cow;

#[cfg(feature = "storage")]
use crate::disk::storage_interface::node::DiskNode;
use crate::graph::nodes::node_additions::NodeAdditions;

pub enum NodeStorageEntry<'a> {
    Mem(NodePtr<'a>),
    Unlocked(NodeEntry<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskNode<'a>),
}

impl<'a> From<NodePtr<'a>> for NodeStorageEntry<'a> {
    fn from(value: NodePtr<'a>) -> Self {
        NodeStorageEntry::Mem(value)
    }
}

impl<'a> From<NodeEntry<'a>> for NodeStorageEntry<'a> {
    fn from(value: NodeEntry<'a>) -> Self {
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
            NodeStorageEntry::Mem(entry) => NodeStorageRef::Mem(*entry),
            NodeStorageEntry::Unlocked(entry) => NodeStorageRef::Mem(entry.as_ref()),
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
        layers: &LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + use<'b, '_> {
        match self {
            NodeStorageEntry::Mem(entry) => StorageVariants3::Mem(entry.edges_iter(layers, dir)),
            NodeStorageEntry::Unlocked(entry) => {
                StorageVariants3::Unlocked(entry.into_edges(layers, dir))
            }
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk(node) => StorageVariants3::Disk(node.edges_iter(layers, dir)),
        }
    }

    pub fn prop_ids(self) -> BoxedLIter<'b, usize> {
        match self {
            NodeStorageEntry::Mem(entry) => Box::new(entry.node().const_prop_ids()),
            NodeStorageEntry::Unlocked(entry) => Box::new(GenLockedIter::from(entry, |e| {
                Box::new(e.as_ref().node().const_prop_ids())
            })),
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk(node) => Box::new(node.constant_node_prop_ids()),
        }
    }

    pub fn temporal_prop_ids(self) -> Box<dyn Iterator<Item = usize> + 'b> {
        match self {
            NodeStorageEntry::Mem(entry) => Box::new(entry.temporal_prop_ids()),
            NodeStorageEntry::Unlocked(entry) => Box::new(GenLockedIter::from(entry, |e| {
                Box::new(e.as_ref().temporal_prop_ids())
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

    fn edges_iter(self, layers: &LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> + 'a {
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

    fn tprops(self) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> {
        self.as_ref().tprops()
    }
}
