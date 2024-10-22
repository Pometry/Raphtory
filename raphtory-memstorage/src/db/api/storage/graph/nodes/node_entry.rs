use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, GidRef, LayerIds, GID},
    utils::iter::GenLockedIter,
    Direction,
};

#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskNode;
use crate::{
    core::{entities::{nodes::node_store::NodeStore, Prop}, storage::Entry},
    db::api::storage::graph::{
        nodes::node_ref::NodeStorageRef, variants::storage_variants3::StorageVariants,
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
    ) -> impl Iterator<Item = EdgeRef> + 'b {
        match self {
            NodeStorageEntry::Mem(entry) => StorageVariants::Mem(entry.edge_tuples(layers, dir)),
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


    pub fn id(&self) -> GID {
        match self {
            NodeStorageEntry::Mem(entry) => entry.global_id().clone(),
            NodeStorageEntry::Unlocked(entry) => entry.global_id().clone(),
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk(node) => node.id(),
        }
    }

    pub fn name(&self) -> String {
        self.id().to_str().as_ref().to_string()
    }

    pub fn node_type_id(&self) -> usize {
        match self {
            NodeStorageEntry::Mem(entry) => entry.node_type_id(),
            NodeStorageEntry::Unlocked(entry) => entry.node_type_id(),
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk(node) => node.node_type_id(),
        }
    }

    pub fn prop(&self, id: usize) -> Option<Prop> {
        match self {
            NodeStorageEntry::Mem(entry) => entry.constant_property(id).cloned(),
            NodeStorageEntry::Unlocked(entry) => entry.constant_property(id).cloned(),
            #[cfg(feature = "storage")]
            NodeStorageEntry::Disk(node) => node.get_const_prop(id),
        }
    }
}