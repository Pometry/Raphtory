use std::borrow::Cow;

// #[cfg(feature = "storage")]
use crate::db::api::storage::variants::storage_variants3::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskNode;
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, LayerIds, VID},
        storage::Entry,
        Direction,
    },
    db::api::{
        storage::{nodes::node_storage_ops::NodeStorageOps, tprop_storage_ops::TPropOps},
        view::internal::NodeAdditions,
    },
};

#[derive(Debug)]
pub enum NodeStorageRef<'a> {
    Mem(&'a NodeStore),
    Unlocked(Entry<'a, NodeStore>),
    #[cfg(feature = "storage")]
    Disk(DiskNode<'a>),
}

impl <'a> NodeStorageRef<'a> {
    pub fn with_additions<'b :'a, B>(&'b self, op: impl FnOnce(NodeAdditions<'a>) ->B) -> B {
        match self {
            NodeStorageRef::Mem(node) => op(NodeAdditions::Mem(node.timestamps())),
            NodeStorageRef::Unlocked(node) => {
                let ts = node.timestamps();
                op(NodeAdditions::Mem(ts))
            },
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk(node) => op(node.additions_for_layers(&LayerIds::All)),
        }        

    }
}

impl<'a> From<&'a NodeStore> for NodeStorageRef<'a> {
    fn from(value: &'a NodeStore) -> Self {
        NodeStorageRef::Mem(value)
    }
}

impl <'a> From<Entry<'a, NodeStore>> for NodeStorageRef<'a> {
    fn from(value: Entry<'a, NodeStore>) -> Self {
        NodeStorageRef::Unlocked(value)
    }
}

#[cfg(feature = "storage")]
impl<'a> From<DiskNode<'a>> for NodeStorageRef<'a> {
    fn from(value: DiskNode<'a>) -> Self {
        NodeStorageRef::Disk(value)
    }
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodeStorageRef::Mem($pattern) => $result,
            NodeStorageRef::Unlocked($pattern) => $result,
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeStorageRef::Mem($pattern) => StorageVariants::Mem($result),
            NodeStorageRef::Unlocked($pattern) => StorageVariants::Unlocked($result),
            NodeStorageRef::Disk($pattern) => StorageVariants::Disk($result),
        }
    }};
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeStorageRef::Mem($pattern) => StorageVariants::Mem($result),
            NodeStorageRef::Unlocked($pattern) => StorageVariants::Unlocked($result),
        }
    }};
}

impl<'a> NodeStorageOps<'a> for NodeStorageRef<'a> {
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

    fn node_type_id(&self) -> usize {
        for_all!(self, node => node.node_type_id())
    }

    fn vid(&self) -> VID {
        for_all!(self, node => node.vid())
    }

    fn id(self) -> u64 {
        for_all!(self, node => node.id())
    }

    fn name(self) -> Option<Cow<'a, str>> {
        for_all!(self, node => node.name())
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        for_all!(self, node => NodeStorageOps::find_edge(node, dst, layer_ids))
    }
}
