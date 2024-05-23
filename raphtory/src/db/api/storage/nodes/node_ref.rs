#[cfg(feature = "arrow")]
use crate::arrow::storage_interface::node::ArrowNode;
#[cfg(feature = "arrow")]
use crate::db::api::storage::variants::storage_variants::StorageVariants;
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, LayerIds, VID},
        Direction,
    },
    db::api::{
        storage::{nodes::node_storage_ops::NodeStorageOps, tprop_storage_ops::TPropOps},
        view::internal::NodeAdditions,
    },
};

#[derive(Copy, Clone, Debug)]
pub enum NodeStorageRef<'a> {
    Mem(&'a NodeStore),
    #[cfg(feature = "arrow")]
    Arrow(ArrowNode<'a>),
}

impl<'a> From<&'a NodeStore> for NodeStorageRef<'a> {
    fn from(value: &'a NodeStore) -> Self {
        NodeStorageRef::Mem(value)
    }
}

#[cfg(feature = "arrow")]
impl<'a> From<ArrowNode<'a>> for NodeStorageRef<'a> {
    fn from(value: ArrowNode<'a>) -> Self {
        NodeStorageRef::Arrow(value)
    }
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            NodeStorageRef::Mem($pattern) => $result,
            #[cfg(feature = "arrow")]
            NodeStorageRef::Arrow($pattern) => $result,
        }
    };
}

#[cfg(feature = "arrow")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeStorageRef::Mem($pattern) => StorageVariants::Mem($result),
            NodeStorageRef::Arrow($pattern) => StorageVariants::Arrow($result),
        }
    }};
}

#[cfg(not(feature = "arrow"))]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeStorageRef::Mem($pattern) => $result,
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

    fn node_type_id(self) -> usize {
        for_all!(self, node => node.node_type_id())
    }

    fn vid(self) -> VID {
        for_all!(self, node => node.vid())
    }

    fn name(self) -> Option<&'a str> {
        for_all!(self, node => node.name())
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        for_all!(self, node => NodeStorageOps::find_edge(node, dst, layer_ids))
    }
}
