use super::row::Row;
use crate::graph::{
    nodes::{node_additions::NodeAdditions, node_storage_ops::NodeStorageOps},
    variants::storage_variants2::StorageVariants2,
};
use raphtory_api::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            properties::{prop::Prop, tprop::TPropOps},
            GidRef, LayerIds, VID,
        },
        storage::timeindex::TimeIndexEntry,
        Direction,
    },
    iter::IntoDynBoxed,
};
use raphtory_core::storage::node_entry::NodePtr;
use std::{borrow::Cow, ops::Range};
use storage::{NodeEntryRef, api::nodes::NodeRefOps};

#[cfg(feature = "storage")]
use crate::disk::storage_interface::node::DiskNode;

#[derive(Copy, Clone, Debug)]
pub enum NodeStorageRef<'a> {
    Mem(NodeEntryRef<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskNode<'a>),
}

impl<'a> From<NodeEntryRef<'a>> for NodeStorageRef<'a> {
    fn from(value: NodeEntryRef<'a>) -> Self {
        NodeStorageRef::Mem(value)
    }
}

impl<'a> NodeStorageRef<'a> {
    pub fn temp_prop_rows(self) -> impl Iterator<Item = (TimeIndexEntry, Row<'a>)> + 'a {
        // match self {
        //     NodeStorageRef::Mem(node_entry) => node_entry
        //         .into_rows()
        //         .map(|(t, row)| (t, Row::Mem(row)))
        //         .into_dyn_boxed(),
        //     #[cfg(feature = "storage")]
        //     NodeStorageRef::Disk(disk_node) => disk_node.into_rows().into_dyn_boxed(),
        // }
        //TODO:
        std::iter::empty()
    }

    pub fn temp_prop_rows_window(
        self,
        window: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Row<'a>)> + 'a {
        // match self {
        //     NodeStorageRef::Mem(node_entry) => node_entry
        //         .into_rows_window(window)
        //         .map(|(t, row)| (t, Row::Mem(row)))
        //         .into_dyn_boxed(),
        //     #[cfg(feature = "storage")]
        //     NodeStorageRef::Disk(disk_node) => disk_node.into_rows_window(window).into_dyn_boxed(),
        // }
        std::iter::empty()
    }

    pub fn last_before_row(self, t: TimeIndexEntry) -> Vec<(usize, Prop)> {
        // match self {
        //     NodeStorageRef::Mem(node_entry) => node_entry.last_before_row(t),
        //     #[cfg(feature = "storage")]
        //     NodeStorageRef::Disk(disk_node) => disk_node.last_before_row(t),
        // }
        todo!()
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
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {{
        match $value {
            NodeStorageRef::Mem($pattern) => StorageVariants2::Mem($result),
            NodeStorageRef::Disk($pattern) => StorageVariants2::Disk($result),
        }
    }};
}

#[cfg(not(feature = "storage"))]
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

    fn edges_iter(self, layers: &LayerIds, dir: Direction) -> impl Iterator<Item = EdgeRef> + 'a {
        for_all_iter!(self, node => node.edges_iter(layers, dir))
    }

    fn node_type_id(self) -> usize {
        for_all!(self, node => node.node_type_id())
    }

    fn vid(self) -> VID {
        for_all!(self, node => node.vid())
    }

    fn id(self) -> GidRef<'a> {
        for_all!(self, node => node.id())
    }

    fn name(self) -> Option<Cow<'a, str>> {
        for_all!(self, node => node.name())
    }

    fn find_edge(self, dst: VID, layer_ids: &LayerIds) -> Option<EdgeRef> {
        for_all!(self, node => NodeStorageOps::find_edge(node, dst, layer_ids))
    }

    fn prop(self, prop_id: usize) -> Option<Prop> {
        for_all!(self, node => node.prop(prop_id))
    }

    fn tprops(self) -> impl Iterator<Item = (usize, impl TPropOps<'a>)> {
        match self {
            NodeStorageRef::Mem(node) => {
                StorageVariants2::Mem(node.tprops().map(|(k, v)| (k, StorageVariants2::Mem(v))))
            }
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk(node) => {
                StorageVariants2::Disk(node.tprops().map(|(k, v)| (k, StorageVariants2::Disk(v))))
            }
        }
    }
}
