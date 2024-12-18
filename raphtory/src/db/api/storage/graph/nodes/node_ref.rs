use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        storage::node_entry::NodePtr,
        Direction,
    },
    db::api::{
        storage::graph::{nodes::node_storage_ops::NodeStorageOps, tprop_storage_ops::TPropOps},
        view::internal::NodeAdditions,
    },
    prelude::Prop,
};
use raphtory_api::{
    core::{entities::GidRef, storage::timeindex::TimeIndexEntry},
    iter::IntoDynBoxed,
};
use std::{borrow::Cow, ops::Range};

#[cfg(feature = "storage")]
use crate::db::api::storage::graph::variants::storage_variants::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskNode;

use super::row::Row;

#[derive(Copy, Clone, Debug)]
pub enum NodeStorageRef<'a> {
    Mem(NodePtr<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskNode<'a>),
}

impl<'a> NodeStorageRef<'a> {
    pub fn temp_prop_rows(self) -> impl Iterator<Item = (TimeIndexEntry, Row<'a>)> + 'a {
        match self {
            NodeStorageRef::Mem(node_entry) => node_entry.into_rows().into_dyn_boxed(),
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk(disk_node) => disk_node.into_rows().into_dyn_boxed(),
        }
    }

    pub fn temp_prop_rows_window(
        self,
        window: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Row<'a>)> + 'a {
        match self {
            NodeStorageRef::Mem(node_entry) => node_entry.into_rows_window(window).into_dyn_boxed(),
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk(disk_node) => disk_node.into_rows_window(window).into_dyn_boxed(),
        }
    }

    pub fn last_before_row(self, t: TimeIndexEntry) -> Vec<(usize, Prop)> {
        match self {
            NodeStorageRef::Mem(node_entry) => node_entry.last_before_row(t),
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk(disk_node) => disk_node.last_before_row(t),
        }
    }
}

impl<'a> From<NodePtr<'a>> for NodeStorageRef<'a> {
    fn from(value: NodePtr<'a>) -> Self {
        NodeStorageRef::Mem(value)
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
            NodeStorageRef::Mem($pattern) => StorageVariants::Mem($result),
            NodeStorageRef::Disk($pattern) => StorageVariants::Disk($result),
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
}
