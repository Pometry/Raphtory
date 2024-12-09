use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        storage::{node_entry::{NodeEntry, Row}, TColumns},
        Direction,
    },
    db::api::{
        storage::graph::{
            nodes::node_storage_ops::NodeStorageOps,
            tprop_storage_ops::{SparseTPropOps, TPropOps, TPropRef},
        },
        view::internal::NodeAdditions,
    },
    prelude::Prop,
};
use raphtory_api::core::{entities::GidRef, storage::timeindex::TimeIndexEntry};
use std::{borrow::Cow, ops::Range};

#[cfg(feature = "storage")]
use crate::db::api::storage::graph::variants::storage_variants::StorageVariants;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::node::DiskNode;

#[derive(Copy, Clone, Debug)]
pub enum NodeStorageRef<'a> {
    Mem(NodeEntry<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskNode<'a>),
}


// #[derive(Debug)]
// struct RowIter<T, A, I: Iterator<Item = (T, Option<A>)>> {
//     cols: Vec<I>,
//     done: bool,
// }

// impl<T, A, I: Iterator<Item = (T, Option<A>)>> Iterator for RowIter<T, A, I> {
//     type Item = (T, Vec<Option<A>>);

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.done {
//             return None;
//         }
//         let mut row = vec![];
//         let mut time: Option<T> = None;
//         self.done = true;
//         for iter in &mut self.cols {
//             let prop = iter.next();
//             if let Some((t, prop)) = prop {
//                 self.done = false;
//                 time = Some(t);
//                 row.push(prop);
//             } else {
//                 row.push(None);
//             }
//         }
//         time.map(|t| (t, row))
//     }
// }

impl<'a> NodeStorageRef<'a> {
    pub fn temp_prop_rows(
        self,
        prop_ids: Range<usize>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Row<'a>)> + 'a {
        match self {
            NodeStorageRef::Mem(node_entry) => node_entry.into_rows(),
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk(disk_node) => todo!(),
        }
    }

    pub fn temp_prop_rows_window(
        self,
        prop_ids: Range<usize>,
        window: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Row<'a>)> + 'a {
        match self {
            NodeStorageRef::Mem(node_entry) => node_entry.into_rows_window(window),
            #[cfg(feature = "storage")]
            NodeStorageRef::Disk(disk_node) => todo!(),
        }
    }
}

impl<'a> From<NodeEntry<'a>> for NodeStorageRef<'a> {
    fn from(value: NodeEntry<'a>) -> Self {
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

    fn tprop(self, prop_id: usize) -> impl SparseTPropOps<'a> {
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
