use storage::NodeEntryRef;

#[cfg(feature = "storage")]
use crate::disk::storage_interface::node::DiskNode;

pub type NodeStorageRef<'a> = NodeEntryRef<'a>;

// impl<'a> NodeStorageRef<'a> {
//     pub fn temp_prop_rows(self) -> impl Iterator<Item = (TimeIndexEntry, Row<'a>)> + 'a {
//         // match self {
//         //     NodeStorageRef::Mem(node_entry) => node_entry
//         //         .into_rows()
//         //         .map(|(t, row)| (t, Row::Mem(row)))
//         //         .into_dyn_boxed(),
//         //     #[cfg(feature = "storage")]
//         //     NodeStorageRef::Disk(disk_node) => disk_node.into_rows().into_dyn_boxed(),
//         // }
//         //TODO:
//         std::iter::empty()
//     }

//     pub fn temp_prop_rows_window(
//         self,
//         window: Range<TimeIndexEntry>,
//     ) -> impl Iterator<Item = (TimeIndexEntry, Row<'a>)> + 'a {
//         // match self {
//         //     NodeStorageRef::Mem(node_entry) => node_entry
//         //         .into_rows_window(window)
//         //         .map(|(t, row)| (t, Row::Mem(row)))
//         //         .into_dyn_boxed(),
//         //     #[cfg(feature = "storage")]
//         //     NodeStorageRef::Disk(disk_node) => disk_node.into_rows_window(window).into_dyn_boxed(),
//         // }
//         std::iter::empty()
//     }

//     pub fn last_before_row(self, t: TimeIndexEntry) -> Vec<(usize, Prop)> {
//         // match self {
//         //     NodeStorageRef::Mem(node_entry) => node_entry.last_before_row(t),
//         //     #[cfg(feature = "storage")]
//         //     NodeStorageRef::Disk(disk_node) => disk_node.last_before_row(t),
//         // }
//         todo!()
//     }
// }
