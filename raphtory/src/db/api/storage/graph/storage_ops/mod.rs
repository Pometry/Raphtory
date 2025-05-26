use crate::db::api::{storage::storage::Storage, view::internal::InternalStorageOps};
#[cfg(feature = "storage")]
use crate::{
    db::api::storage::graph::variants::storage_variants::StorageVariants,
    disk_graph::{
        storage_interface::{
            edges::DiskEdges,
            edges_ref::DiskEdgesRef,
            node::{DiskNode, DiskOwnedNode},
            nodes::DiskNodesOwned,
            nodes_ref::DiskNodesRef,
        },
        DiskGraphStorage,
    },
};
use raphtory_storage::graph::graph::GraphStorage;

pub mod const_props;
#[cfg(feature = "storage")]
mod disk_storage;
pub mod edge_filter;
pub mod list_ops;
pub mod materialize;
pub mod node_filter;
pub mod time_props;
pub mod time_semantics;

impl InternalStorageOps for GraphStorage {
    fn get_storage(&self) -> Option<&Storage> {
        None
    }
}
