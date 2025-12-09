use crate::db::api::{storage::storage::Storage, view::internal::InternalStorageOps};
use raphtory_storage::graph::graph::GraphStorage;
use std::path::Path;

pub mod edge_filter;
pub mod list_ops;
pub mod materialize;
pub mod metadata;
pub mod node_filter;
pub mod time_props;
pub mod time_semantics;

impl InternalStorageOps for GraphStorage {
    fn get_storage(&self) -> Option<&Storage> {
        None
    }

    fn disk_storage_enabled(&self) -> Option<&Path> {
        self.disk_storage_enabled()
    }
}
