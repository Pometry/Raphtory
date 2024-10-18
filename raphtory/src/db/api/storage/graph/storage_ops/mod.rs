use raphtory_memstorage::db::api::storage::graph::GraphStorage;

use crate::db::api::{mutation::DeletionOps, view::internal::CoreGraphOps};
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

pub mod additions;
pub mod const_props;
pub mod deletions;
pub mod edge_filter;
pub mod layer_ops;
pub mod list_ops;
pub mod materialize;
pub mod node_filter;
pub mod prop_add;
pub mod time_props;
pub mod time_semantics;

impl DeletionOps for GraphStorage {}

impl CoreGraphOps for GraphStorage {
    #[inline(always)]
    fn core_graph(&self) -> &GraphStorage {
        self
    }
}
