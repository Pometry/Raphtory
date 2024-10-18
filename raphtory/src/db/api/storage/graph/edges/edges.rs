use super::{edge_entry::EdgeStorageEntry, unlocked::UnlockedEdges};
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::{edges::DiskEdges, edges_ref::DiskEdgesRef};
use crate::{
    core::{entities::LayerIds, storage::raw_edges::LockedEdges},
    db::api::storage::graph::{
        edges::edge_storage_ops::EdgeStorageOps, variants::storage_variants3::StorageVariants,
    },
};
use raphtory_api::core::entities::EID;
use rayon::iter::ParallelIterator;
use std::sync::Arc;

