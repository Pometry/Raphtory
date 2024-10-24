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

pub enum EdgesStorage {
    Mem(Arc<LockedEdges>),
    #[cfg(feature = "storage")]
    Disk(DiskEdges),
}

impl EdgesStorage {
    #[inline]
    pub fn as_ref(&self) -> EdgesStorageRef {
        match self {
            EdgesStorage::Mem(storage) => EdgesStorageRef::Mem(storage),
            #[cfg(feature = "storage")]
            EdgesStorage::Disk(storage) => EdgesStorageRef::Disk(storage.as_ref()),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum EdgesStorageRef<'a> {
    Mem(&'a LockedEdges),
    Unlocked(UnlockedEdges<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskEdgesRef<'a>),
}

impl<'a> EdgesStorageRef<'a> {
    #[cfg(feature = "storage")]
    pub fn iter(self, layers: LayerIds) -> impl Iterator<Item = EdgeStorageEntry<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => StorageVariants::Mem(
                storage
                    .iter()
                    .filter(move |e| e.has_layer(layers.clone()))
                    .map(EdgeStorageEntry::Mem),
            ),
            EdgesStorageRef::Unlocked(edges) => StorageVariants::Unlocked(
                edges
                    .iter()
                    .filter(move |e| e.has_layer(layers.clone()))
                    .map(EdgeStorageEntry::Unlocked),
            ),
            EdgesStorageRef::Disk(storage) => {
                StorageVariants::Disk(storage.iter(layers).map(EdgeStorageEntry::Disk))
            }
        }
    }

    #[cfg(not(feature = "storage"))]
    pub fn iter(self, layers: LayerIds) -> impl Iterator<Item = EdgeStorageEntry<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => StorageVariants::Mem(
                storage
                    .iter()
                    .filter(move |e| e.has_layer(layers.clone()))
                    .map(EdgeStorageEntry::Mem),
            ),
            EdgesStorageRef::Unlocked(edges) => StorageVariants::Unlocked(
                edges
                    .iter()
                    .filter(move |e| e.as_mem_edge().has_layer(layers.clone))
                    .map(EdgeStorageEntry::Unlocked),
            ),
        }
    }

    #[cfg(feature = "storage")]
    pub fn par_iter(self, layers: LayerIds) -> impl ParallelIterator<Item = EdgeStorageEntry<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => StorageVariants::Mem(
                storage
                    .par_iter()
                    .filter(move |e| e.has_layer(layers.clone()))
                    .map(EdgeStorageEntry::Mem),
            ),
            EdgesStorageRef::Unlocked(edges) => StorageVariants::Unlocked(
                edges
                    .par_iter()
                    .filter(move |e| e.has_layer(layers.clone()))
                    .map(EdgeStorageEntry::Unlocked),
            ),
            EdgesStorageRef::Disk(storage) => {
                StorageVariants::Disk(storage.par_iter(layers).map(EdgeStorageEntry::Disk))
            }
        }
    }

    #[cfg(not(feature = "storage"))]
    pub fn par_iter(self, layers: LayerIds) -> impl ParallelIterator<Item = EdgeStorageEntry<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => StorageVariants::Mem(
                storage
                    .par_iter()
                    .filter(move |e| e.has_layer(layers.clone()))
                    .map(EdgeStorageEntry::Mem),
            ),
            EdgesStorageRef::Unlocked(edges) => StorageVariants::Unlocked(
                edges
                    .par_iter()
                    .filter(move |e| e.as_mem_edge().has_layer(layers.clone()))
                    .map(EdgeStorageEntry::Unlocked),
            ),
        }
    }

    #[inline]
    pub fn count(self, layers: LayerIds) -> usize {
        match self {
            EdgesStorageRef::Mem(storage) => match layers {
                LayerIds::None => 0,
                LayerIds::All => storage.len(),
                _ => storage
                    .par_iter()
                    .filter(|e| e.has_layer(layers.clone()))
                    .count(),
            },
            EdgesStorageRef::Unlocked(edges) => match layers {
                LayerIds::None => 0,
                LayerIds::All => edges.len(),
                _ => edges
                    .par_iter()
                    .filter(|e| e.as_mem_edge().has_layer(layers.clone()))
                    .count(),
            },
            #[cfg(feature = "storage")]
            EdgesStorageRef::Disk(storage) => storage.count(layers),
        }
    }

    #[inline]
    pub fn edge(self, edge: EID) -> EdgeStorageEntry<'a> {
        match self {
            EdgesStorageRef::Mem(storage) => EdgeStorageEntry::Mem(storage.get_mem(edge)),
            EdgesStorageRef::Unlocked(storage) => {
                EdgeStorageEntry::Unlocked(storage.0.edge_entry(edge))
            }
            #[cfg(feature = "storage")]
            EdgesStorageRef::Disk(storage) => EdgeStorageEntry::Disk(storage.edge(edge)),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            EdgesStorageRef::Mem(storage) => storage.len(),
            EdgesStorageRef::Unlocked(storage) => storage.len(),
            #[cfg(feature = "storage")]
            EdgesStorageRef::Disk(storage) => storage.len(),
        }
    }
}
