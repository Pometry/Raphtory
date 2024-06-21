use crate::{
    core::{
        entities::{edges::edge_store::EdgeStore, LayerIds, EID},
        storage::ReadLockedStorage,
    },
    db::api::storage::nodes::unlocked::UnlockedEdges,
};

#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::edges_ref::DiskEdgesRef;

use super::edge_entry::EdgeStorageEntry;
use crate::db::api::storage::edges::edge_storage_ops::EdgeStorageOps;
#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::edges::DiskEdges;
use either::Either;
use rayon::iter::ParallelIterator;
use std::sync::Arc;

pub enum EdgesStorage {
    Mem(Arc<ReadLockedStorage<EdgeStore, EID>>),
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

#[derive(Debug)]
pub enum EdgesStorageRef<'a> {
    Mem(&'a ReadLockedStorage<EdgeStore, EID>),
    Unlocked(UnlockedEdges<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskEdgesRef<'a>),
}

#[cfg(feature = "storage")]
use crate::db::api::storage::variants::storage_variants3::StorageVariants;

impl<'a> EdgesStorageRef<'a> {
    #[cfg(feature = "storage")]
    pub fn iter(self, layers: LayerIds) -> impl Iterator<Item = EdgeStorageEntry<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => StorageVariants::Mem(
                storage
                    .iter()
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageEntry::Mem),
            ),
            EdgesStorageRef::Unlocked(edges) => StorageVariants::Unlocked(
                edges
                    .iter()
                    .filter(move |e| e.has_layer(&layers))
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
            EdgesStorageRef::Mem(storage) => Either::Left(
                storage
                    .iter()
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageEntry::Mem),
            ),
            EdgesStorageRef::Unlocked(edges) => Either::Right(
                edges
                    .iter()
                    .filter(move |e| e.has_layer(&layers))
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
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageEntry::Mem),
            ),
            EdgesStorageRef::Unlocked(edges) => StorageVariants::Unlocked(
                edges
                    .par_iter()
                    .filter(move |e| e.has_layer(&layers))
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
            EdgesStorageRef::Mem(storage) => Either::Left(
                storage
                    .par_iter()
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageEntry::Mem),
            ),
            EdgesStorageRef::Unlocked(edges) => Either::Right(
                edges
                    .par_iter()
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageEntry::Unlocked),
            ),
        }
    }

    #[inline]
    pub fn count(self, layers: &LayerIds) -> usize {
        match self {
            EdgesStorageRef::Mem(storage) => match layers {
                LayerIds::None => 0,
                LayerIds::All => storage.len(),
                _ => storage.par_iter().filter(|e| e.has_layer(layers)).count(),
            },
            EdgesStorageRef::Unlocked(edges) => match layers {
                LayerIds::None => 0,
                LayerIds::All => edges.len(),
                _ => edges.par_iter().filter(|e| e.has_layer(layers)).count(),
            },
            #[cfg(feature = "storage")]
            EdgesStorageRef::Disk(storage) => storage.count(layers),
        }
    }
}
