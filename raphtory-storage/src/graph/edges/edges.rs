use super::{edge_entry::EdgeStorageEntry, unlocked::UnlockedEdges};
use crate::graph::{
    edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
    variants::storage_variants3::StorageVariants3,
};
use raphtory_api::core::entities::{LayerIds, EID};
use rayon::iter::ParallelIterator;
use std::sync::Arc;
use storage::{Extension, ReadLockedEdges};

pub struct EdgesStorage {
    storage: Arc<ReadLockedEdges<Extension>>,
}

impl EdgesStorage {
    #[inline]
    pub fn as_ref(&self) -> EdgesStorageRef {
        EdgesStorageRef::Mem(self.storage.as_ref())
    }

    pub fn edge(&self, eid: EID) -> EdgeStorageRef {
        self.storage.edge_ref(eid)
    }

    pub fn iter<'a>(
        &'a self,
        layers: &'a LayerIds,
    ) -> impl Iterator<Item = EdgeStorageRef<'a>> + Send + Sync + 'a {
        self.storage.iter(layers)
    }

    pub fn par_iter<'a>(
        &'a self,
        layers: &'a LayerIds,
    ) -> impl ParallelIterator<Item = EdgeStorageRef<'a>> + Sync + 'a {
        self.storage.par_iter(layers)
    }
}

#[derive(Debug, Copy, Clone)]
pub enum EdgesStorageRef<'a> {
    Mem(&'a ReadLockedEdges<Extension>),
    Unlocked(UnlockedEdges<'a>),
}

impl<'a> EdgesStorageRef<'a> {
    pub fn iter(
        self,
        layers: &'a LayerIds,
    ) -> impl Iterator<Item = EdgeStorageEntry<'a>> + Send + Sync + 'a {
        match self {
            EdgesStorageRef::Mem(storage) => StorageVariants3::Mem(
                storage
                    .iter()
                    .filter(move |e| e.has_layer(layers))
                    .map(EdgeStorageEntry::Mem),
            ),
            EdgesStorageRef::Unlocked(edges) => StorageVariants3::Unlocked(
                edges
                    .iter()
                    .filter(move |e| e.as_mem_edge().has_layer(layers))
                    .map(EdgeStorageEntry::Unlocked),
            ),
            #[cfg(feature = "storage")]
            EdgesStorageRef::Disk(storage) => {
                StorageVariants3::Disk(storage.iter(layers).map(EdgeStorageEntry::Disk))
            }
        }
    }

    pub fn par_iter(
        self,
        layers: &LayerIds,
    ) -> impl ParallelIterator<Item = EdgeStorageEntry<'a>> + use<'a, '_> {
        match self {
            EdgesStorageRef::Mem(storage) => StorageVariants3::Mem(
                storage
                    .par_iter()
                    .filter(move |e| e.has_layer(layers))
                    .map(EdgeStorageEntry::Mem),
            ),
            EdgesStorageRef::Unlocked(edges) => StorageVariants3::Unlocked(
                edges
                    .par_iter()
                    .filter(move |e| e.as_mem_edge().has_layer(layers))
                    .map(EdgeStorageEntry::Unlocked),
            ),
            #[cfg(feature = "storage")]
            EdgesStorageRef::Disk(storage) => {
                StorageVariants3::Disk(storage.par_iter(layers).map(EdgeStorageEntry::Disk))
            }
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
                _ => edges
                    .par_iter()
                    .filter(|e| e.as_mem_edge().has_layer(layers))
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
