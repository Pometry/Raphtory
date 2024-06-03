use crate::{
    core::{
        entities::{edges::edge_store::EdgeStore, LayerIds, EID},
        storage::ReadLockedStorage,
    },
    db::api::storage::edges::edge_ref::EdgeStorageRef,
};

#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::edges_ref::ArrowEdgesRef;

#[cfg(feature = "storage")]
use crate::disk_graph::storage_interface::edges::ArrowEdges;
use crate::db::api::storage::edges::edge_storage_ops::EdgeStorageOps;
use either::Either;
use rayon::iter::ParallelIterator;
use std::sync::Arc;

pub enum EdgesStorage {
    Mem(Arc<ReadLockedStorage<EdgeStore, EID>>),
    #[cfg(feature = "storage")]
    Disk(ArrowEdges),
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

#[derive(Copy, Clone, Debug)]
pub enum EdgesStorageRef<'a> {
    Mem(&'a ReadLockedStorage<EdgeStore, EID>),
    #[cfg(feature = "storage")]
    Disk(ArrowEdgesRef<'a>),
}

impl<'a> EdgesStorageRef<'a> {
    #[cfg(feature = "storage")]
    pub fn iter(self, layers: LayerIds) -> impl Iterator<Item = EdgeStorageRef<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => Either::Left(
                storage
                    .iter()
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageRef::Mem),
            ),
            EdgesStorageRef::Disk(storage) => {
                Either::Right(storage.iter(layers).map(EdgeStorageRef::Disk))
            }
        }
    }

    #[cfg(not(feature = "storage"))]
    pub fn iter(self, layers: LayerIds) -> impl Iterator<Item = EdgeStorageRef<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => {
                Either::<_, std::iter::Empty<EdgeStorageRef<'a>>>::Left(
                    storage
                        .iter()
                        .filter(move |e| e.has_layer(&layers))
                        .map(EdgeStorageRef::Mem),
                )
            }
        }
    }

    #[cfg(feature = "storage")]
    pub fn par_iter(self, layers: LayerIds) -> impl ParallelIterator<Item = EdgeStorageRef<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => Either::Left(
                storage
                    .par_iter()
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageRef::Mem),
            ),
            EdgesStorageRef::Disk(storage) => {
                Either::Right(storage.par_iter(layers).map(EdgeStorageRef::Disk))
            }
        }
    }

    #[cfg(not(feature = "storage"))]
    pub fn par_iter(self, layers: LayerIds) -> impl ParallelIterator<Item = EdgeStorageRef<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => {
                Either::<_, rayon::iter::Empty<EdgeStorageRef<'a>>>::Left(
                    storage
                        .par_iter()
                        .filter(move |e| e.has_layer(&layers))
                        .map(EdgeStorageRef::Mem),
                )
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
            #[cfg(feature = "storage")]
            EdgesStorageRef::Disk(storage) => storage.count(layers),
        }
    }
}
