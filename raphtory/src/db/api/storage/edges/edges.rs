use crate::{
    core::{
        entities::{edges::edge_store::EdgeStore, LayerIds, EID},
        storage::ReadLockedStorage,
    },
    db::api::storage::edges::edge_ref::EdgeStorageRef,
};

#[cfg(feature = "arrow")]
use crate::arrow::storage_interface::edges_ref::ArrowEdgesRef;

#[cfg(feature = "arrow")]
use crate::arrow::storage_interface::edges::ArrowEdges;
use crate::db::api::storage::edges::edge_storage_ops::EdgeStorageOps;
use either::Either;
use rayon::iter::ParallelIterator;
use std::sync::Arc;

pub enum EdgesStorage {
    Mem(Arc<ReadLockedStorage<EdgeStore, EID>>),
    #[cfg(feature = "arrow")]
    Arrow(ArrowEdges),
}

impl EdgesStorage {
    #[inline]
    pub fn as_ref(&self) -> EdgesStorageRef {
        match self {
            EdgesStorage::Mem(storage) => EdgesStorageRef::Mem(storage),
            #[cfg(feature = "arrow")]
            EdgesStorage::Arrow(storage) => EdgesStorageRef::Arrow(storage.as_ref()),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum EdgesStorageRef<'a> {
    Mem(&'a ReadLockedStorage<EdgeStore, EID>),
    #[cfg(feature = "arrow")]
    Arrow(ArrowEdgesRef<'a>),
}

impl<'a> EdgesStorageRef<'a> {
    #[allow(unused_variables)]
    pub fn get_layer(self, eid: EID, layer_id: usize) -> EdgeStorageRef<'a> {
        match self {
            EdgesStorageRef::Mem(storage) => EdgeStorageRef::Mem(storage.get(eid)),
            #[cfg(feature = "arrow")]
            EdgesStorageRef::Arrow(storage) => EdgeStorageRef::Arrow(storage.edge(eid, layer_id)),
        }
    }

    #[inline]
    pub fn get(self, eid: EID) -> EdgeStorageRef<'a> {
        match self {
            EdgesStorageRef::Mem(storage) => EdgeStorageRef::Mem(storage.get(eid)),
            #[cfg(feature = "arrow")]
            EdgesStorageRef::Arrow(_) => {
                todo!("getting multilayer edge not implemented for arrow graph")
            }
        }
    }

    #[cfg(feature = "arrow")]
    pub fn iter(self, layers: LayerIds) -> impl Iterator<Item = EdgeStorageRef<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => Either::Left(
                storage
                    .iter()
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageRef::Mem),
            ),
            EdgesStorageRef::Arrow(storage) => {
                Either::Right(storage.iter(layers).map(EdgeStorageRef::Arrow))
            }
        }
    }

    #[cfg(not(feature = "arrow"))]
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

    #[cfg(feature = "arrow")]
    pub fn par_iter(self, layers: LayerIds) -> impl ParallelIterator<Item = EdgeStorageRef<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => Either::Left(
                storage
                    .par_iter()
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageRef::Mem),
            ),
            EdgesStorageRef::Arrow(storage) => {
                Either::Right(storage.par_iter(layers).map(EdgeStorageRef::Arrow))
            }
        }
    }

    #[cfg(not(feature = "arrow"))]
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
            #[cfg(feature = "arrow")]
            EdgesStorageRef::Arrow(storage) => storage.count(layers),
        }
    }
}
