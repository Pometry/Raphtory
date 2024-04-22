use crate::{
    arrow::edge::Edge,
    core::{
        entities::{edges::edge_store::EdgeStore, LayerIds, EID},
        storage::ReadLockedStorage,
    },
    db::api::storage::{
        arrow::edges::{ArrowEdges, ArrowEdgesRef},
        edges::edge_ref::EdgeStorageRef,
    },
};
use arrow2::Either;
use rayon::iter::ParallelIterator;

pub enum EdgesStorage {
    Mem(ReadLockedStorage<EdgeStore, EID>),
    #[cfg(feature = "arrow")]
    Arrow(ArrowEdges),
}

impl EdgesStorage {
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
    pub fn get_layer(self, eid: EID, layer_id: usize) -> EdgeStorageRef<'a> {
        match self {
            EdgesStorageRef::Mem(storage) => EdgeStorageRef::Mem(storage.get(eid)),
            #[cfg(feature = "arrow")]
            EdgesStorageRef::Arrow(storage) => EdgeStorageRef::Arrow(storage.edge(eid, layer_id)),
        }
    }

    pub fn get(self, eid: EID) -> EdgeStorageRef<'a> {
        match self {
            EdgesStorageRef::Mem(storage) => EdgeStorageRef::Mem(storage.get(eid)),
            #[cfg(feature = "arrow")]
            EdgesStorageRef::Arrow(_) => {
                todo!("getting multilayer edge not implemented for arrow graph")
            }
        }
    }

    pub fn iter(self, layers: LayerIds) -> impl Iterator<Item = EdgeStorageRef<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => Either::Left(
                storage
                    .iter()
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageRef::Mem),
            ),
            #[cfg(feature = "arrow")]
            EdgesStorageRef::Arrow(storage) => {
                Either::Right(storage.iter(layers).map(EdgeStorageRef::Arrow))
            }
        }
    }

    pub fn par_iter(self, layers: LayerIds) -> impl ParallelIterator<Item = EdgeStorageRef<'a>> {
        match self {
            EdgesStorageRef::Mem(storage) => Either::Left(
                storage
                    .par_iter()
                    .filter(move |e| e.has_layer(&layers))
                    .map(EdgeStorageRef::Mem),
            ),
            #[cfg(feature = "arrow")]
            EdgesStorageRef::Arrow(storage) => {
                Either::Right(storage.par_iter(layers).map(EdgeStorageRef::Arrow))
            }
        }
    }
}
