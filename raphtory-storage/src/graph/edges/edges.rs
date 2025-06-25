use super::{edge_entry::EdgeStorageEntry, unlocked::UnlockedEdges};
use crate::graph::edges::edge_ref::EdgeStorageRef;
use raphtory_api::core::entities::{LayerIds, EID};
use rayon::iter::ParallelIterator;
use std::sync::Arc;
use storage::{utils::Iter2, Extension, ReadLockedEdges};

pub struct EdgesStorage {
    storage: Arc<ReadLockedEdges<Extension>>,
}

impl EdgesStorage {
    pub fn new(storage: Arc<ReadLockedEdges<Extension>>) -> Self {
        Self { storage }
    }

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
            EdgesStorageRef::Mem(storage) => {
                Iter2::I1(storage.iter(layers).map(EdgeStorageEntry::Mem))
            }
            EdgesStorageRef::Unlocked(edges) => Iter2::I2(edges.iter(layers)),
        }
    }

    pub fn par_iter(
        self,
        layers: &'a LayerIds,
    ) -> impl ParallelIterator<Item = EdgeStorageEntry<'a>> + use<'a> {
        match self {
            EdgesStorageRef::Mem(storage) => {
                Iter2::I1(storage.par_iter(layers).map(EdgeStorageEntry::Mem))
            }
            EdgesStorageRef::Unlocked(edges) => Iter2::I2(edges.par_iter(layers)),
        }
    }

    #[inline]
    pub fn count(self, layers: &LayerIds) -> usize {
        match self {
            EdgesStorageRef::Mem(storage) => match layers {
                LayerIds::None => 0,
                LayerIds::All => storage.storage().num_edges(),
                LayerIds::One(layer_id) => storage.storage().num_edges_layer(*layer_id),
                _ => self.par_iter(layers).count(),
            },
            EdgesStorageRef::Unlocked(edges) => match layers {
                LayerIds::None => 0,
                LayerIds::One(layer_id) => edges.storage().num_edges_layer(*layer_id),
                LayerIds::All => edges.storage().num_edges_layer(0),
                _ => self.par_iter(layers).count(),
            },
        }
    }

    #[inline]
    pub fn edge(self, edge: EID) -> EdgeStorageEntry<'a> {
        match self {
            EdgesStorageRef::Mem(storage) => EdgeStorageEntry::Mem(storage.edge_ref(edge)),
            EdgesStorageRef::Unlocked(storage) => storage.edge(edge),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            EdgesStorageRef::Mem(storage) => storage.storage().num_edges(),
            EdgesStorageRef::Unlocked(storage) => storage.storage().num_edges(),
        }
    }
}
