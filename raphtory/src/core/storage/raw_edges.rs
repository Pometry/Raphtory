use super::{resolve, timeindex::TimeIndex};
use crate::{
    core::entities::{
        edges::edge_store::{EdgeDataLike, EdgeLayer, EdgeStore},
        LayerIds,
    },
    db::api::storage::graph::edges::edge_storage_ops::{EdgeStorageOps, MemEdge},
};
use lock_api::ArcRwLockReadGuard;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use raphtory_api::core::{entities::EID, storage::timeindex::TimeIndexEntry};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{self, AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct EdgeShard {
    edge_ids: Vec<EdgeStore>,
    props: Vec<Vec<EdgeLayer>>,
    additions: Vec<Vec<TimeIndex<TimeIndexEntry>>>,
    deletions: Vec<Vec<TimeIndex<TimeIndexEntry>>>,
}

#[must_use]
pub struct UninitialisedEdge<'a> {
    guard: RwLockWriteGuard<'a, EdgeShard>,
    offset: usize,
    value: EdgeStore,
}

impl<'a> UninitialisedEdge<'a> {
    pub fn init(mut self) -> EdgeWGuard<'a> {
        self.guard.insert(self.offset, self.value);
        EdgeWGuard {
            guard: self.guard,
            i: self.offset,
        }
    }

    pub fn value(&self) -> &EdgeStore {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut EdgeStore {
        &mut self.value
    }
}

impl EdgeShard {
    pub fn insert(&mut self, index: usize, value: EdgeStore) {
        if index >= self.edge_ids.len() {
            self.edge_ids.resize_with(index + 1, Default::default);
        }
        self.edge_ids[index] = value;
    }

    pub fn edge_store(&self, index: usize) -> &EdgeStore {
        &self.edge_ids[index]
    }

    pub fn internal_num_layers(&self) -> usize {
        self.additions.len().max(self.deletions.len())
    }

    pub fn additions(&self, index: usize, layer_id: usize) -> Option<&TimeIndex<TimeIndexEntry>> {
        self.additions.get(layer_id).and_then(|add| add.get(index))
    }

    pub fn deletions(&self, index: usize, layer_id: usize) -> Option<&TimeIndex<TimeIndexEntry>> {
        self.deletions.get(layer_id).and_then(|del| del.get(index))
    }

    pub fn props(&self, index: usize, layer_id: usize) -> Option<&EdgeLayer> {
        self.props.get(layer_id).and_then(|props| props.get(index))
    }

    pub fn props_iter(&self, index: usize) -> impl Iterator<Item = (usize, &EdgeLayer)> {
        self.props
            .iter()
            .enumerate()
            .filter_map(move |(id, layer)| layer.get(index).map(|l| (id, l)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgesStorage {
    shards: Arc<[Arc<RwLock<EdgeShard>>]>,
    len: Arc<AtomicUsize>,
}

impl PartialEq for EdgesStorage {
    fn eq(&self, other: &Self) -> bool {
        self.shards.len() == other.shards.len()
            && self
                .shards
                .iter()
                .zip(other.shards.iter())
                .all(|(a, b)| a.read().eq(&b.read()))
    }
}

impl Default for EdgesStorage {
    fn default() -> Self {
        Self::new(rayon::current_num_threads())
    }
}

impl EdgesStorage {
    pub fn new(num_shards: usize) -> Self {
        let shards = (0..num_shards).map(|_| {
            Arc::new(RwLock::new(EdgeShard {
                edge_ids: vec![],
                props: Vec::with_capacity(0),
                additions: Vec::with_capacity(1),
                deletions: Vec::with_capacity(0),
            }))
        });
        EdgesStorage {
            shards: shards.collect(),
            len: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(atomic::Ordering::SeqCst)
    }

    pub fn next_id(&self) -> EID {
        EID(self.len.fetch_add(1, Ordering::Relaxed))
    }

    pub fn read_lock(&self) -> LockedEdges {
        LockedEdges {
            shards: self
                .shards
                .iter()
                .map(|shard| Arc::new(shard.read_arc()))
                .collect(),
            len: self.len(),
        }
    }

    pub fn write_lock(&self) -> WriteLockedEdges {
        WriteLockedEdges {
            shards: self.shards.iter().map(|shard| shard.write()).collect(),
            global_len: &self.len,
        }
    }

    #[inline]
    fn resolve(&self, index: usize) -> (usize, usize) {
        resolve(index, self.shards.len())
    }

    pub(crate) fn push(&self, mut value: EdgeStore) -> UninitialisedEdge {
        let index = self.len.fetch_add(1, atomic::Ordering::Relaxed);
        value.eid = EID(index);
        let (bucket, offset) = self.resolve(index);
        let guard = self.shards[bucket].write();
        UninitialisedEdge {
            guard,
            offset,
            value,
        }
    }

    pub fn get_edge_mut(&self, eid: EID) -> EdgeWGuard {
        let (bucket, offset) = self.resolve(eid.into());
        EdgeWGuard {
            guard: self.shards[bucket].write(),
            i: offset,
        }
    }

    pub fn get_edge(&self, eid: EID) -> EdgeRGuard {
        let (bucket, offset) = self.resolve(eid.into());
        EdgeRGuard {
            guard: self.shards[bucket].read(),
            offset,
        }
    }
}

pub struct EdgeWGuard<'a> {
    guard: RwLockWriteGuard<'a, EdgeShard>,
    i: usize,
}

impl<'a> EdgeWGuard<'a> {
    pub fn as_mut(&mut self) -> MutEdge {
        MutEdge {
            guard: self.guard.deref_mut(),
            i: self.i,
        }
    }

    pub fn as_ref(&self) -> MemEdge {
        MemEdge::new(&self.guard, self.i)
    }

    pub fn eid(&self) -> EID {
        self.as_ref().eid()
    }
}

pub struct MutEdge<'a> {
    guard: &'a mut EdgeShard,
    i: usize,
}

impl<'a> MutEdge<'a> {
    pub fn edge_store_mut(&mut self) -> &mut EdgeStore {
        &mut self.guard.edge_ids[self.i]
    }

    pub fn deletions_mut(&mut self, layer_id: usize) -> &mut TimeIndex<TimeIndexEntry> {
        if layer_id >= self.guard.deletions.len() {
            self.guard
                .deletions
                .resize_with(layer_id + 1, Default::default);
        }
        if self.i >= self.guard.deletions[layer_id].len() {
            self.guard.deletions[layer_id].resize_with(self.i + 1, Default::default);
        }
        &mut self.guard.deletions[layer_id][self.i]
    }

    pub fn additions_mut(&mut self, layer_id: usize) -> &mut TimeIndex<TimeIndexEntry> {
        if layer_id >= self.guard.additions.len() {
            self.guard
                .additions
                .resize_with(layer_id + 1, Default::default);
        }
        if self.i >= self.guard.additions[layer_id].len() {
            self.guard.additions[layer_id].resize_with(self.i + 1, Default::default);
        }
        &mut self.guard.additions[layer_id][self.i]
    }

    pub fn layer_mut(&mut self, layer_id: usize) -> &mut EdgeLayer {
        if layer_id >= self.guard.props.len() {
            self.guard.props.resize_with(layer_id + 1, Default::default);
        }
        if self.i >= self.guard.props[layer_id].len() {
            self.guard.props[layer_id].resize_with(self.i + 1, Default::default);
        }

        &mut self.guard.props[layer_id][self.i]
    }
}

#[derive(Debug)]
pub struct EdgeRGuard<'a> {
    guard: RwLockReadGuard<'a, EdgeShard>,
    offset: usize,
}

impl<'a> EdgeRGuard<'a> {
    pub fn as_mem_edge(&self) -> MemEdge {
        MemEdge::new(&self.guard, self.offset)
    }

    pub fn has_layer(&self, layers: &LayerIds) -> bool {
        self.as_mem_edge().has_layer(layers)
    }

    pub fn layer_iter(
        &self,
    ) -> impl Iterator<Item = (usize, impl Deref<Target = EdgeLayer> + '_)> + '_ {
        self.guard.props_iter(self.offset)
    }

    pub(crate) fn temp_prop_ids(
        &self,
        layer_id: Option<usize>,
    ) -> Box<dyn Iterator<Item = usize> + Send + '_> {
        if let Some(layer_id) = layer_id {
            Box::new(
                self.guard
                    .props(self.offset, layer_id)
                    .into_iter()
                    .flat_map(|layer| layer.temporal_prop_ids()),
            )
        } else {
            Box::new(
                self.guard
                    .props_iter(self.offset)
                    .flat_map(|(_, layer)| layer.temporal_prop_ids()),
            )
        }
    }

    pub(crate) fn layer(&self, layer_id: usize) -> Option<impl Deref<Target = EdgeLayer> + '_> {
        self.guard.props(self.offset, layer_id)
    }
}

#[derive(Debug)]
pub struct LockedEdges {
    shards: Arc<[Arc<ArcRwLockReadGuard<parking_lot::RawRwLock, EdgeShard>>]>,
    len: usize,
}

impl LockedEdges {
    pub fn get_mem(&self, eid: EID) -> MemEdge {
        let (bucket, offset) = resolve(eid.into(), self.shards.len());
        MemEdge::new(&self.shards[bucket], offset)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> impl Iterator<Item = MemEdge> + '_ {
        self.shards.iter().flat_map(|shard| {
            shard
                .edge_ids
                .iter()
                .enumerate()
                .map(move |(offset, _)| MemEdge::new(shard, offset))
        })
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = MemEdge> + '_ {
        self.shards.par_iter().flat_map(|shard| {
            shard
                .edge_ids
                .par_iter()
                .enumerate()
                .map(move |(offset, _)| MemEdge::new(shard, offset))
        })
    }
}

pub struct EdgeShardWriter<'a, S> {
    shard: S,
    shard_id: usize,
    num_shards: usize,
    global_len: &'a AtomicUsize,
}

impl<'a, S> EdgeShardWriter<'a, S>
where
    S: DerefMut<Target = EdgeShard>,
{
    /// Map an edge id to local offset if it is in the shard
    fn resolve(&self, eid: EID) -> Option<usize> {
        let EID(eid) = eid;
        let (bucket, offset) = resolve(eid, self.num_shards);
        (bucket == self.shard_id).then_some(offset)
    }

    pub fn get_mut(&mut self, eid: EID) -> Option<MutEdge> {
        let offset = self.resolve(eid)?;
        if self.shard.edge_ids.len() <= offset {
            self.global_len.fetch_max(eid.0 + 1, Ordering::Relaxed);
            self.shard
                .edge_ids
                .resize_with(offset + 1, EdgeStore::default)
        }
        Some(MutEdge {
            guard: self.shard.deref_mut(),
            i: offset,
        })
    }

    pub fn shard_id(&self) -> usize {
        self.shard_id
    }
}

pub struct WriteLockedEdges<'a> {
    shards: Vec<RwLockWriteGuard<'a, EdgeShard>>,
    global_len: &'a AtomicUsize,
}

impl<'a> WriteLockedEdges<'a> {
    pub fn par_iter_mut(
        &mut self,
    ) -> impl IndexedParallelIterator<Item = EdgeShardWriter<&mut EdgeShard>> + '_ {
        let num_shards = self.shards.len();
        let shards: Vec<_> = self
            .shards
            .iter_mut()
            .map(|shard| shard.deref_mut())
            .collect();
        let global_len = self.global_len;
        shards
            .into_par_iter()
            .enumerate()
            .map(move |(shard_id, shard)| EdgeShardWriter {
                shard,
                shard_id,
                num_shards,
                global_len,
            })
    }

    pub fn into_par_iter_mut(
        self,
    ) -> impl IndexedParallelIterator<Item = EdgeShardWriter<'a, RwLockWriteGuard<'a, EdgeShard>>> + 'a
    {
        let num_shards = self.shards.len();
        let global_len = self.global_len;
        self.shards
            .into_par_iter()
            .enumerate()
            .map(move |(shard_id, shard)| EdgeShardWriter {
                shard,
                shard_id,
                num_shards,
                global_len,
            })
    }

    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }
}
