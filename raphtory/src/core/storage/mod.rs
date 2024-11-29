use crate::core::entities::nodes::node_store::NodeStore;
use lazy_vec::LazyVec;
use lock_api;
use node_entry::NodeEntry;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use raphtory_api::core::{
    entities::{GidRef, VID},
    storage::arc_str::ArcStr,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Deref, DerefMut, Index, IndexMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use super::Prop;

pub mod lazy_vec;
pub mod locked_view;
pub mod node_entry;
pub mod raw_edges;
pub mod timeindex;

type ArcRwLockReadGuard<T> = lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, T>;
#[must_use]
pub struct UninitialisedEntry<'a, T, TS> {
    offset: usize,
    guard: RwLockWriteGuard<'a, TS>,
    value: T,
}

impl<'a, T: Default, TS: DerefMut<Target = Vec<T>>> UninitialisedEntry<'a, T, TS> {
    pub fn init(mut self) {
        if self.offset >= self.guard.len() {
            self.guard.resize_with(self.offset + 1, Default::default);
        }
        self.guard[self.offset] = self.value;
    }
    pub fn value(&self) -> &T {
        &self.value
    }
}

#[inline]
fn resolve(index: usize, num_buckets: usize) -> (usize, usize) {
    let bucket = index % num_buckets;
    let offset = index / num_buckets;
    (bucket, offset)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeVec {
    data: Arc<RwLock<NodeSlot>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct NodeSlot {
    nodes: Vec<NodeStore>,
    t_props_log: TColumns, // not the same size as nodes
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct TColumns {
    t_props_log: LazyVec<TPropColumn>,
    num_rows: usize,
}

impl TColumns {
    pub fn push(&mut self, prop_id: usize, prop: Prop) -> usize {
        let id = self.num_rows;
        self.t_props_log.upsert(prop_id, |col| col.push(Some(prop)));
        for col in self.t_props_log.values_mut() {
            if let Some((id, col)) = col {
                if id != prop_id {
                    col.push(None)
                }
            }
        }
        id
    }

    pub fn get(&self, prop_id: usize) -> Option<&TPropColumn> {
        self.t_props_log.get(prop_id).map(|col| col)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub(crate) enum TPropColumn {
    #[default]
    Empty,
    U64(LazyVec<u64>),
    Str(LazyVec<ArcStr>),
}

impl TPropColumn {
    pub(crate) fn push(&mut self, prop: Option<Prop>) {
        match (self, prop) {
            (col @ TPropColumn::Empty, prop @ Some(Prop::U64(_))) => {
                *col = TPropColumn::U64(Default::default());
                col.push(prop);
            }
            (col @ TPropColumn::Empty, prop @ Some(Prop::Str(_))) => {
                *col = TPropColumn::Str(Default::default());
                col.push(prop)
            }
            (TPropColumn::U64(col), Some(Prop::U64(v))) => col.push(Some(v)),
            (TPropColumn::Str(col), Some(Prop::Str(v))) => col.push(Some(v)),
            _ => panic!("Invalid prop type"),
        }
    }

    pub(crate) fn get(&self, index: usize) -> Option<Prop> {
        match self {
            TPropColumn::U64(col) => col.get(index).map(|prop| (*prop).into()),
            TPropColumn::Str(col) => col.get(index).map(|prop| prop.into()),
            _ => None,
        }
    }
}

impl NodeSlot {
    pub fn t_props_log(&self) -> &TColumns {
        &self.t_props_log
    }

    pub fn t_props_log_mut(&mut self) -> &mut TColumns {
        &mut self.t_props_log
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeEntry> {
        self.nodes
            .iter()
            .map(|ns| NodeEntry::new(ns, &self.t_props_log))
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = NodeEntry> {
        self.nodes
            .par_iter()
            .map(|ns| NodeEntry::new(ns, &self.t_props_log))
    }
}

impl Index<usize> for NodeSlot {
    type Output = NodeStore;

    fn index(&self, index: usize) -> &Self::Output {
        &self.nodes[index]
    }
}

impl IndexMut<usize> for NodeSlot {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.nodes[index]
    }
}

impl Deref for NodeSlot {
    type Target = Vec<NodeStore>;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl DerefMut for NodeSlot {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.nodes
    }
}

impl PartialEq for NodeVec {
    fn eq(&self, other: &Self) -> bool {
        let a = self.data.read();
        let b = other.data.read();
        a.deref() == b.deref()
    }
}

impl Default for NodeVec {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeVec {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(Default::default())),
        }
    }

    #[inline]
    pub fn read_arc_lock(&self) -> ArcRwLockReadGuard<NodeSlot> {
        RwLock::read_arc(&self.data)
    }

    #[inline]
    pub fn write(&self) -> impl DerefMut<Target = NodeSlot> + '_ {
        self.data.write()
    }

    #[inline]
    pub fn read(&self) -> impl Deref<Target = NodeSlot> + '_ {
        self.data.read()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NodeStorage {
    pub(crate) data: Box<[NodeVec]>,
    len: AtomicUsize,
}

impl PartialEq for NodeStorage {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}

#[derive(Debug)]
pub struct ReadLockedStorage<Index = usize> {
    pub(crate) locks: Vec<Arc<ArcRwLockReadGuard<NodeSlot>>>,
    len: usize,
    _index: PhantomData<Index>,
}

impl<Index> ReadLockedStorage<Index>
where
    usize: From<Index>,
{
    fn resolve(&self, index: Index) -> (usize, usize) {
        let index: usize = index.into();
        let n = self.locks.len();
        let bucket = index % n;
        let offset = index / n;
        (bucket, offset)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn get(&self, index: Index) -> &NodeStore {
        let (bucket, offset) = self.resolve(index);
        let bucket = &self.locks[bucket];
        &bucket[offset]
    }

    pub(crate) fn arc_entry(&self, index: Index) -> ArcEntry {
        let (bucket, offset) = self.resolve(index);
        ArcEntry {
            guard: self.locks[bucket].clone(),
            i: offset,
        }
    }

    pub(crate) fn get_entry(&self, index: Index) -> NodeEntry {
        let (bucket, offset) = self.resolve(index);
        let bucket = &self.locks[bucket];
        NodeEntry::new(&bucket[offset], &bucket.t_props_log)
    }

    pub(crate) fn t_props_log(&self, index: Index) -> &TColumns {
        let (bucket, _) = self.resolve(index);
        self.locks[bucket].t_props_log()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = NodeEntry> + '_ {
        self.locks.iter().flat_map(|v| v.iter())
    }

    pub(crate) fn par_iter(&self) -> impl ParallelIterator<Item = NodeEntry> + '_ {
        self.locks.par_iter().flat_map(|v| v.par_iter())
    }

    #[allow(unused)]
    pub(crate) fn into_iter(self) -> impl Iterator<Item = ArcEntry> {
        self.locks.into_iter().flat_map(|data| {
            (0..data.len()).map(move |offset| ArcEntry {
                guard: data.clone(),
                i: offset,
            })
        })
    }

    #[allow(unused)]
    pub(crate) fn into_par_iter(self) -> impl ParallelIterator<Item = ArcEntry> {
        self.locks.into_par_iter().flat_map(|data| {
            (0..data.len()).into_par_iter().map(move |offset| ArcEntry {
                guard: data.clone(),
                i: offset,
            })
        })
    }
}

impl NodeStorage {
    pub fn count_with_filter<F: Fn(NodeEntry<'_>) -> bool + Send + Sync>(&self, f: F) -> usize {
        self.read_lock().par_iter().filter(|x| f(*x)).count()
    }
}

impl NodeStorage {
    #[inline]
    fn resolve(&self, index: usize) -> (usize, usize) {
        resolve(index, self.data.len())
    }

    #[inline]
    pub fn read_lock(&self) -> ReadLockedStorage<VID> {
        let guards = self
            .data
            .iter()
            .map(|v| Arc::new(v.read_arc_lock()))
            .collect();
        ReadLockedStorage {
            locks: guards,
            len: self.len(),
            _index: PhantomData,
        }
    }

    pub(crate) fn write_lock(&self) -> WriteLockedNodes {
        WriteLockedNodes {
            guards: self.data.iter().map(|lock| lock.data.write()).collect(),
            global_len: &self.len,
        }
    }

    pub fn new(n_locks: usize) -> Self {
        let data: Box<[NodeVec]> = (0..n_locks)
            .map(|_| NodeVec::new())
            .collect::<Vec<_>>()
            .into();

        Self {
            data,
            len: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, mut value: NodeStore) -> UninitialisedEntry<NodeStore, NodeSlot> {
        let index = self.len.fetch_add(1, Ordering::Relaxed);
        value.vid = VID(index);
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.write();
        UninitialisedEntry {
            offset,
            guard,
            value,
        }
    }

    pub fn set(&self, value: NodeStore) {
        let VID(index) = value.vid;
        self.len.fetch_max(index + 1, Ordering::Relaxed);
        let (bucket, offset) = self.resolve(index);
        let mut guard = self.data[bucket].data.write();
        if guard.len() <= offset {
            guard.resize_with(offset + 1, NodeStore::default)
        }
        guard[offset] = value
    }

    #[inline]
    pub fn entry(&self, index: VID) -> Entry<'_> {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.read_recursive();
        Entry { offset, guard }
    }

    pub fn entry_arc(&self, index: VID) -> ArcEntry {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = &self.data[bucket].data;
        let arc_guard = RwLock::read_arc_recursive(guard);
        ArcEntry {
            i: offset,
            guard: Arc::new(arc_guard),
        }
    }

    pub fn entry_mut(&self, index: VID) -> EntryMut {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.write();
        EntryMut { i: offset, guard }
    }

    pub fn prop_entry_mut(&self, index: VID) -> impl DerefMut<Target = TColumns> + '_ {
        let index = index.into();
        let (bucket, _) = self.resolve(index);
        let lock = self.data[bucket].data.write();
        RwLockWriteGuard::map(lock, |data| &mut data.t_props_log)
    }

    // This helps get the right locks when adding an edge
    pub fn pair_entry_mut(&self, i: VID, j: VID) -> PairEntryMut<'_> {
        let i = i.into();
        let j = j.into();
        let (bucket_i, offset_i) = self.resolve(i);
        let (bucket_j, offset_j) = self.resolve(j);
        // always acquire lock for smaller bucket first to avoid deadlock between two updates for the same pair of buckets
        if bucket_i < bucket_j {
            let guard_i = self.data[bucket_i].data.write();
            let guard_j = self.data[bucket_j].data.write();
            PairEntryMut::Different {
                i: offset_i,
                j: offset_j,
                guard1: guard_i,
                guard2: guard_j,
            }
        } else if bucket_i > bucket_j {
            let guard_j = self.data[bucket_j].data.write();
            let guard_i = self.data[bucket_i].data.write();
            PairEntryMut::Different {
                i: offset_i,
                j: offset_j,
                guard1: guard_i,
                guard2: guard_j,
            }
        } else {
            PairEntryMut::Same {
                i: offset_i,
                j: offset_j,
                guard: self.data[bucket_i].data.write(),
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    pub fn next_id(&self) -> VID {
        VID(self.len.fetch_add(1, Ordering::Relaxed))
    }
}

pub struct WriteLockedNodes<'a> {
    guards: Vec<RwLockWriteGuard<'a, NodeSlot>>,
    global_len: &'a AtomicUsize,
}

pub struct NodeShardWriter<'a, S> {
    shard: S,
    shard_id: usize,
    num_shards: usize,
    global_len: &'a AtomicUsize,
}

impl<'a, S> NodeShardWriter<'a, S>
where
    S: DerefMut<Target = NodeSlot>,
{
    #[inline]
    fn resolve(&self, index: VID) -> Option<usize> {
        let (shard_id, offset) = resolve(index.into(), self.num_shards);
        (shard_id == self.shard_id).then_some(offset)
    }

    #[inline]
    pub fn get_mut(&mut self, index: VID) -> Option<&mut NodeStore> {
        self.resolve(index).map(|offset| &mut self.shard[offset])
    }

    #[inline]
    pub fn get(&self, index: VID) -> Option<&NodeStore> {
        self.resolve(index).map(|offset| &self.shard[offset])
    }

    pub fn set(&mut self, vid: VID, gid: GidRef) -> Option<&mut NodeStore> {
        self.resolve(vid).map(|offset| {
            if offset >= self.shard.len() {
                self.shard.resize_with(offset + 1, NodeStore::default);
                self.global_len
                    .fetch_max(vid.index() + 1, Ordering::Relaxed);
            }
            self.shard[offset] = NodeStore::resolved(gid.to_owned(), vid);
            &mut self.shard[offset]
        })
    }

    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    fn resize(&mut self, new_global_len: usize) {
        let mut new_len = new_global_len / self.num_shards;
        if self.shard_id < new_global_len % self.num_shards {
            new_len += 1;
        }
        if new_len > self.shard.len() {
            self.shard.resize_with(new_len, Default::default);
            self.global_len.fetch_max(new_global_len, Ordering::Relaxed);
        }
    }
}

impl<'a> WriteLockedNodes<'a> {
    pub fn par_iter_mut(
        &mut self,
    ) -> impl IndexedParallelIterator<Item = NodeShardWriter<&mut NodeSlot>> + '_ {
        let num_shards = self.guards.len();
        let global_len = self.global_len;
        let shards: Vec<&mut NodeSlot> = self
            .guards
            .iter_mut()
            .map(|guard| guard.deref_mut())
            .collect();
        shards
            .into_par_iter()
            .enumerate()
            .map(move |(shard_id, shard)| NodeShardWriter {
                shard,
                shard_id,
                num_shards,
                global_len,
            })
    }

    pub fn into_par_iter_mut(
        self,
    ) -> impl IndexedParallelIterator<Item = NodeShardWriter<'a, RwLockWriteGuard<'a, NodeSlot>>> + 'a
    {
        let num_shards = self.guards.len();
        let global_len = self.global_len;
        self.guards
            .into_par_iter()
            .enumerate()
            .map(move |(shard_id, shard)| NodeShardWriter {
                shard,
                shard_id,
                num_shards,
                global_len,
            })
    }

    pub fn resize(&mut self, new_len: usize) {
        self.par_iter_mut()
            .for_each(|mut shard| shard.resize(new_len))
    }

    pub fn num_shards(&self) -> usize {
        self.guards.len()
    }
}

#[derive(Debug)]
pub struct Entry<'a> {
    offset: usize,
    guard: RwLockReadGuard<'a, NodeSlot>,
}

impl Entry<'_> {
    #[inline]
    pub fn get(&self) -> &NodeStore {
        &self.guard[self.offset]
    }

    #[inline]
    pub fn get_entry(&self) -> NodeEntry<'_> {
        NodeEntry::new(&self.guard[self.offset], &self.guard.t_props_log)
    }

    #[inline]
    pub fn t_props_log(&self) -> &TColumns {
        &self.guard.t_props_log
    }
}

#[derive(Debug)]
pub struct ArcEntry {
    guard: Arc<ArcRwLockReadGuard<NodeSlot>>,
    i: usize,
}

impl ArcEntry {
    #[inline]
    pub fn get(&self) -> &NodeStore {
        &self.guard[self.i]
    }

    #[inline]
    pub fn t_props_log(&self) -> &TColumns {
        &self.guard.t_props_log
    }
}

impl Clone for ArcEntry {
    fn clone(&self) -> Self {
        Self {
            guard: self.guard.clone(),
            i: self.i,
        }
    }
}

pub enum PairEntryMut<'a> {
    Same {
        i: usize,
        j: usize,
        guard: parking_lot::RwLockWriteGuard<'a, NodeSlot>,
    },
    Different {
        i: usize,
        j: usize,
        guard1: parking_lot::RwLockWriteGuard<'a, NodeSlot>,
        guard2: parking_lot::RwLockWriteGuard<'a, NodeSlot>,
    },
}

impl<'a> PairEntryMut<'a> {
    pub(crate) fn get_i(&self) -> &NodeStore {
        match self {
            PairEntryMut::Same { i, guard, .. } => &guard[*i],
            PairEntryMut::Different { i, guard1, .. } => &guard1[*i],
        }
    }
    pub(crate) fn get_mut_i(&mut self) -> &mut NodeStore {
        match self {
            PairEntryMut::Same { i, guard, .. } => &mut guard[*i],
            PairEntryMut::Different { i, guard1, .. } => &mut guard1[*i],
        }
    }

    pub(crate) fn get_j(&self) -> &NodeStore {
        match self {
            PairEntryMut::Same { j, guard, .. } => &guard[*j],
            PairEntryMut::Different { j, guard2, .. } => &guard2[*j],
        }
    }

    pub(crate) fn get_mut_j(&mut self) -> &mut NodeStore {
        match self {
            PairEntryMut::Same { j, guard, .. } => &mut guard[*j],
            PairEntryMut::Different { j, guard2, .. } => &mut guard2[*j],
        }
    }
}

pub struct EntryMut<'a> {
    i: usize,
    guard: parking_lot::RwLockWriteGuard<'a, NodeSlot>,
}

impl<'a> EntryMut<'a> {
    pub fn get(&self) -> &NodeStore {
        &self.guard[self.i]
    }

    pub fn get_mut(&mut self) -> &mut NodeStore {
        &mut self.guard[self.i]
    }

    pub fn t_props_log(&self) -> &TColumns {
        &self.guard.t_props_log
    }

    pub fn t_props_log_mut(&mut self) -> &mut TColumns {
        &mut self.guard.t_props_log
    }
}

#[cfg(test)]
mod test {
    use super::NodeStorage;
    use crate::core::entities::nodes::node_store::NodeStore;
    use pretty_assertions::assert_eq;
    use quickcheck_macros::quickcheck;
    use raphtory_api::core::entities::{GID, VID};
    use rayon::prelude::*;
    use std::borrow::Cow;

    #[test]
    fn add_5_values_to_storage() {
        let storage = NodeStorage::new(2);

        for i in 0..5 {
            storage.push(NodeStore::empty(i.into())).init();
        }

        assert_eq!(storage.len(), 5);

        for i in 0..5 {
            let entry = storage.entry(VID(i));
            assert_eq!(entry.get().vid, VID(i));
        }

        let items_iter = storage.read_lock().into_iter();

        let actual = items_iter.map(|s| s.get().vid.index()).collect::<Vec<_>>();

        assert_eq!(actual, vec![0, 2, 4, 1, 3]);
    }

    #[test]
    fn test_index_correctness() {
        let storage = NodeStorage::new(2);

        for i in 0..5 {
            storage.push(NodeStore::empty(i.into())).init();
        }
        let locked = storage.read_lock();
        let actual: Vec<_> = (0..5)
            .map(|i| (i, locked.get(VID(i)).global_id.to_str()))
            .collect();

        assert_eq!(
            actual,
            vec![
                (0usize, Cow::Borrowed("0")),
                (1, "1".into()),
                (2, "2".into()),
                (3, "3".into()),
                (4, "4".into())
            ]
        );
    }

    #[test]
    fn test_entry() {
        let storage = NodeStorage::new(2);

        for i in 0..5 {
            storage.push(NodeStore::empty(i.into())).init();
        }

        for i in 0..5 {
            let entry = storage.entry(VID(i));
            assert_eq!(*entry.get().global_id.to_str(), i.to_string());
        }
    }

    #[quickcheck]
    fn concurrent_push(v: Vec<u64>) -> bool {
        let storage = NodeStorage::new(16);
        let mut expected = v
            .into_par_iter()
            .map(|v| {
                storage.push(NodeStore::empty(GID::U64(v))).init();
                v
            })
            .collect::<Vec<_>>();

        let locked = storage.read_lock();
        let mut actual: Vec<_> = locked
            .iter()
            .map(|n| n.node().global_id.as_u64().unwrap())
            .collect();

        actual.sort();
        expected.sort();

        actual == expected
    }
}
