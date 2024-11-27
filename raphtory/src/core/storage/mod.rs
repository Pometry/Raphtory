use crate::{ core::entities::nodes::node_store::NodeStore, prelude::Prop };
use lock_api;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use raphtory_api::core::entities::{GidRef, VID};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

pub mod lazy_vec;
pub mod locked_view;
pub mod raw_edges;
pub mod timeindex;

type ArcRwLockReadGuard<T> = lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, T>;
#[must_use]
pub struct UninitialisedEntry<'a, T> {
    offset: usize,
    guard: RwLockWriteGuard<'a, Vec<T>>,
    value: T,
}

impl<'a, T: Default> UninitialisedEntry<'a, T> {
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
pub struct LockVec<T> {
    data: Arc<RwLock<Vec<T>>>,
}

impl<T: PartialEq> PartialEq for LockVec<T> {
    fn eq(&self, other: &Self) -> bool {
        let a = self.data.read();
        let b = other.data.read();
        a.deref() == b.deref()
    }
}

impl<T: Default> Default for LockVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> LockVec<T> {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(Vec::new())),
        }
    }

    #[inline]
    pub fn read_arc_lock(&self) -> ArcRwLockReadGuard<Vec<T>> {
        RwLock::read_arc(&self.data)
    }

    #[inline]
    pub fn write(&self) -> impl DerefMut<Target = Vec<T>> + '_ {
        self.data.write()
    }

    #[inline]
    pub fn read(&self) -> impl Deref<Target = Vec<T>> + '_ {
        self.data.read()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NodeStorage {
    pub(crate) data: Box<[LockVec<NodeStore>]>,
    pub(crate) props: Box<[LockVec<Prop>]>,
    len: AtomicUsize,
}

impl PartialEq for NodeStorage {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}

#[derive(Debug)]
pub struct ReadLockedStorage<T, Index = usize> {
    pub(crate) locks: Vec<Arc<ArcRwLockReadGuard<Vec<T>>>>,
    len: usize,
    _index: PhantomData<Index>,
}

impl<Index, T> ReadLockedStorage<T, Index>
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

    pub(crate) fn get(&self, index: Index) -> &T {
        let (bucket, offset) = self.resolve(index);
        let bucket = &self.locks[bucket];
        &bucket[offset]
    }

    pub(crate) fn arc_entry(&self, index: Index) -> ArcEntry<T> {
        let (bucket, offset) = self.resolve(index);
        ArcEntry {
            guard: self.locks[bucket].clone(),
            i: offset,
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.locks.iter().flat_map(|v| v.iter())
    }

    pub(crate) fn par_iter(&self) -> impl ParallelIterator<Item = &T> + '_
    where
        T: Send + Sync,
    {
        self.locks.par_iter().flat_map(|v| v.par_iter())
    }

    #[allow(unused)]
    pub(crate) fn into_iter(self) -> impl Iterator<Item = ArcEntry<T>> + Send
    where
        T: Send + Sync + 'static,
    {
        self.locks.into_iter().flat_map(|data| {
            (0..data.len()).map(move |offset| ArcEntry {
                guard: data.clone(),
                i: offset,
            })
        })
    }

    #[allow(unused)]
    pub(crate) fn into_par_iter(self) -> impl ParallelIterator<Item = ArcEntry<T>>
    where
        T: Send + Sync + 'static,
    {
        self.locks.into_par_iter().flat_map(|data| {
            (0..data.len()).into_par_iter().map(move |offset| ArcEntry {
                guard: data.clone(),
                i: offset,
            })
        })
    }
}

impl NodeStorage {
    pub fn count_with_filter<F: Fn(&NodeStore) -> bool + Send + Sync>(&self, f: F) -> usize {
        self.read_lock().par_iter().filter(|x| f(x)).count()
    }
}

impl NodeStorage {
    #[inline]
    fn resolve(&self, index: usize) -> (usize, usize) {
        resolve(index, self.data.len())
    }

    #[inline]
    pub fn read_lock(&self) -> ReadLockedStorage<NodeStore, VID> {
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
        let data: Box<[LockVec<NodeStore>]> = (0..n_locks)
            .map(|_| LockVec::new())
            .collect::<Vec<_>>()
            .into();

        let props: Box<[LockVec<Prop>]> = (0..n_locks)
            .map(|_| LockVec::new())
            .collect::<Vec<_>>()
            .into();
        Self {
            data,
            props,
            len: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, mut value: NodeStore) -> UninitialisedEntry<NodeStore> {
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
    pub fn entry(&self, index: VID) -> Entry<'_, NodeStore> {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.read_recursive();
        Entry { offset, guard }
    }

    pub fn entry_arc(&self, index: VID) -> ArcEntry<NodeStore> {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = &self.data[bucket].data;
        let arc_guard = RwLock::read_arc_recursive(guard);
        ArcEntry {
            i: offset,
            guard: Arc::new(arc_guard),
        }
    }

    pub fn entry_mut(&self, index: VID) -> EntryMut<'_, NodeStore> {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.write();
        EntryMut { i: offset, guard }
    }

    pub fn prop_entry_mut(&self, index: VID) -> impl DerefMut<Target = Vec<Prop>> + '_ {
        let index = index.into();
        let (bucket, _) = self.resolve(index);
        self.props[bucket].data.write()
    }

    // This helps get the right locks when adding an edge
    pub fn pair_entry_mut(&self, i: VID, j: VID) -> PairEntryMut<'_, NodeStore> {
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
    guards: Vec<RwLockWriteGuard<'a, Vec<NodeStore>>>,
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
    S: DerefMut<Target = Vec<NodeStore>>,
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
    ) -> impl IndexedParallelIterator<Item = NodeShardWriter<&mut Vec<NodeStore>>> + '_ {
        let num_shards = self.guards.len();
        let global_len = self.global_len;
        let shards: Vec<&mut Vec<NodeStore>> = self
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
    ) -> impl IndexedParallelIterator<Item = NodeShardWriter<'a, RwLockWriteGuard<'a, Vec<NodeStore>>>>
           + 'a {
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
pub struct Entry<'a, T: 'static> {
    offset: usize,
    guard: RwLockReadGuard<'a, Vec<T>>,
}

#[derive(Debug)]
pub struct ArcEntry<T> {
    guard: Arc<ArcRwLockReadGuard<Vec<T>>>,
    i: usize,
}

impl<T> Clone for ArcEntry<T> {
    fn clone(&self) -> Self {
        Self {
            guard: self.guard.clone(),
            i: self.i,
        }
    }
}

impl<T> Deref for ArcEntry<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard[self.i]
    }
}

impl<'a, T> Deref for Entry<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard[self.offset]
    }
}

pub enum PairEntryMut<'a, T: 'static> {
    Same {
        i: usize,
        j: usize,
        guard: parking_lot::RwLockWriteGuard<'a, Vec<T>>,
    },
    Different {
        i: usize,
        j: usize,
        guard1: parking_lot::RwLockWriteGuard<'a, Vec<T>>,
        guard2: parking_lot::RwLockWriteGuard<'a, Vec<T>>,
    },
}

impl<'a, T: 'static> PairEntryMut<'a, T> {
    pub(crate) fn get_i(&self) -> &T {
        match self {
            PairEntryMut::Same { i, guard, .. } => &guard[*i],
            PairEntryMut::Different { i, guard1, .. } => &guard1[*i],
        }
    }
    pub(crate) fn get_mut_i(&mut self) -> &mut T {
        match self {
            PairEntryMut::Same { i, guard, .. } => &mut guard[*i],
            PairEntryMut::Different { i, guard1, .. } => &mut guard1[*i],
        }
    }

    pub(crate) fn get_j(&self) -> &T {
        match self {
            PairEntryMut::Same { j, guard, .. } => &guard[*j],
            PairEntryMut::Different { j, guard2, .. } => &guard2[*j],
        }
    }

    pub(crate) fn get_mut_j(&mut self) -> &mut T {
        match self {
            PairEntryMut::Same { j, guard, .. } => &mut guard[*j],
            PairEntryMut::Different { j, guard2, .. } => &mut guard2[*j],
        }
    }
}

pub struct EntryMut<'a, T: 'static> {
    i: usize,
    guard: parking_lot::RwLockWriteGuard<'a, Vec<T>>,
}

impl<'a, T> Deref for EntryMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard[self.i]
    }
}

impl<'a, T> DerefMut for EntryMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard[self.i]
    }
}

#[cfg(test)]
mod test {
    use super::NodeStorage;
    use crate::core::entities::nodes::node_store::NodeStore;
    use pretty_assertions::assert_eq;
    use quickcheck_macros::quickcheck;
    use raphtory_api::core::entities::{GID, VID};
    use rayon::prelude::{IntoParallelIterator, ParallelIterator};
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
            assert_eq!(entry.vid, VID(i));
        }

        let items_iter = storage.read_lock().into_iter();

        let actual = items_iter.map(|s| s.vid.index()).collect::<Vec<_>>();

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
            assert_eq!(*entry.global_id.to_str(), i.to_string());
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
            .map(|n| n.global_id.as_u64().unwrap())
            .collect();

        actual.sort();
        expected.sort();

        actual == expected
    }
}
