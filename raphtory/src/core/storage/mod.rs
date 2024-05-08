#![allow(unused)]

pub(crate) mod iter;
pub mod lazy_vec;
pub mod locked_view;
pub mod sorted_vec_map;
pub mod timeindex;

use self::iter::Iter;
use lock_api;
use locked_view::LockedView;
use parking_lot::{RwLock, RwLockReadGuard};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    array,
    fmt::Debug,
    iter::FusedIterator,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

type ArcRwLockReadGuard<T> = lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, T>;

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

impl<T: PartialEq + Default> PartialEq for LockVec<T> {
    fn eq(&self, other: &Self) -> bool {
        let a = self.data.read();
        let b = other.data.read();
        a.deref() == b.deref()
    }
}

impl<T: Default> LockVec<T> {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(Vec::new())),
        }
    }

    #[inline]
    pub fn read_arc_lock(&self) -> ArcRwLockReadGuard<Vec<T>> {
        RwLock::read_arc(&self.data)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RawStorage<T: Default, Index = usize> {
    pub(crate) data: Box<[LockVec<T>]>,
    len: AtomicUsize,
    _index: PhantomData<Index>,
}

impl<Index, T: PartialEq + Default> PartialEq for RawStorage<T, Index> {
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

    pub(crate) fn into_iter(self) -> impl Iterator<Item = ArcEntry<T>> + Send
    where
        T: Send + Sync + 'static,
    {
        self.locks
            .into_iter()
            .enumerate()
            .flat_map(|(bucket, data)| {
                (0..data.len()).map(move |offset| ArcEntry {
                    guard: data.clone(),
                    i: offset,
                })
            })
    }

    pub(crate) fn into_par_iter(self) -> impl ParallelIterator<Item = ArcEntry<T>>
    where
        T: Send + Sync + 'static,
    {
        self.locks
            .into_par_iter()
            .enumerate()
            .flat_map(|(bucket, data)| {
                (0..data.len()).into_par_iter().map(move |offset| ArcEntry {
                    guard: data.clone(),
                    i: offset,
                })
            })
    }
}

impl<Index, T: Default + Send + Sync> RawStorage<T, Index>
where
    usize: From<Index>,
{
    pub fn count_with_filter<F: Fn(&T) -> bool + Send + Sync>(&self, f: F) -> usize {
        self.read_lock().par_iter().filter(|x| f(x)).count()
    }
}

impl<Index, T: Default> RawStorage<T, Index>
where
    usize: From<Index>,
{
    #[inline]
    fn resolve(&self, index: usize) -> (usize, usize) {
        resolve(index, self.data.len())
    }

    #[inline]
    pub fn read_lock(&self) -> ReadLockedStorage<T, Index> {
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

    pub fn indices(&self) -> impl Iterator<Item = usize> + Send + '_ {
        0..self.len()
    }

    pub fn new(n_locks: usize) -> Self {
        let data: Box<[LockVec<T>]> = (0..n_locks)
            .map(|_| LockVec::new())
            .collect::<Vec<_>>()
            .into();
        Self {
            data,
            len: AtomicUsize::new(0),
            _index: PhantomData,
        }
    }

    pub fn push<F: Fn(usize, &mut T)>(&self, mut value: T, f: F) -> usize {
        let index = self.len.fetch_add(1, Ordering::SeqCst);
        let (bucket, offset) = self.resolve(index);
        let mut vec = self.data[bucket].data.write();
        if offset >= vec.len() {
            vec.resize_with(offset + 1, || Default::default());
        }
        f(index, &mut value);
        vec[offset] = value;
        index
    }

    #[inline]
    pub fn entry(&self, index: Index) -> Entry<'_, T> {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.read_recursive();
        Entry { offset, guard }
    }

    #[inline]
    pub fn get(&self, index: Index) -> impl Deref<Target = T> + '_ {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.read_recursive();
        RwLockReadGuard::map(guard, |guard| &guard[offset])
    }

    pub fn entry_arc(&self, index: Index) -> ArcEntry<T> {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = &self.data[bucket].data;
        let arc_guard = RwLock::read_arc_recursive(guard);
        ArcEntry {
            i: offset,
            guard: Arc::new(arc_guard),
        }
    }

    pub fn entry_mut(&self, index: Index) -> EntryMut<'_, T> {
        let index = index.into();
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.write();
        EntryMut { i: offset, guard }
    }

    // This helps get the right locks when adding an edge
    pub fn pair_entry_mut(&self, i: Index, j: Index) -> PairEntryMut<'_, T> {
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
}

#[derive(Debug)]
pub struct Entry<'a, T: 'static> {
    offset: usize,
    guard: RwLockReadGuard<'a, Vec<T>>,
}

impl<'a, T: 'static> Clone for Entry<'a, T> {
    fn clone(&self) -> Self {
        let guard = RwLockReadGuard::rwlock(&self.guard).read_recursive();
        let i = self.offset;
        Self { offset: i, guard }
    }
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

impl<T, S> AsRef<T> for ArcEntry<S>
where
    T: ?Sized,
    S: AsRef<T>,
{
    fn as_ref(&self) -> &T {
        self.deref().as_ref()
    }
}

impl<'a, T> Entry<'a, T> {
    pub fn value(&self) -> &T {
        &self.guard[self.offset]
    }

    pub fn map<U, F: Fn(&T) -> &U>(self, f: F) -> LockedView<'a, U> {
        let mapped_guard = RwLockReadGuard::map(self.guard, |guard| {
            let what = &guard[self.offset];
            f(what)
        });
        LockedView::LockMapped(mapped_guard)
    }
}

impl<'a, T> Deref for Entry<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value()
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
    pub(crate) fn get_mut_i(&mut self) -> &mut T {
        match self {
            PairEntryMut::Same { i, guard, .. } => &mut guard[*i],
            PairEntryMut::Different { i, guard1, .. } => &mut guard1[*i],
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
    use rayon::prelude::{IntoParallelIterator, ParallelIterator};

    use super::RawStorage;

    #[test]
    fn add_5_values_to_storage() {
        let storage = RawStorage::<String>::new(2);

        for i in 0..5 {
            storage.push(i.to_string(), |_, _| {});
        }

        assert_eq!(storage.len(), 5);

        for i in 0..5 {
            let entry = storage.entry(i);
            assert_eq!(*entry, i.to_string());
        }

        let items_iter = storage.read_lock().into_iter();

        let actual = items_iter.map(|s| (*s).to_owned()).collect::<Vec<_>>();

        assert_eq!(actual, vec!["0", "2", "4", "1", "3"]);
    }

    #[test]
    fn test_index_correctness() {
        let storage = RawStorage::<String>::new(2);

        for i in 0..5 {
            storage.push(i.to_string(), |_, _| {});
        }
        let locked = storage.read_lock();
        let actual: Vec<_> = (0..5).map(|i| (i, locked.get(i).as_str())).collect();
        assert_eq!(
            actual,
            vec![(0usize, "0"), (1, "1"), (2, "2"), (3, "3"), (4, "4")]
        );
    }

    #[test]
    fn test_entry() {
        let storage = RawStorage::<String>::new(2);

        for i in 0..5 {
            storage.push(i.to_string(), |_, _| {});
        }

        for i in 0..5 {
            let entry = storage.entry(i);
            assert_eq!(*entry, i.to_string());
        }
    }

    use pretty_assertions::assert_eq;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn concurrent_push(v: Vec<usize>) -> bool {
        let storage = RawStorage::<usize>::new(16);
        let mut expected = v
            .into_par_iter()
            .map(|v| {
                storage.push(v, |_, _| {});
                v
            })
            .collect::<Vec<_>>();

        let locked = storage.read_lock();
        let mut actual: Vec<_> = locked.iter().copied().collect();

        actual.sort();
        expected.sort();

        actual == expected
    }
}
