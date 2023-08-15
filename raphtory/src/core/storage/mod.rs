#![allow(unused)]

pub(crate) mod iter;
pub mod lazy_vec;
pub mod locked_view;
pub mod sorted_vec_map;
pub mod timeindex;

use self::iter::Iter;
use locked_view::LockedView;
use parking_lot::{RwLock, RwLockReadGuard};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[inline]
fn resolve<const N: usize>(index: usize) -> (usize, usize) {
    let bucket = index % N;
    let offset = index / N;
    (bucket, offset)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LockVec<T: Default> {
    data: Arc<parking_lot::RwLock<Vec<T>>>,
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
            data: Arc::new(parking_lot::RwLock::new(Vec::new())),
        }
    }

    pub fn read_arc_lock(&self) -> lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, Vec<T>> {
        RwLock::read_arc(&self.data)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RawStorage<T: Default, const N: usize> {
    pub(crate) data: Box<[LockVec<T>]>,
    len: AtomicUsize,
}

impl<T: PartialEq + Default, const N: usize> PartialEq for RawStorage<T, N> {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}

#[derive(Debug)]
pub struct ReadLockedStorage<T, const N: usize> {
    locks: Box<[lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, Vec<T>>; N]>,
    len: usize,
}

impl<T, const N: usize> ReadLockedStorage<T, N> {
    pub(crate) fn get(&self, index: usize) -> &T {
        let (bucket, offset) = resolve::<N>(index);
        let bucket = &self.locks[bucket];
        &bucket[offset]
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.locks.iter().flat_map(|v| v.iter())
    }
}

impl<T: Default + Send + Sync, const N: usize> RawStorage<T, N> {
    pub fn count_with_filter<F: Fn(&T) -> bool + Send + Sync>(&self, f: F) -> usize {
        self.data
            .par_iter()
            .map(|lock_vec| lock_vec.data.read().par_iter().filter(|x| f(x)).count())
            .sum()
    }
}

impl<T: Default, const N: usize> RawStorage<T, N> {
    pub fn read_lock(&self) -> ReadLockedStorage<T, N> {
        let guards: [lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, Vec<T>>; N] =
            std::array::from_fn(|i| self.data[i].read_arc_lock());
        ReadLockedStorage {
            locks: guards.into(),
            len: self.len(),
        }
    }

    pub fn indices(&self) -> impl Iterator<Item = usize> + Send + '_ {
        0..self.len()
    }

    pub fn new() -> Self {
        let mut data = Vec::with_capacity(N);
        for _ in 0..N {
            data.push(LockVec::new());
        }

        if let Ok(data) = data.try_into() {
            Self {
                data,
                len: AtomicUsize::new(0),
            }
        } else {
            panic!("failed to convert to array");
        }
    }

    pub fn push<F: Fn(usize, &mut T)>(&self, mut value: T, f: F) -> usize {
        let index = self.len.fetch_add(1, Ordering::SeqCst);
        let (bucket, offset) = resolve::<N>(index);
        let mut vec = self.data[bucket].data.write();
        if offset >= vec.len() {
            vec.resize_with(offset + 1, || Default::default());
        }
        f(index, &mut value);
        vec[offset] = value;
        index
    }

    #[inline]
    pub fn entry(&self, index: usize) -> Entry<'_, T, N> {
        let (bucket, _) = resolve::<N>(index);
        let guard = self.data[bucket].data.read_recursive();
        Entry {
            offset: index,
            guard,
        }
    }

    pub fn get(&self, index: usize) -> impl Deref<Target = T> + '_ {
        let (bucket, offset) = resolve::<N>(index);
        let guard = self.data[bucket].data.read_recursive();
        RwLockReadGuard::map(guard, |guard| &guard[offset])
    }

    pub fn entry_arc(&self, index: usize) -> ArcEntry<T, N> {
        let (bucket, offset) = resolve::<N>(index);
        let guard = &self.data[bucket].data;
        let arc_guard = RwLock::read_arc_recursive(guard);
        ArcEntry {
            i: offset,
            guard: arc_guard,
        }
    }

    pub fn entry_mut(&self, index: usize) -> EntryMut<'_, T> {
        let (bucket, offset) = resolve::<N>(index);
        let guard = self.data[bucket].data.write();
        EntryMut { i: offset, guard }
    }

    // This helps get the right locks when adding an edge
    pub fn pair_entry_mut(&self, i: usize, j: usize) -> PairEntryMut<'_, T> {
        let (bucket_i, offset_i) = resolve::<N>(i);
        let (bucket_j, offset_j) = resolve::<N>(j);
        if bucket_i != bucket_j {
            // The code below deadlocks! left here as an example of what not to do
            // PairEntryMut::Different {
            //     i: offset_i,
            //     j: offset_j,
            //     guard1: self.data[bucket].data.write(),
            //     guard2: self.data[bucket2].data.write(),
            // }
            loop {
                if let Some((guard_i, guard_j)) = self.data[bucket_i]
                    .data
                    .try_write()
                    .zip(self.data[bucket_j].data.try_write())
                {
                    break PairEntryMut::Different {
                        i: offset_i,
                        j: offset_j,
                        guard1: guard_i,
                        guard2: guard_j,
                    };
                }
                // TODO add a counter to avoid spinning for too long
            }
        } else {
            PairEntryMut::Same {
                i: offset_i,
                j: offset_j,
                guard: self.data[bucket_i].data.write(),
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    pub fn iter(&self) -> Iter<T, N> {
        Iter::new(self)
    }
}

#[derive(Debug)]
pub struct Entry<'a, T: 'static, const N: usize> {
    offset: usize,
    guard: parking_lot::RwLockReadGuard<'a, Vec<T>>,
}

impl<'a, T: 'static, const N: usize> Clone for Entry<'a, T, N> {
    fn clone(&self) -> Self {
        let guard = RwLockReadGuard::rwlock(&self.guard).read_recursive();
        let i = self.offset;
        Self { offset: i, guard }
    }
}

pub struct ArcEntry<T: 'static, const N: usize> {
    i: usize,
    guard: lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock, Vec<T>>,
}

impl<T: 'static, const N: usize> Deref for ArcEntry<T, N> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard[self.i]
    }
}

impl<'a, T: 'static, const N: usize> Entry<'a, T, N> {
    pub fn value(&self) -> &T {
        let (_, offset) = resolve::<N>(self.offset);
        &self.guard[offset]
    }

    pub fn index(&self) -> usize {
        self.offset
    }

    pub fn map<U, F: Fn(&T) -> &U>(self, f: F) -> LockedView<'a, U> {
        let (_, offset) = resolve::<N>(self.offset);
        let mapped_guard = RwLockReadGuard::map(self.guard, |guard| {
            let what = &guard[offset];
            f(what)
        });

        LockedView::LockMapped(mapped_guard)
    }
}

impl<'a, T, const N: usize> Deref for Entry<'a, T, N> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let (_, offset) = resolve::<N>(self.offset);
        &self.guard[offset]
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
        let storage = RawStorage::<String, 2>::new();

        for i in 0..5 {
            storage.push(i.to_string(), |_, _| {});
        }

        assert_eq!(storage.len(), 5);

        for i in 0..5 {
            let entry = storage.entry(i);
            assert_eq!(*entry, i.to_string());
        }

        let items_iter = storage.iter();

        let actual = items_iter.map(|s| (*s).to_owned()).collect::<Vec<_>>();

        assert_eq!(actual, vec!["0", "2", "4", "1", "3"]);
    }

    #[test]
    fn test_index_correctness() {
        let storage = RawStorage::<String, 2>::new();

        for i in 0..5 {
            storage.push(i.to_string(), |_, _| {});
        }

        let items_iter = storage.iter();
        let actual = items_iter
            .map(|s| (s.index(), (*s).to_owned()))
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![(0, "0"), (2, "2"), (4, "4"), (1, "1"), (3, "3"),]
                .into_iter()
                .map(|(i, s)| (i, s.to_string()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_entry() {
        let storage = RawStorage::<String, 2>::new();

        for i in 0..5 {
            storage.push(i.to_string(), |_, _| {});
        }

        for i in 0..5 {
            let entry = storage.entry(i);
            assert_eq!(*entry, i.to_string());
        }
    }

    use pretty_assertions::assert_eq;

    #[quickcheck]
    fn concurrent_push(v: Vec<usize>) -> bool {
        let storage = RawStorage::<usize, 16>::new();
        let mut expected = v
            .into_par_iter()
            .map(|v| {
                storage.push(v, |_, _| {});
                v
            })
            .collect::<Vec<_>>();

        let mut actual = storage.iter().map(|s| *s).collect::<Vec<_>>();

        actual.sort();
        expected.sort();

        actual == expected
    }
}
