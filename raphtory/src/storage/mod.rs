mod items;
pub(crate) mod iter;

use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{atomic::{AtomicUsize, Ordering}, Arc},
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use self::iter::Iter;

fn resolve<const N: usize>(index: usize) -> (usize, usize) {
    let bucket = index % N;
    let offset = index / N;
    (bucket, offset)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LockVec<T> {
    data: Arc<parking_lot::RwLock<Vec<Option<T>>>>,
}

impl<T: PartialEq> PartialEq for LockVec<T> {
    fn eq(&self, other: &Self) -> bool {
        let a = self.data.read();
        let b = other.data.read();
        a.deref() == b.deref()
    }
}

impl<T> LockVec<T> {
    pub fn new() -> Self {
        Self {
            data: Arc::new(parking_lot::RwLock::new(Vec::new())),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RawStorage<T, const N: usize> {
    pub(crate) data: Box<[LockVec<T>]>,
    len: AtomicUsize,
}

impl<T: PartialEq, const N: usize> PartialEq for RawStorage<T, N> {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}

#[derive(Debug)]
pub struct ReadLockedStorage<'a, T, const N: usize> {
    locks: Box<[parking_lot::RwLockReadGuard<'a, Vec<Option<T>>>; N]>,
}

impl<'a, T, const N: usize> ReadLockedStorage<'a, T, N> {
    pub(crate) fn get(&'a self, index: usize) -> &'a T {
        let (bucket, offset) = resolve::<N>(index);
        let bucket = &self.locks[bucket];
        bucket[offset].as_ref().unwrap()
    }

    pub fn iter2(&'a self) -> impl Iterator<Item = &'a T> {
        self.locks.iter().map(|l| l.iter().flatten()).flatten()
    }
}

impl<T, const N: usize> RawStorage<T, N> {
    pub fn read_lock<'a>(&'a self) -> ReadLockedStorage<'a, T, N> {
        let guards: [parking_lot::RwLockReadGuard<'a, Vec<Option<T>>>; N] =
            std::array::from_fn(|i| self.data[i].data.read());
        ReadLockedStorage {
            locks: guards.into(),
        }
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

    pub fn push(&self, value: T) -> usize {
        let index = self.len.fetch_add(1, Ordering::SeqCst);
        let (bucket, offset) = resolve::<N>(index);
        let mut vec = self.data[bucket].data.write();
        vec.resize_with(offset, || None);
        vec.insert(offset, Some(value));
        index
    }

    pub fn entry(&self, index: usize) -> Entry<'_, T, N> {
        let (bucket, _) = resolve::<N>(index);
        let guard = self.data[bucket].data.read();
        Entry { i: index, guard }
    }

    pub fn entry_arc(&self, index: usize) -> ArcEntry<T, N>{
        let (bucket, offset) = resolve::<N>(index);
        let guard = &self.data[bucket].data;
        let arc_guard = RwLock::read_arc(guard);
        ArcEntry { i: offset, guard: arc_guard }
    }

    pub fn entry_mut(&self, index: usize) -> EntryMut<'_, T> {
        let (bucket, offset) = resolve::<N>(index);
        let guard = self.data[bucket].data.write();
        EntryMut { i: offset, guard }
    }

    // This helps get the right locks when adding an edge
    pub fn pair_entry_mut(&self, i: usize, j: usize) -> PairEntryMut<'_, T> {
        let (bucket, offset_i) = resolve::<N>(i);
        let (bucket2, offset_j) = resolve::<N>(j);
        if bucket != bucket2 {
            PairEntryMut::Different {
                i: offset_i,
                j: offset_j,
                guard1: self.data[bucket].data.write(),
                guard2: self.data[bucket2].data.write(),
            }
        } else {
            PairEntryMut::Same {
                i: offset_i,
                j: offset_j,
                guard: self.data[bucket].data.write(),
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    // pub fn items<'a>(&'a self) -> Items<'a, T, L> {
    //     let guards = self.data.iter().map(|vec| vec.data.read()).collect();
    //     Items::new(guards)
    // }

    pub fn iter<'a>(&'a self) -> Iter<'a, T, N> {
        Iter::new(self)
    }
}

#[derive(Debug)]
pub struct Entry<'a, T: 'static, const N: usize> {
    i: usize,
    guard: parking_lot::RwLockReadGuard<'a, Vec<Option<T>>>,
}

pub struct ArcEntry<T: 'static, const N: usize> {
    i: usize,
    guard: lock_api::ArcRwLockReadGuard<parking_lot::RawRwLock,Vec<Option<T>>>,
}

impl <T: 'static, const N: usize> Deref for ArcEntry<T, N> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard[self.i].as_ref().unwrap()
    }
}

impl<'a, T: 'static, const N: usize> Entry<'a, T, N> {
    pub fn value(&self) -> Option<&T> {
        let (_, offset) = resolve::<N>(self.i);
        let t: &Option<T> = self.guard.get(offset)?;
        t.as_ref()
    }

    pub fn is_vacant(&self) -> bool {
        self.value().is_none()
    }

    pub fn index(&self) -> usize {
        self.i
    }
}

impl<'a, T, const N: usize> Deref for Entry<'a, T, N> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let (_, offset) = resolve::<N>(self.i);
        self.guard[offset].as_ref().unwrap()
    }
}

pub enum PairEntryMut<'a, T: 'static> {
    Same {
        i: usize,
        j: usize,
        guard: parking_lot::RwLockWriteGuard<'a, Vec<Option<T>>>,
    },
    Different {
        i: usize,
        j: usize,
        guard1: parking_lot::RwLockWriteGuard<'a, Vec<Option<T>>>,
        guard2: parking_lot::RwLockWriteGuard<'a, Vec<Option<T>>>,
    },
}

impl<'a, T: 'static> PairEntryMut<'a, T> {
    pub(crate) fn get_mut_i(&mut self) -> &mut T {
        match self {
            PairEntryMut::Same { i, guard, .. } => guard[*i].as_mut().unwrap(),
            PairEntryMut::Different { i, guard1, .. } => guard1[*i].as_mut().unwrap(),
        }
    }

    pub(crate) fn get_mut_j(&mut self) -> &mut T {
        match self {
            PairEntryMut::Same { j, guard, .. } => guard[*j].as_mut().unwrap(),
            PairEntryMut::Different { j, guard2, .. } => guard2[*j].as_mut().unwrap(),
        }
    }
}

pub struct EntryMut<'a, T: 'static> {
    i: usize,
    guard: parking_lot::RwLockWriteGuard<'a, Vec<Option<T>>>,
}

impl<'a, T> Deref for EntryMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard[self.i].as_ref().unwrap()
    }
}

impl<'a, T> DerefMut for EntryMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard[self.i].as_mut().unwrap()
    }
}

#[cfg(test)]
mod test {
    use lock_api::RawRwLock;

    use super::RawStorage;

    struct NoLock;

    unsafe impl RawRwLock for NoLock {
        const INIT: Self = Self;

        type GuardMarker = lock_api::GuardNoSend;

        fn lock_shared(&self) {}

        fn try_lock_shared(&self) -> bool {
            true
        }

        unsafe fn unlock_shared(&self) {}

        fn lock_exclusive(&self) {}

        fn try_lock_exclusive(&self) -> bool {
            true
        }

        unsafe fn unlock_exclusive(&self) {}
    }

    #[test]
    fn add_5_values_to_storage() {
        let storage = RawStorage::<String, 2>::new();

        for i in 0..5 {
            storage.push(i.to_string());
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
            storage.push(i.to_string());
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
            storage.push(i.to_string());
        }

        for i in 0..5 {
            let entry = storage.entry(i);
            assert_eq!(*entry, i.to_string());
        }
    }
}
