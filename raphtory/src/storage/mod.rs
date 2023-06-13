mod items;
pub(crate) mod iter;

use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
};

use lock_api::{RawRwLock, RwLock};
use serde::{Deserialize, Serialize};

use self::{items::Items, iter::Iter};

#[derive(Debug, Serialize, Deserialize)]
pub struct LockVec<T, L: RawRwLock> {
    data: RwLock<L, Vec<Option<T>>>,
}

impl<T, L: RawRwLock> LockVec<T, L> {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(Vec::new()),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct RawStorage<T, L: RawRwLock, const N: usize> {
    data: Box<[LockVec<T, L>]>,
    len: AtomicUsize,
}

impl<T, L: RawRwLock, const N: usize> RawStorage<T, L, N> {
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

    fn resolve(&self, index: usize) -> (usize, usize) {
        let bucket = index % N;
        let offset = index / N;
        (bucket, offset)
    }

    pub fn push(&self, value: T) -> usize {
        let index = self.len.fetch_add(1, Ordering::SeqCst);
        let (bucket, offset) = self.resolve(index);
        let mut vec = self.data[bucket].data.write();
        vec.resize_with(offset, || None);
        vec.insert(offset, Some(value));
        index
    }

    pub fn entry(&self, index: usize) -> Entry<'_, T, L> {
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.read();
        Entry { i: offset, guard }
    }

    pub fn entry_mut(&self, index: usize) -> EntryMut<'_, T, L> {
        let (bucket, offset) = self.resolve(index);
        let guard = self.data[bucket].data.write();
        EntryMut { i: offset, guard }
    }

    // This helps get the right locks when adding an edge
    pub fn pair_entry_mut(&self, i: usize, j: usize) -> PairEntryMut<'_, T, L> {
        let (bucket, offset_i) = self.resolve(i);
        let (bucket2, offset_j) = self.resolve(j);
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

    pub fn items<'a>(&'a self) -> Items<'a, T, L> {
        let guards = self.data.iter().map(|vec| vec.data.read()).collect();
        Items::new(guards)
    }

    pub fn iter2<'a>(&'a self) -> Iter<'a, T, L, N> {
        Iter::new(self)
    }
}

pub struct Entry<'a, T: 'static, L: RawRwLock> {
    i: usize,
    guard: lock_api::RwLockReadGuard<'a, L, Vec<Option<T>>>,
}

impl <'a, T: 'static, L: RawRwLock> Entry<'a, T, L>{
    pub fn value(&self) -> Option<&T> {
        let t = self.guard.get(self.i)?;
        t.as_ref()
    }

    pub fn is_vacant(&self) -> bool {
        self.value().is_none()
    }
}

impl<'a, T, L: lock_api::RawRwLock> Deref for Entry<'a, T, L> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard[self.i].as_ref().unwrap()
    }
}

pub enum PairEntryMut<'a, T: 'static, L: RawRwLock> {
    Same {
        i: usize,
        j: usize,
        guard: lock_api::RwLockWriteGuard<'a, L, Vec<Option<T>>>,
    },
    Different {
        i: usize,
        j: usize,
        guard1: lock_api::RwLockWriteGuard<'a, L, Vec<Option<T>>>,
        guard2: lock_api::RwLockWriteGuard<'a, L, Vec<Option<T>>>,
    },
}

impl <'a, T: 'static, L: RawRwLock> PairEntryMut<'a, T, L> {

    pub(crate) fn get_mut_i(&mut self) -> &mut T{
        match self {
            PairEntryMut::Same { i, guard, .. } => guard[*i].as_mut().unwrap(),
            PairEntryMut::Different { i, guard1, .. } => guard1[*i].as_mut().unwrap(),
        }
    }

    pub(crate) fn get_mut_j(&mut self) -> &mut T{
        match self {
            PairEntryMut::Same { j, guard, .. } => guard[*j].as_mut().unwrap(),
            PairEntryMut::Different { j, guard2, .. } => guard2[*j].as_mut().unwrap(),
        }
    }
}

pub struct EntryMut<'a, T: 'static, L: RawRwLock> {
    i: usize,
    guard: lock_api::RwLockWriteGuard<'a, L, Vec<Option<T>>>,
}

impl<'a, T, L: lock_api::RawRwLock> Deref for EntryMut<'a, T, L> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard[self.i].as_ref().unwrap()
    }
}

impl<'a, T, L: lock_api::RawRwLock> DerefMut for EntryMut<'a, T, L> {
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
        let storage = RawStorage::<String, NoLock, 2>::new();

        for i in 0..5 {
            storage.push(i.to_string());
        }

        assert_eq!(storage.len(), 5);

        for i in 0..5 {
            let entry = storage.entry(i);
            assert_eq!(*entry, i.to_string());
        }

        let items = storage.items();

        let actual = items.iter().map(|s| s.to_owned()).collect::<Vec<_>>();

        assert_eq!(actual, vec!["0", "2", "4", "1", "3"]);

        let items_iter = storage.iter2();

        let actual = items_iter.map(|s| s.to_owned()).collect::<Vec<_>>();

        assert_eq!(actual, vec!["0", "2", "4", "1", "3"]);
    }
}
