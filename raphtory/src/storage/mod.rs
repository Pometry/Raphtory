use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

// use dashmap::DashMap;
use lock_api::{ArcRwLockReadGuard, RawRwLock, RwLock};

#[derive(Clone, Debug)]
pub(crate) struct LockVec<T, L: RawRwLock> {
    data: Arc<RwLock<L, Vec<T>>>,
}

// new for LockVec
impl<T, L: RawRwLock> LockVec<T, L> {
    pub(crate) fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

pub(crate) struct RawStorage<T, L: RawRwLock, const N: usize> {
    data: [LockVec<T, L>; N],
    len: AtomicUsize,
}

impl<T: std::fmt::Debug + Default, L: RawRwLock, const N: usize> RawStorage<T, L, N> {
    pub(crate) fn new() -> Self {
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

    pub(crate) fn push(&self, value: T) {
        let index = self.len.fetch_add(1, Ordering::SeqCst);
        let (bucket, offset) = self.resolve(index);
        let mut vec = self.data[bucket].data.write();
        vec.resize_with(offset, || T::default());
        vec.insert(offset, value);
    }

    pub(crate) fn entry(&self, index: usize) -> Entry<'_, T, L> {
        let (bucket, offset) = self.resolve(index);
        let guard: lock_api::RwLockReadGuard<'_, L, Vec<T>> = self.data[bucket].data.read();
        Entry { i: offset, guard }
    }

    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    pub(crate) fn items<'a>(&'a self) -> IterableGuards<'a, T, L>{
        let guards = self.data.iter().map(|vec| vec.data.read()).collect();
        IterableGuards { guards }
    }
}

pub(crate) struct IterableGuard<'a, T: 'static, L: RawRwLock> {
    guard: lock_api::RwLockReadGuard<'a ,L, Vec<T>>,
}

impl<'a, T: 'static, L: RawRwLock> IterableGuard<'a, T, L> {
    fn iter(&'a self) -> std::slice::Iter<'a, T> {
        self.guard.iter()
    }
}

pub(crate) struct IterableGuards<'a, T: 'static, L: RawRwLock> {
    guards: Vec<lock_api::RwLockReadGuard<'a, L, Vec<T>>>,
}

impl<'a, T: 'static, L: RawRwLock> IterableGuards<'a, T, L> {
    fn iter(&'a self) -> Box<dyn Iterator<Item = &'a T> + 'a> {
        let iter = self.guards.iter().flat_map(|guard| guard.iter());
        Box::new(iter)
    }
}

pub(crate) struct Entry<'a, T: 'static, L: RawRwLock> {
    i: usize,
    guard: lock_api::RwLockReadGuard<'a, L, Vec<T>>,
}

impl<'a, T, L: lock_api::RawRwLock> Deref for Entry<'a, T, L> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard[self.i]
    }
}

#[cfg(test)]
mod test {
    use dashmap::DashMap;
    use lock_api::{RawRwLock, RwLock};

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
    }

}
