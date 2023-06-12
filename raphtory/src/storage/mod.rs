use std::{
    fmt::Debug,
    ops::Deref,
    rc::Rc,
    sync::atomic::{AtomicUsize, Ordering},
};

use lock_api::{RawRwLock, RwLock};

#[derive(Debug)]
pub struct LockVec<T, L: RawRwLock> {
    data: RwLock<L, Vec<T>>,
}

impl<T, L: RawRwLock> LockVec<T, L> {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(Vec::new()),
        }
    }
}

pub struct RawStorage<T, L: RawRwLock, const N: usize> {
    data: Box<[LockVec<T, L>; N]>,
    len: AtomicUsize,
}

impl<T: std::fmt::Debug + Default, L: RawRwLock, const N: usize> RawStorage<T, L, N> {
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

    pub fn push(&self, value: T) {
        let index = self.len.fetch_add(1, Ordering::SeqCst);
        let (bucket, offset) = self.resolve(index);
        let mut vec = self.data[bucket].data.write();
        vec.resize_with(offset, || T::default());
        vec.insert(offset, value);
    }

    pub fn entry(&self, index: usize) -> Entry<'_, T, L> {
        let (bucket, offset) = self.resolve(index);
        let guard: lock_api::RwLockReadGuard<'_, L, Vec<T>> = self.data[bucket].data.read();
        Entry { i: offset, guard }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    pub fn items<'a>(&'a self) -> IterableGuards<'a, T, L> {
        let guards = self.data.iter().map(|vec| vec.data.read()).collect();
        IterableGuards { guards }
    }

    pub fn iter2<'a>(&'a self) -> Iter<'a, T, L, N> {
        Iter {
            raw: self,
            i: 0,
            current: None,
        }
    }
}

pub struct IterableGuards<'a, T: 'static, L: RawRwLock> {
    guards: Vec<lock_api::RwLockReadGuard<'a, L, Vec<T>>>,
}

impl<'a, T: 'static, L: RawRwLock> IterableGuards<'a, T, L> {
    pub fn iter(&'a self) -> Box<dyn Iterator<Item = &'a T> + 'a> {
        let iter = self.guards.iter().flat_map(|guard| guard.iter());
        Box::new(iter)
    }
}

pub struct Entry<'a, T: 'static, L: RawRwLock> {
    i: usize,
    guard: lock_api::RwLockReadGuard<'a, L, Vec<T>>,
}

impl<'a, T, L: lock_api::RawRwLock> Deref for Entry<'a, T, L> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard[self.i]
    }
}

type GuardIter<'a, T, L> = (
    Rc<lock_api::RwLockReadGuard<'a, L, Vec<T>>>,
    std::slice::Iter<'a, T>,
);

pub struct Iter<'a, T, L: lock_api::RawRwLock, const N: usize> {
    raw: &'a RawStorage<T, L, N>,
    i: usize,
    current: Option<GuardIter<'a, T, L>>,
}

pub struct RefT<'a, T, L: lock_api::RawRwLock, const N: usize> {
    _guard: Rc<lock_api::RwLockReadGuard<'a, L, Vec<T>>>,
    t: &'a T,
}

impl<'a, T, L: lock_api::RawRwLock, const N: usize> Deref for RefT<'a, T, L, N> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.t
    }
}

// simple impl for RefT that returns &T in the value function
impl<'a, T, L: lock_api::RawRwLock, const N: usize> RefT<'a, T, L, N> {
    pub fn value(&self) -> &T {
        self.t
    }
}

/// # Safety
///
/// Requires that you ensure the reference does not become invalid.
/// The object has to outlive the reference.
pub unsafe fn change_lifetime_const<'a, 'b, T>(x: &'a T) -> &'b T {
    &*(x as *const T)
}

impl<'a, T: Debug + Default, L: lock_api::RawRwLock, const N: usize> Iterator
    for Iter<'a, T, L, N>
{
    type Item = RefT<'a, T, L, N>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = self.current.as_mut() {
                if let Some(t) = current.1.next() {
                    let guard = current.0.clone();
                    return Some(RefT { _guard: guard, t });
                }
            }

            if self.i >= N {
                return None;
            }

            let guard = self.raw.data[self.i].data.read();

            let raw = unsafe { change_lifetime_const(&*guard) };

            let iter = raw.iter();

            self.current = Some((Rc::new(guard), iter));

            self.i += 1;
        }
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
