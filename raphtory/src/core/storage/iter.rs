use super::RawStorage;
use std::{ops::Deref, sync::Arc};

pub struct Iter<'a, T, const N: usize> {
    raw: &'a RawStorage<T, N>,
    segment: usize,
    offset: usize,
    current: Option<GuardIter<'a, T>>,
}

// impl new for Iter
impl<'a, T, const N: usize> Iter<'a, T, N> {
    pub fn new(raw: &'a RawStorage<T, N>) -> Self {
        Iter {
            raw,
            segment: 0,
            offset: 0,
            current: None,
        }
    }
}

type GuardIter<'a, T> = (
    Arc<parking_lot::RwLockReadGuard<'a, Vec<Option<T>>>>,
    std::iter::Flatten<std::slice::Iter<'a, std::option::Option<T>>>,
);

pub struct RefT<'a, T, const N: usize> {
    _guard: Arc<parking_lot::RwLockReadGuard<'a, Vec<Option<T>>>>,
    t: &'a T,
    i: usize,
}

impl<'a, T, const N: usize> Clone for RefT<'_, T, N> {
    fn clone(&self) -> Self {
        RefT {
            _guard: self._guard.clone(),
            t: self.t,
            i: self.i,
        }
    }
}

impl<'a, T, const N: usize> Deref for RefT<'a, T, N> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.t
    }
}

// simple impl for RefT that returns &T in the value function
impl<'a, T, const N: usize> RefT<'a, T, N> {
    pub fn value(&self) -> &T {
        self.t
    }

    pub fn index(&self) -> usize {
        self.i
    }
}

/// # Safety
///
/// Requires that you ensure the reference does not become invalid.
/// The object has to outlive the reference.
pub unsafe fn change_lifetime_const<'a, 'b, T>(x: &'a T) -> &'b T {
    &*(x as *const T)
}

impl<'a, T: std::fmt::Debug + Default, const N: usize> Iterator for Iter<'a, T, N> {
    type Item = RefT<'a, T, N>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((guard, iter)) = self.current.as_mut() {
                if let Some(t) = iter.next() {
                    let guard = guard.clone();
                    let next = Some(RefT {
                        _guard: guard,
                        i: (self.offset * N + (self.segment - 1)),
                        t,
                    });
                    self.offset += 1;
                    return next;
                }
            }

            if self.segment >= N {
                return None;
            }

            // get the next segment
            let guard = self.raw.data[self.segment].data.read();

            // convince the rust compiler that the reference is valid
            let raw = unsafe { change_lifetime_const(&*guard) };

            // grab the iterator
            let iter = raw.iter().flatten();

            // set the current segment with the new iterator
            self.current = Some((Arc::new(guard), iter));
            self.offset = 0;
            self.segment += 1;
        }
    }
}
