use std::{ops::Deref, rc::Rc};

use super::{RawStorage, Entry};

pub struct Iter<'a, T, L: lock_api::RawRwLock, const N: usize> {
    raw: &'a RawStorage<T, L, N>,
    segment: usize,
    offset: usize,
    current: Option<GuardIter<'a, T, L>>,
}

// impl new for Iter
impl<'a, T, L: lock_api::RawRwLock, const N: usize> Iter<'a, T, L, N> {
    pub fn new(raw: &'a RawStorage<T, L, N>) -> Self {
        Iter {
            raw,
            segment: 0,
            offset: 0,
            current: None,
        }
    }
}

type GuardIter<'a, T, L> = (
    Rc<lock_api::RwLockReadGuard<'a, L, Vec<Option<T>>>>,
    std::iter::Flatten<std::slice::Iter<'a, std::option::Option<T>>>,
);

pub struct RefT<'a, T, L: lock_api::RawRwLock, const N: usize> {
    _guard: Rc<lock_api::RwLockReadGuard<'a, L, Vec<Option<T>>>>,
    t: &'a T,
    i: usize,
}

impl <'a, T, L: lock_api::RawRwLock, const N: usize> Clone for RefT<'_, T, L, N> {
    fn clone(&self) -> Self {
        RefT {
            _guard: self._guard.clone(),
            t: self.t,
            i: self.i,
        }
    }
}

impl <'a, T, L: lock_api::RawRwLock, const N: usize> From<Entry<'a, T, L>> for RefT<'a, T, L, N> {
    fn from(value: Entry<'a, T, L>) -> Self {
        todo!()
    }
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

impl<'a, T: std::fmt::Debug + Default, L: lock_api::RawRwLock, const N: usize> Iterator
    for Iter<'a, T, L, N>
{
    type Item = RefT<'a, T, L, N>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = self.current.as_mut() {
                if let Some(t) = current.1.next() {
                    let guard = current.0.clone();
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
            self.current = Some((Rc::new(guard), iter));
            self.offset = 0;
            self.segment += 1;
        }
    }
}
