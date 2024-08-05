use super::RawStorage;
use std::{ops::Deref, sync::Arc};

pub struct Iter<'a, T: Default, Index> {
    raw: &'a RawStorage<T, Index>,
    segment: usize,
    offset: usize,
    current: Option<GuardIter<'a, T>>,
}

// impl new for Iter
impl<'a, T: Default, Index> Iter<'a, T, Index> {
    pub fn new(raw: &'a RawStorage<T, Index>) -> Self {
        Iter {
            raw,
            segment: 0,
            offset: 0,
            current: None,
        }
    }
}

type GuardIter<'a, T> = (
    Arc<parking_lot::RwLockReadGuard<'a, Vec<T>>>,
    std::slice::Iter<'a, T>,
);

pub struct RefT<'a, T> {
    _guard: Arc<parking_lot::RwLockReadGuard<'a, Vec<T>>>,
    t: &'a T,
}

impl<'a, T> Clone for RefT<'_, T> {
    fn clone(&self) -> Self {
        RefT {
            _guard: self._guard.clone(),
            t: self.t,
        }
    }
}

impl<'a, T> Deref for RefT<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.t
    }
}

// simple impl for RefT that returns &T in the value function
impl<'a, T> RefT<'a, T> {
    pub fn value(&self) -> &T {
        self.t
    }
}

/// # Safety
///
/// Requires that you ensure the reference does not become invalid.
/// The object has to outlive the reference.
pub unsafe fn change_lifetime_const<'b, T>(x: &T) -> &'b T {
    &*(x as *const T)
}

impl<'a, T: std::fmt::Debug + Default, Index> Iterator for Iter<'a, T, Index> {
    type Item = RefT<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((guard, iter)) = self.current.as_mut() {
                if let Some(t) = iter.next() {
                    let guard = guard.clone();
                    let next = Some(RefT { _guard: guard, t });
                    self.offset += 1;
                    return next;
                }
            }

            if self.segment >= self.raw.data.len() {
                return None;
            }

            // get the next segment
            let guard = self.raw.data[self.segment].data.read();

            // convince the rust compiler that the reference is valid
            let raw = unsafe { change_lifetime_const(&*guard) };

            // grab the iterator
            let iter = raw.iter();

            // set the current segment with the new iterator
            self.current = Some((Arc::new(guard), iter));
            self.offset = 0;
            self.segment += 1;
        }
    }
}
