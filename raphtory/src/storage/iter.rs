use std::{rc::Rc, ops::Deref};

use super::RawStorage;



pub struct Iter<'a, T, L: lock_api::RawRwLock, const N: usize> {
    raw: &'a RawStorage<T, L, N>,
    i: usize,
    current: Option<GuardIter<'a, T, L>>,
}

// impl new for Iter
impl<'a, T, L: lock_api::RawRwLock, const N: usize> Iter<'a, T, L, N> {
    pub fn new(raw: &'a RawStorage<T, L, N>) -> Self {
        Iter {
            raw,
            i: 0,
            current: None,
        }
    }
}

type GuardIter<'a, T, L> = (
    Rc<lock_api::RwLockReadGuard<'a, L, Vec<T>>>,
    std::slice::Iter<'a, T>,
);

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

impl<'a, T: std::fmt::Debug + Default, L: lock_api::RawRwLock, const N: usize> Iterator
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