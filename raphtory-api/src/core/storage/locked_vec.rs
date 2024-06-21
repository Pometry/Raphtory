use crate::core::storage::ArcRwLockReadGuard;
use std::ops::Deref;

#[derive(Debug)]
pub struct ArcReadLockedVec<T> {
    pub(crate) guard: ArcRwLockReadGuard<Vec<T>>,
}

impl<T> Deref for ArcReadLockedVec<T> {
    type Target = Vec<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<T: Clone> IntoIterator for ArcReadLockedVec<T> {
    type Item = T;
    type IntoIter = LockedIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        let guard = self.guard;
        let len = guard.len();
        let pos = 0;
        LockedIter { guard, pos, len }
    }
}

pub struct LockedIter<T> {
    guard: ArcRwLockReadGuard<Vec<T>>,
    pos: usize,
    len: usize,
}

impl<T: Clone> Iterator for LockedIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.len {
            let next_val = Some(self.guard[self.pos].clone());
            self.pos += 1;
            next_val
        } else {
            None
        }
    }
}
