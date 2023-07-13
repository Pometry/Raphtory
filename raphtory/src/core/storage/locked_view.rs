use dashmap::mapref::one::Ref;
use parking_lot::{MappedRwLockReadGuard, RwLockReadGuard};
use rustc_hash::FxHasher;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::{hash::BuildHasherDefault, ops::Deref};
use tantivy::directory::Lock;

pub enum LockedView<'a, T> {
    LockMapped(parking_lot::MappedRwLockReadGuard<'a, T>),
    Locked(parking_lot::RwLockReadGuard<'a, T>),
    DashMap(Ref<'a, usize, T, BuildHasherDefault<rustc_hash::FxHasher>>),
}

impl<'a, T, O> AsRef<T> for LockedView<'a, O>
where
    T: ?Sized,
    <LockedView<'a, O> as Deref>::Target: AsRef<T>,
{
    fn as_ref(&self) -> &T {
        self.deref().as_ref()
    }
}

impl<'a, T> Borrow<T> for LockedView<'a, T> {
    fn borrow(&self) -> &T {
        self.deref()
    }
}

impl<'a> From<LockedView<'a, String>> for String {
    fn from(value: LockedView<'a, String>) -> Self {
        value.deref().clone()
    }
}

impl<'a, T: PartialEq<Rhs>, Rhs, LRhs: Deref<Target = Rhs>> PartialEq<LRhs> for LockedView<'a, T> {
    fn eq(&self, other: &LRhs) -> bool {
        self.deref() == other.deref()
    }
}

impl<'a, T: Eq> Eq for LockedView<'a, T> {}

impl<'a, T: PartialOrd<Rhs>, Rhs, LRhs: Deref<Target = Rhs>> PartialOrd<LRhs>
    for LockedView<'a, T>
{
    fn partial_cmp(&self, other: &LRhs) -> Option<Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl<'a, T: Ord> Ord for LockedView<'a, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deref().cmp(other.deref())
    }
}

impl<'a, T: Hash> Hash for LockedView<'a, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.deref().hash(state)
    }
}

impl<'a, T> From<parking_lot::MappedRwLockReadGuard<'a, T>> for LockedView<'a, T> {
    fn from(value: MappedRwLockReadGuard<'a, T>) -> Self {
        Self::LockMapped(value)
    }
}

impl<'a, T> From<parking_lot::RwLockReadGuard<'a, T>> for LockedView<'a, T> {
    fn from(value: RwLockReadGuard<'a, T>) -> Self {
        Self::Locked(value)
    }
}

impl<'a, T> From<Ref<'a, usize, T, BuildHasherDefault<rustc_hash::FxHasher>>>
    for LockedView<'a, T>
{
    fn from(value: Ref<'a, usize, T, BuildHasherDefault<FxHasher>>) -> Self {
        Self::DashMap(value)
    }
}

impl<'a, T> Deref for LockedView<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            LockedView::LockMapped(guard) => guard.deref(),
            LockedView::DashMap(r) => (*r).deref(),
            LockedView::Locked(guard) => guard.deref(),
        }
    }
}

impl<'a, T: Debug> Debug for LockedView<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LockedView({:?})", self.deref())
    }
}
