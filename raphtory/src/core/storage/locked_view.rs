use dashmap::mapref::one::Ref;
use parking_lot::{MappedRwLockReadGuard, RwLockReadGuard};
use rustc_hash::FxHasher;
use std::fmt::{Debug, Formatter};
use std::{hash::BuildHasherDefault, ops::Deref};

pub enum LockedView<'a, T> {
    LockMapped(parking_lot::MappedRwLockReadGuard<'a, T>),
    Locked(parking_lot::RwLockReadGuard<'a, T>),
    DashMap(Ref<'a, usize, T, BuildHasherDefault<rustc_hash::FxHasher>>),
}

impl<'a, T> AsRef<T> for LockedView<'a, T> {
    fn as_ref(&self) -> &T {
        match self {
            LockedView::LockMapped(guard) => guard.deref(),
            LockedView::Locked(guard) => guard.deref(),
            LockedView::DashMap(r) => r.deref(),
        }
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
