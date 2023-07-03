use dashmap::mapref::one::Ref;
use std::{hash::BuildHasherDefault, ops::Deref};

pub enum LockedView<'a, T> {
    Locked(parking_lot::MappedRwLockReadGuard<'a, T>),
    DashMap(Ref<'a, usize, T, BuildHasherDefault<rustc_hash::FxHasher>>),
}

impl<'a, T> Deref for LockedView<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            LockedView::Locked(guard) => guard.deref(),
            LockedView::DashMap(r) => (*r).deref(),
        }
    }
}
