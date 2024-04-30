use crate::{
    arrow::graph_impl::tprops::{ArrowTProp, TPropColumn},
    core::{
        entities::properties::tprop::TProp,
        storage::{locked_view::LockedView, timeindex::AsTime},
        Prop,
    },
    db::api::storage::storage_variants::StorageVariants,
    prelude::TimeIndexEntry,
};
use itertools::Itertools;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::ops::{Deref, Range};
use tantivy::SegmentComponent::Store;

#[derive(Copy, Clone, Debug)]
pub enum TPropRef<'a> {
    Mem(&'a TProp),
    #[cfg(feature = "arrow")]
    Arrow(ArrowTProp<'a>),
}

pub trait TPropOps<'a>: Sized + Copy + 'a {
    fn active(self, w: Range<i64>) -> bool {
        self.iter_window_t(w).next().is_some()
    }
    fn last_before(self, t: i64) -> Option<(TimeIndexEntry, Prop)>;

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a;
    fn iter_t(self) -> impl Iterator<Item = (i64, Prop)> + Send + 'a {
        self.iter().map(|(t, v)| (t.t(), v))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a;

    fn iter_window_t(self, r: Range<i64>) -> impl Iterator<Item = (i64, Prop)> + Send + 'a {
        self.iter_window(TimeIndexEntry::range(r))
            .map(|(t, v)| (t.t(), v))
    }
    fn iter_window_te(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (i64, Prop)> + Send + 'a {
        self.iter_window(r).map(|(t, v)| (t.t(), v))
    }
    fn at(self, ti: &TimeIndexEntry) -> Option<Prop>;

    fn len(self) -> usize;

    fn is_empty(self) -> bool {
        self.len() == 0
    }
}

impl<'a> TPropOps<'a> for TPropRef<'a> {
    fn last_before(self, t: i64) -> Option<(TimeIndexEntry, Prop)> {
        match self {
            TPropRef::Arrow(tprop) => tprop.last_before(t),
            TPropRef::Mem(t_prop) => t_prop.last_before(t),
        }
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        match self {
            TPropRef::Arrow(tprop) => StorageVariants::Arrow(tprop.iter()),
            TPropRef::Mem(t_prop) => StorageVariants::Mem(t_prop.iter()),
        }
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        match self {
            TPropRef::Arrow(tprop) => StorageVariants::Arrow(tprop.iter_window(r)),
            TPropRef::Mem(t_prop) => StorageVariants::Mem(t_prop.iter_window(r)),
        }
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        match self {
            TPropRef::Arrow(tprop) => tprop.at(ti),
            TPropRef::Mem(t_prop) => t_prop.at(ti),
        }
    }

    fn len(self) -> usize {
        match self {
            TPropRef::Mem(prop) => prop.len(),
            TPropRef::Arrow(prop) => prop.len(),
        }
    }
}
