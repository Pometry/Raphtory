use crate::core::{entities::properties::tprop::TProp, storage::timeindex::AsTime, Prop};
#[cfg(feature = "storage")]
use crate::db::api::storage::graph::variants::storage_variants::StorageVariants;
#[cfg(feature = "storage")]
use pometry_storage::tprops::DiskTProp;
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use std::ops::Range;

#[derive(Copy, Clone, Debug)]
pub enum TPropRef<'a> {
    Mem(&'a TProp),
    #[cfg(feature = "storage")]
    Disk(DiskTProp<'a, TimeIndexEntry>),
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            TPropRef::Mem($pattern) => $result,
            #[cfg(feature = "storage")]
            TPropRef::Disk($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            TPropRef::Mem($pattern) => StorageVariants::Mem($result),
            TPropRef::Disk($pattern) => StorageVariants::Disk($result),
        }
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            TPropRef::Mem($pattern) => $result,
        }
    };
}

pub trait TPropOps<'a>: Sized + 'a + Send + Copy + Clone {
    fn active(self, w: Range<TimeIndexEntry>) -> bool {
        self.iter_window(w).next().is_some()
    }

    /// Is there any event in this window
    fn active_t(self, w: Range<i64>) -> bool {
        self.iter_window_t(w).next().is_some()
    }

    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)>;

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a;

    fn iter_t(self) -> impl Iterator<Item = (i64, Prop)> + Send + Sync + 'a {
        self.iter().map(|(t, v)| (t.t(), v))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a;

    fn iter_window_t(self, r: Range<i64>) -> impl Iterator<Item = (i64, Prop)> + Send + Sync + 'a {
        self.iter_window(TimeIndexEntry::range(r))
            .map(|(t, v)| (t.t(), v))
    }

    fn iter_window_te(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (i64, Prop)> + Send + Sync + 'a {
        self.iter_window(r).map(|(t, v)| (t.t(), v))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop>;
}

pub trait IndexedTPropOps<'a>: Sized + 'a + Send {
    type PropT: 'a;

    fn prop_at(&self, idx: usize) -> Self::PropT;
}

impl<'a> TPropOps<'a> for TPropRef<'a> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, tprop => tprop.last_before(t))
    }

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_variants!(self, tprop => tprop.iter())
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_variants!(self, tprop => tprop.iter_window(r))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all!(self, tprop => tprop.at(ti))
    }
}
