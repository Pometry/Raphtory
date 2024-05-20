#[cfg(feature = "arrow")]
use crate::db::api::storage::variants::storage_variants::StorageVariants;
use crate::{
    core::{entities::properties::tprop::TProp, storage::timeindex::AsTime, Prop},
    prelude::TimeIndexEntry,
};
#[cfg(feature = "arrow")]
use raphtory_arrow::tprops::ArrowTProp;
use std::ops::Range;

#[derive(Copy, Clone, Debug)]
pub enum TPropRef<'a> {
    Mem(&'a TProp),
    #[cfg(feature = "arrow")]
    Arrow(ArrowTProp<'a, TimeIndexEntry>),
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            TPropRef::Mem($pattern) => $result,
            #[cfg(feature = "arrow")]
            TPropRef::Arrow($pattern) => $result,
        }
    };
}

#[cfg(feature = "arrow")]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            TPropRef::Mem($pattern) => StorageVariants::Mem($result),
            TPropRef::Arrow($pattern) => StorageVariants::Arrow($result),
        }
    };
}

#[cfg(not(feature = "arrow"))]
macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            TPropRef::Mem($pattern) => $result,
        }
    };
}

pub trait TPropOps<'a>: Sized + Copy + 'a + Send {
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
        for_all!(self, tprop => tprop.last_before(t))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_variants!(self, tprop => tprop.iter())
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_variants!(self, tprop => tprop.iter_window(r))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all!(self, tprop => tprop.at(ti))
    }

    fn len(self) -> usize {
        for_all!(self, tprop => tprop.len())
    }
}
