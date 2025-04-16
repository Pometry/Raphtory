use crate::core::{storage::timeindex::AsTime, Prop};
use raphtory_api::{
    core::storage::timeindex::TimeIndexEntry,
    iter::{BoxedLDIter, IntoDynDBoxed},
};
use std::ops::Range;

pub trait TPropOps<'a>: 'a + Send + Sync {
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        self.iter_window(w).next().is_some()
    }

    /// Is there any event in this window
    fn active_t(&self, w: Range<i64>) -> bool {
        self.iter_window_t(w).next().is_some()
    }

    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.iter_window(TimeIndexEntry::MIN..t).rev().next()
    }

    fn iter(&self) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)>;

    fn iter_t(&self) -> BoxedLDIter<'a, (i64, Prop)> {
        self.iter().map(|(t, v)| (t.t(), v)).into_dyn_dboxed()
    }

    fn iter_window(&self, r: Range<TimeIndexEntry>) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)>;

    fn iter_window_t(&self, r: Range<i64>) -> BoxedLDIter<'a, (i64, Prop)> {
        self.iter_window(TimeIndexEntry::range(r))
            .map(|(t, v)| (t.t(), v))
            .into_dyn_dboxed()
    }

    fn iter_window_te(&self, r: Range<TimeIndexEntry>) -> BoxedLDIter<'a, (i64, Prop)> {
        self.iter_window(r)
            .map(|(t, v)| (t.t(), v))
            .into_dyn_dboxed()
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop>;
}

impl<'a, 'b: 'a, T: TPropOps<'b>> TPropOps<'a> for &'a T {
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        T::active(self, w)
    }

    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        T::last_before(self, t)
    }

    fn iter(&self) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        T::iter(self)
    }

    fn iter_window(&self, r: Range<TimeIndexEntry>) -> BoxedLDIter<'a, (TimeIndexEntry, Prop)> {
        T::iter_window(self, r)
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        T::at(self, ti)
    }
}

pub trait IndexedTPropOps<'a>: Sized + 'a + Send {
    type PropT: 'a;

    fn prop_at(&self, idx: usize) -> Self::PropT;
}
