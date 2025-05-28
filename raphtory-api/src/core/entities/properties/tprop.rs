use crate::core::{
    entities::properties::prop::Prop,
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use std::ops::Range;

pub trait TPropOps<'a>: Clone + Send + Sync + Sized + 'a {
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        self.clone().iter_window(w).next().is_some()
    }

    /// Is there any event in this window
    fn active_t(&self, w: Range<i64>) -> bool {
        self.clone().iter_window_t(w).next().is_some()
    }

    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.clone().iter_window(TimeIndexEntry::MIN..t).next_back()
    }

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a;

    fn iter_t(self) -> impl DoubleEndedIterator<Item = (i64, Prop)> + Send + Sync + 'a {
        self.iter().map(|(t, v)| (t.t(), v))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a;

    fn iter_window_t(
        self,
        r: Range<i64>,
    ) -> impl DoubleEndedIterator<Item = (i64, Prop)> + Send + Sync + 'a {
        self.iter_window(TimeIndexEntry::range(r))
            .map(|(t, v)| (t.t(), v))
    }

    fn iter_window_te(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (i64, Prop)> + Send + Sync + 'a {
        self.iter_window(r).map(|(t, v)| (t.t(), v))
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop>;
}
