use crate::core::{
    entities::properties::prop::Prop,
    storage::timeindex::{AsTime, EventTime},
};
use std::ops::Range;

pub trait TPropOps<'a>: Clone + Send + Sync + Sized + 'a {
    fn active(&self, w: Range<EventTime>) -> bool {
        self.clone().iter_window(w).next().is_some()
    }

    /// Is there any event in this window
    fn active_t(&self, w: Range<i64>) -> bool {
        self.clone().iter_window_t(w).next().is_some()
    }

    fn last_before(&self, t: EventTime) -> Option<(EventTime, Prop)> {
        self.clone().iter_inner_rev(Some(EventTime::MIN..t)).next()
    }

    fn iter_inner(
        self,
        range: Option<Range<EventTime>>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'a;

    fn iter_inner_rev(
        self,
        range: Option<Range<EventTime>>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'a;

    fn iter(self) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'a {
        self.iter_inner(None)
    }

    fn iter_rev(self) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'a {
        self.iter_inner_rev(None)
    }

    fn iter_window(
        self,
        r: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'a {
        self.iter_inner(Some(r))
    }

    fn iter_window_rev(
        self,
        r: Range<EventTime>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'a {
        self.iter_inner_rev(Some(r))
    }

    fn iter_t(self) -> impl Iterator<Item = (i64, Prop)> + Send + Sync + 'a {
        self.iter().map(|(t, v)| (t.t(), v))
    }

    fn iter_window_t(self, r: Range<i64>) -> impl Iterator<Item = (i64, Prop)> + Send + Sync + 'a {
        self.iter_window(EventTime::range(r))
            .map(|(t, v)| (t.t(), v))
    }

    fn iter_window_te(
        self,
        r: Range<EventTime>,
    ) -> impl Iterator<Item = (i64, Prop)> + Send + Sync + 'a {
        self.iter_window(r).map(|(t, v)| (t.t(), v))
    }

    fn at(&self, ti: &EventTime) -> Option<Prop>;
}
