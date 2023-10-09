use crate::{
    core::{entities::LayerIds, utils::time::error::ParseTimeError},
    db::api::mutation::{internal::InternalAdditionOps, InputTime, TryIntoInputTime},
};
use itertools::{Itertools, KMerge};
use num_traits::Saturating;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::BTreeSet,
    fmt::Debug,
    iter,
    iter::{Copied, Map, Zip},
    marker::PhantomData,
    ops::{Deref, Range},
    slice::Iter,
    sync::Arc,
};
use tantivy::time::Time;

use super::locked_view::LockedView;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Ord, PartialOrd, Eq)]
pub struct TimeIndexEntry(pub i64, pub usize);

pub trait AsTime: Debug + Copy + Ord + Eq + Send + Sync {
    fn t(&self) -> &i64;
    fn range(w: Range<i64>) -> Range<Self>;
}

impl From<i64> for TimeIndexEntry {
    fn from(value: i64) -> Self {
        Self::start(value)
    }
}

impl TimeIndexEntry {
    pub const MIN: TimeIndexEntry = TimeIndexEntry(i64::MIN, 0);

    pub const MAX: TimeIndexEntry = TimeIndexEntry(i64::MAX, usize::MAX);
    pub fn new(t: i64, s: usize) -> Self {
        Self(t, s)
    }

    pub fn from_input<G: InternalAdditionOps, T: TryIntoInputTime>(
        g: &G,
        t: T,
    ) -> Result<Self, ParseTimeError> {
        let t = t.try_into_input_time()?;
        Ok(match t {
            InputTime::Simple(t) => Self::new(t, g.next_event_id()),
            InputTime::Indexed(t, s) => Self::new(t, s),
        })
    }

    pub fn start(t: i64) -> Self {
        Self(t, 0)
    }

    pub fn end(t: i64) -> Self {
        Self(t.saturating_add(1), 0)
    }
}

impl AsTime for i64 {
    fn t(&self) -> &i64 {
        self
    }

    fn range(w: Range<i64>) -> Range<Self> {
        w
    }
}

impl AsTime for TimeIndexEntry {
    fn t(&self) -> &i64 {
        &self.0
    }
    fn range(w: Range<i64>) -> Range<Self> {
        Self::start(w.start)..Self::start(w.end)
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TimeIndex<T: Ord + Eq + Copy + Debug> {
    #[default]
    Empty,
    One(T),
    Set(BTreeSet<T>),
}

impl<T: AsTime> TimeIndex<T> {
    pub fn is_empty(&self) -> bool {
        matches!(self, TimeIndex::Empty)
    }

    pub fn one(ti: T) -> Self {
        Self::One(ti)
    }
    pub fn insert(&mut self, ti: T) -> bool {
        match self {
            TimeIndex::Empty => {
                *self = TimeIndex::One(ti);
                true
            }
            TimeIndex::One(t0) => {
                if t0 == &ti {
                    false
                } else {
                    *self = TimeIndex::Set([*t0, ti].into_iter().collect());
                    true
                }
            }
            TimeIndex::Set(ts) => ts.insert(ti),
        }
    }

    pub(crate) fn contains(&self, w: Range<i64>) -> bool {
        match self {
            TimeIndex::Empty => false,
            TimeIndex::One(t) => w.contains(t.t()),
            TimeIndex::Set(ts) => ts.range(T::range(w)).next().is_some(),
        }
    }

    pub(crate) fn iter_ref(&self) -> Box<dyn Iterator<Item = &T> + Send + '_> {
        match self {
            TimeIndex::Empty => Box::new(std::iter::empty()),
            TimeIndex::One(t) => Box::new(std::iter::once(t)),
            TimeIndex::Set(ts) => Box::new(ts.iter()),
        }
    }

    pub(crate) fn range_iter(
        &self,
        w: Range<i64>,
    ) -> Box<dyn DoubleEndedIterator<Item = &T> + Send + '_> {
        match self {
            TimeIndex::Empty => Box::new(std::iter::empty()),
            TimeIndex::One(t) => {
                if w.contains(t.t()) {
                    Box::new(std::iter::once(t))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TimeIndex::Set(ts) => Box::new(ts.range(T::range(w))),
        }
    }

    // = note: see issue #65991 <https://github.com/rust-lang/rust/issues/65991> for more information
    // = note: required when coercing `Box<dyn DoubleEndedIterator<Item = &i64> + Send>` into `Box<dyn Iterator<Item = &i64> + Send>`
    pub(crate) fn range_iter_forward(
        &self,
        w: Range<i64>,
    ) -> Box<dyn Iterator<Item = &T> + Send + '_> {
        Box::new(self.range_iter(w))
    }
}

pub enum TimeIndexWindow<'a, T: AsTime> {
    Empty,
    TimeIndexRange {
        timeindex: &'a TimeIndex<T>,
        range: Range<i64>,
    },
    All(&'a TimeIndex<T>),
}

pub struct LayeredTimeIndexWindow<'a, T: AsTime> {
    timeindex: Vec<TimeIndexWindow<'a, T>>,
}

pub type LockedLayeredIndex<'a, T> = LayeredIndex<'a, T, LockedView<'a, Vec<TimeIndex<T>>>>;

pub struct LayeredIndex<'a, T: AsTime, V: AsRef<[TimeIndex<T>]> + 'a> {
    layers: &'a LayerIds,
    view: V,
    marker: PhantomData<&'a Vec<TimeIndex<T>>>,
}

impl<'a, T: AsTime, V: AsRef<[TimeIndex<T>]> + 'a + Sync> LayeredIndex<'a, T, V> {
    pub fn new(layers: &'a LayerIds, view: V) -> Self {
        Self {
            layers,
            view,
            marker: PhantomData,
        }
    }

    pub fn range_iter(&'a self, w: Range<i64>) -> impl Iterator<Item = &i64> + Send + '_ {
        self.layer_iter()
            .map(|t| t.range_iter(w.clone()).map(|t| t.t()))
            .kmerge()
            .dedup()
    }

    pub fn layer_iter(&self) -> impl Iterator<Item = &TimeIndex<T>> + Send + '_ {
        self.view
            .as_ref()
            .iter()
            .enumerate()
            .filter(|(i, _)| self.layers.contains(i))
            .map(|(_, t)| t)
    }
}

impl<'a, T: AsTime, V: AsRef<[TimeIndex<T>]> + 'a + Sync> TimeIndexOps<T>
    for LayeredIndex<'a, T, V>
{
    type IterType<'b> = Box<dyn Iterator<Item = T> + Send + 'b> where Self: 'b;
    type WindowType<'b> = LayeredTimeIndexWindow<'b, T> where Self: 'b;

    fn active(&self, w: Range<i64>) -> bool {
        self.layer_iter().any(|t| t.active(w.clone()))
    }

    fn range(&self, w: Range<i64>) -> LayeredTimeIndexWindow<T> {
        let timeindex = self.layer_iter().map(|l| l.range(w.clone())).collect();
        LayeredTimeIndexWindow { timeindex }
    }

    fn first(&self) -> Option<T> {
        self.layer_iter().flat_map(|t| t.first()).min()
    }

    fn last(&self) -> Option<T> {
        self.layer_iter().flat_map(|t| t.last()).max()
    }

    fn iter(&self) -> Self::IterType<'_> {
        let iter = self
            .layer_iter()
            .map(|t| t.iter_ref().copied())
            .kmerge()
            .dedup();
        Box::new(iter)
    }
}

pub trait TimeIndexOps<T: AsTime> {
    type IterType<'a>: Iterator<Item = T> + Send + 'a
    where
        Self: 'a;

    type WindowType<'a>: TimeIndexOps<T> + 'a
    where
        Self: 'a;

    fn active(&self, w: Range<i64>) -> bool;

    fn range(&self, w: Range<i64>) -> Self::WindowType<'_>;

    fn first_t(&self) -> Option<i64> {
        self.first().map(|ti| *ti.t())
    }

    fn first(&self) -> Option<T>;

    fn last_t(&self) -> Option<i64> {
        self.last().map(|ti| *ti.t())
    }

    fn last(&self) -> Option<T>;

    fn iter(&self) -> Self::IterType<'_>;

    fn iter_t(&self) -> Map<Self::IterType<'_>, fn(T) -> i64> {
        self.iter().map(|t| *t.t())
    }

    fn is_empty(&self) -> bool {
        self.first().is_none()
    }
}

impl<T: AsTime> TimeIndexOps<T> for TimeIndex<T> {
    type IterType<'a> = Box<dyn Iterator<Item = T> + Send + 'a> where T: 'a;
    type WindowType<'a> = TimeIndexWindow<'a, T> where Self: 'a;

    #[inline(always)]
    fn active(&self, w: Range<i64>) -> bool {
        match &self {
            TimeIndex::Empty => false,
            TimeIndex::One(t) => w.contains(t.t()),
            TimeIndex::Set(ts) => ts.range(T::range(w)).next().is_some(),
        }
    }

    fn range(&self, w: Range<i64>) -> TimeIndexWindow<'_, T> {
        match &self {
            TimeIndex::Empty => TimeIndexWindow::Empty,
            TimeIndex::One(t) => {
                if w.contains(t.t()) {
                    TimeIndexWindow::All(self)
                } else {
                    TimeIndexWindow::Empty
                }
            }
            TimeIndex::Set(ts) => {
                if let Some(min_val) = ts.first() {
                    if let Some(max_val) = ts.last() {
                        if min_val.t() >= &w.start && max_val.t() < &w.end {
                            TimeIndexWindow::All(self)
                        } else {
                            TimeIndexWindow::TimeIndexRange {
                                timeindex: self,
                                range: w,
                            }
                        }
                    } else {
                        TimeIndexWindow::Empty
                    }
                } else {
                    TimeIndexWindow::Empty
                }
            }
        }
    }

    fn first(&self) -> Option<T> {
        match self {
            TimeIndex::Empty => None,
            TimeIndex::One(t) => Some(*t),
            TimeIndex::Set(ts) => ts.first().copied(),
        }
    }

    fn last(&self) -> Option<T> {
        match self {
            TimeIndex::Empty => None,
            TimeIndex::One(t) => Some(*t),
            TimeIndex::Set(ts) => ts.last().copied(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = T> + Send + '_> {
        match self {
            TimeIndex::Empty => Box::new(std::iter::empty()),
            TimeIndex::One(t) => Box::new(std::iter::once(*t)),
            TimeIndex::Set(ts) => Box::new(ts.iter().copied()),
        }
    }
}

impl<'b, T: AsTime> TimeIndexOps<T> for TimeIndexWindow<'b, T>
where
    Self: 'b,
{
    type IterType<'a> = Box<dyn Iterator<Item=T> + Send + 'a> where Self: 'a;
    type WindowType<'a> = TimeIndexWindow<'a, T> where Self: 'a;

    fn active(&self, w: Range<i64>) -> bool {
        match self {
            TimeIndexWindow::Empty => false,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                w.start < range.end
                    && w.end > range.start
                    && (timeindex.active(max(w.start, range.start)..min(w.end, range.end)))
            }
            TimeIndexWindow::All(timeindex) => timeindex.active(w),
        }
    }

    fn range(&self, w: Range<i64>) -> TimeIndexWindow<T> {
        match self {
            TimeIndexWindow::Empty => TimeIndexWindow::Empty,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                let start = max(range.start, w.start);
                let end = min(range.start, w.start);
                if end <= start {
                    TimeIndexWindow::Empty
                } else {
                    TimeIndexWindow::TimeIndexRange {
                        timeindex,
                        range: start..end,
                    }
                }
            }
            TimeIndexWindow::All(timeindex) => timeindex.range(w),
        }
    }

    fn first(&self) -> Option<T> {
        match self {
            TimeIndexWindow::Empty => None,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.range_iter(range.clone()).next().copied()
            }
            TimeIndexWindow::All(timeindex) => timeindex.first(),
        }
    }

    fn last(&self) -> Option<T> {
        match self {
            TimeIndexWindow::Empty => None,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.range_iter(range.clone()).next_back().copied()
            }
            TimeIndexWindow::All(timeindex) => timeindex.last(),
        }
    }

    fn iter(&self) -> Self::IterType<'_> {
        match self {
            TimeIndexWindow::Empty => Box::new(iter::empty()),
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                Box::new(timeindex.range_iter(range.clone()).copied())
            }
            TimeIndexWindow::All(timeindex) => Box::new(timeindex.iter_ref().copied()),
        }
    }
}

impl<'b, T: AsTime> TimeIndexOps<T> for LayeredTimeIndexWindow<'b, T>
where
    Self: 'b,
{
    type IterType<'a> = KMerge<Box<dyn Iterator<Item=T> + Send + 'a>> where Self: 'a;
    type WindowType<'a> = LayeredTimeIndexWindow<'a, T> where Self: 'a;

    fn active(&self, w: Range<i64>) -> bool {
        self.timeindex.iter().any(|t| t.active(w.clone()))
    }

    fn range(&self, w: Range<i64>) -> Self::WindowType<'_> {
        let timeindex = self
            .timeindex
            .iter()
            .map(|t| t.range(w.clone()))
            .collect_vec();
        Self::WindowType { timeindex }
    }

    fn first(&self) -> Option<T> {
        self.timeindex.iter().flat_map(|t| t.first()).min()
    }

    fn last(&self) -> Option<T> {
        self.timeindex.iter().flat_map(|t| t.last()).max()
    }

    fn iter(&self) -> Self::IterType<'_> {
        self.timeindex.iter().map(|t| t.iter()).kmerge()
    }
}

pub struct SortedTimeIndexT<'a>(pub &'a [i64]);

pub struct SortedTimeIndex<'a> {
    timestamps: &'a [i64],
    secondary: &'a [u64],
}

impl<'a> TimeIndexOps<TimeIndexEntry> for SortedTimeIndex<'a> {
    type IterType<'b>  = Map<Zip<Iter<'b, i64>, Iter<'b, u64>>, fn((&i64, &u64)) -> TimeIndexEntry> where Self: 'b;
    type WindowType<'b>  = SortedTimeIndex<'b> where Self: 'b;

    #[inline]
    fn active(&self, w: Range<i64>) -> bool {
        let index = SortedTimeIndexT(self.timestamps);
        <SortedTimeIndexT as TimeIndexOps<i64>>::active(&index, w)
    }

    fn range(&self, w: Range<i64>) -> Self::WindowType<'_> {
        if w.start >= w.end {
            SortedTimeIndex {
                timestamps: &[],
                secondary: &[],
            }
        } else {
            let start = match self.timestamps.binary_search(&w.start) {
                Ok(i) => i,
                Err(i) => i,
            };
            let end = match self.timestamps.binary_search(&w.end) {
                Ok(i) => i,
                Err(i) => i,
            };
            SortedTimeIndex {
                timestamps: &self.timestamps[start..end],
                secondary: &self.secondary[start..end],
            }
        }
    }

    fn first(&self) -> Option<TimeIndexEntry> {
        self.timestamps.first().and_then(|t| {
            self.secondary
                .first()
                .map(|s| TimeIndexEntry(*t, *s as usize))
        })
    }

    fn last(&self) -> Option<TimeIndexEntry> {
        self.timestamps.last().and_then(|t| {
            self.secondary
                .last()
                .map(|s| TimeIndexEntry(*t, *s as usize))
        })
    }

    fn iter(&self) -> Self::IterType<'_> {
        self.timestamps
            .iter()
            .zip(self.secondary.iter())
            .map(|(&t, &s)| TimeIndexEntry(t, s as usize))
    }
}

impl<'a> TimeIndexOps<i64> for SortedTimeIndexT<'a> {
    type IterType<'b>  = Copied<Iter<'b, i64>> where Self: 'b;
    type WindowType<'b>  = SortedTimeIndexT<'b> where Self: 'b;

    fn active(&self, w: Range<i64>) -> bool {
        if w.start < w.end {
            false
        } else {
            match self.0.binary_search(&w.start) {
                Ok(_) => true,
                Err(i) => self.0.get(i).filter(|&t| t < &w.end).is_some(),
            }
        }
    }

    fn range(&self, w: Range<i64>) -> Self::WindowType<'_> {
        if w.start < w.end {
            SortedTimeIndexT(&[])
        } else {
            let start = match self.0.binary_search(&w.start) {
                Ok(i) => i,
                Err(i) => i,
            };
            let end = match self.0.binary_search(&w.end) {
                Ok(i) => i,
                Err(i) => i,
            };
            SortedTimeIndexT(&self.0[start..end])
        }
    }

    fn first(&self) -> Option<i64> {
        self.0.first().copied()
    }

    fn last(&self) -> Option<i64> {
        self.0.last().copied()
    }

    fn iter(&self) -> Self::IterType<'_> {
        self.0.iter().copied()
    }
}

pub enum TimeIndexViewT<'a> {
    Windowed(TimeIndexWindow<'a, i64>),
    Indexed(&'a TimeIndex<i64>),
    Sorted(SortedTimeIndexT<'a>),
}

impl<'a> From<TimeIndexWindow<'a, i64>> for TimeIndexViewT<'a> {
    fn from(value: TimeIndexWindow<'a, i64>) -> Self {
        Self::Windowed(value)
    }
}

impl<'a> From<&'a TimeIndex<i64>> for TimeIndexViewT<'a> {
    fn from(value: &'a TimeIndex<i64>) -> Self {
        Self::Indexed(value)
    }
}

impl<'a> From<SortedTimeIndexT<'a>> for TimeIndexViewT<'a> {
    fn from(value: SortedTimeIndexT<'a>) -> Self {
        Self::Sorted(value)
    }
}

impl<'a> TimeIndexOps<i64> for TimeIndexViewT<'a> {
    type IterType<'b> = Box<dyn Iterator<Item=i64> + Send + 'b> where Self: 'b;
    type WindowType<'b>  = TimeIndexViewT<'b> where Self: 'b;

    fn active(&self, w: Range<i64>) -> bool {
        match self {
            Self::Windowed(v) => v.active(w),
            Self::Indexed(v) => v.active(w),
            Self::Sorted(v) => <SortedTimeIndexT as TimeIndexOps<i64>>::active(v, w),
        }
    }

    fn range(&self, w: Range<i64>) -> Self::WindowType<'_> {
        match self {
            Self::Windowed(v) => v.range(w).into(),
            Self::Indexed(v) => v.range(w).into(),
            Self::Sorted(v) => <SortedTimeIndexT as TimeIndexOps<i64>>::range(v, w).into(),
        }
    }

    fn first(&self) -> Option<i64> {
        match self {
            Self::Windowed(v) => v.first(),
            Self::Indexed(v) => v.first(),
            Self::Sorted(v) => v.first(),
        }
    }

    fn last(&self) -> Option<i64> {
        match self {
            Self::Windowed(v) => v.last(),
            Self::Indexed(v) => v.last(),
            Self::Sorted(v) => v.last(),
        }
    }

    fn iter(&self) -> Self::IterType<'_> {
        match self {
            Self::Windowed(v) => Box::new(v.iter()),
            Self::Indexed(v) => Box::new(v.iter()),
            Self::Sorted(v) => Box::new(v.iter()),
        }
    }
}

pub enum TimeIndexView<'a> {
    Windowed(TimeIndexWindow<'a, TimeIndexEntry>),
    Layered(LayeredIndex<'a, TimeIndexEntry, &'a [TimeIndex<TimeIndexEntry>]>),
    LayeredWindowed(LayeredTimeIndexWindow<'a, TimeIndexEntry>),
    Indexed(&'a TimeIndex<TimeIndexEntry>),
    Sorted(SortedTimeIndex<'a>),
    SortedT(SortedTimeIndexT<'a>), // FIXME: hack until we add secondary index to arrow
}

impl<'a> From<TimeIndexWindow<'a, TimeIndexEntry>> for TimeIndexView<'a> {
    fn from(value: TimeIndexWindow<'a, TimeIndexEntry>) -> Self {
        Self::Windowed(value)
    }
}

impl<'a> From<LayeredIndex<'a, TimeIndexEntry, &'a [TimeIndex<TimeIndexEntry>]>>
    for TimeIndexView<'a>
{
    fn from(value: LayeredIndex<'a, TimeIndexEntry, &'a [TimeIndex<TimeIndexEntry>]>) -> Self {
        Self::Layered(value)
    }
}

impl<'a> From<LayeredTimeIndexWindow<'a, TimeIndexEntry>> for TimeIndexView<'a> {
    fn from(value: LayeredTimeIndexWindow<'a, TimeIndexEntry>) -> Self {
        Self::LayeredWindowed(value)
    }
}

impl<'a> From<&'a TimeIndex<TimeIndexEntry>> for TimeIndexView<'a> {
    fn from(value: &'a TimeIndex<TimeIndexEntry>) -> Self {
        Self::Indexed(value)
    }
}

impl<'a> From<SortedTimeIndex<'a>> for TimeIndexView<'a> {
    fn from(value: SortedTimeIndex<'a>) -> Self {
        Self::Sorted(value)
    }
}

impl<'a> From<SortedTimeIndexT<'a>> for TimeIndexView<'a> {
    fn from(value: SortedTimeIndexT<'a>) -> Self {
        Self::SortedT(value)
    }
}

impl<'a> TimeIndexOps<TimeIndexEntry> for TimeIndexView<'a> {
    type IterType<'b> = Box<dyn Iterator<Item=TimeIndexEntry> + Send + 'b> where Self: 'b;
    type WindowType<'b>  = TimeIndexView<'b> where Self: 'b;

    fn active(&self, w: Range<i64>) -> bool {
        match self {
            Self::Windowed(v) => v.active(w),
            Self::Indexed(v) => v.active(w),
            Self::Sorted(v) => v.active(w),
            Self::Layered(v) => v.active(w),
            Self::LayeredWindowed(v) => v.active(w),
            Self::SortedT(v) => v.active(w), // FIXME: arrow secondary index
        }
    }

    fn range(&self, w: Range<i64>) -> Self::WindowType<'_> {
        match self {
            Self::Windowed(v) => v.range(w).into(),
            Self::Indexed(v) => v.range(w).into(),
            Self::Sorted(v) => v.range(w).into(),
            Self::Layered(v) => v.range(w).into(),
            Self::LayeredWindowed(v) => v.range(w).into(),
            Self::SortedT(v) => v.range(w).into(), // FIXME: arrow secondary index
        }
    }

    fn first(&self) -> Option<TimeIndexEntry> {
        match self {
            Self::Windowed(v) => v.first(),
            Self::Indexed(v) => v.first(),
            Self::Sorted(v) => v.first(),
            Self::Layered(v) => v.first(),
            Self::LayeredWindowed(v) => v.first(),
            Self::SortedT(v) => v.first().map(TimeIndexEntry::start),
        }
    }

    fn last(&self) -> Option<TimeIndexEntry> {
        match self {
            Self::Windowed(v) => v.last(),
            Self::Indexed(v) => v.last(),
            Self::Sorted(v) => v.last(),
            Self::Layered(v) => v.last(),
            Self::LayeredWindowed(v) => v.last(),
            Self::SortedT(v) => v.last().map(TimeIndexEntry::start),
        }
    }

    fn iter(&self) -> Self::IterType<'_> {
        match self {
            Self::Windowed(v) => Box::new(v.iter()),
            Self::Indexed(v) => Box::new(v.iter()),
            Self::Sorted(v) => Box::new(v.iter()),
            Self::Layered(v) => Box::new(v.iter()),
            Self::LayeredWindowed(v) => Box::new(v.iter()),
            Self::SortedT(v) => Box::new(v.iter().map(TimeIndexEntry::start)),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::core::storage::timeindex::{TimeIndex, TimeIndexOps, TimeIndexViewT};

    #[test]
    fn test_indexed() {
        let indexed = TimeIndex::one(0i64);
        let generic_view = TimeIndexViewT::Indexed(&indexed);
        assert_eq!(generic_view.first(), Some(0))
    }
}
