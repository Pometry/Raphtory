use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::BTreeSet,
    fmt::Debug,
    iter,
    ops::Range,
};

pub use raphtory_api::core::storage::timeindex::*;
use raphtory_api::iter::BoxedLIter;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TimeIndex<T: Ord + Eq + Copy + Debug> {
    #[default]
    Empty,
    One(T),
    Set(BTreeSet<T>),
}

impl<T: Ord + Eq + Copy + Debug> Default for &TimeIndex<T> {
    fn default() -> Self {
        &TimeIndex::Empty
    }
}

impl<T: AsTime> TimeIndex<T> {
    pub fn is_empty(&self) -> bool {
        matches!(self, TimeIndex::Empty)
    }

    pub fn len(&self) -> usize {
        match self {
            TimeIndex::Empty => 0,
            TimeIndex::One(_) => 1,
            TimeIndex::Set(ts) => ts.len(),
        }
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

    #[allow(unused)]
    pub(crate) fn contains(&self, w: Range<i64>) -> bool {
        match self {
            TimeIndex::Empty => false,
            TimeIndex::One(t) => w.contains(&t.t()),
            TimeIndex::Set(ts) => ts.range(T::range(w)).next().is_some(),
        }
    }

    pub(crate) fn iter(&self) -> BoxedLIter<T> {
        match self {
            TimeIndex::Empty => Box::new(iter::empty()),
            TimeIndex::One(t) => Box::new(iter::once(*t)),
            TimeIndex::Set(ts) => Box::new(ts.iter().copied()),
        }
    }

    pub(crate) fn range_iter(
        &self,
        w: Range<T>,
    ) -> Box<dyn DoubleEndedIterator<Item = T> + Send + Sync + '_> {
        match self {
            TimeIndex::Empty => Box::new(iter::empty()),
            TimeIndex::One(t) => {
                if w.contains(t) {
                    Box::new(iter::once(*t))
                } else {
                    Box::new(iter::empty())
                }
            }
            TimeIndex::Set(ts) => Box::new(ts.range(w).copied()),
        }
    }

    pub(crate) fn range_inner(&self, w: Range<T>) -> TimeIndexWindow<T, Self> {
        match &self {
            TimeIndex::Empty => TimeIndexWindow::Empty,
            TimeIndex::One(t) => {
                if w.contains(t) {
                    TimeIndexWindow::All(self)
                } else {
                    TimeIndexWindow::Empty
                }
            }
            TimeIndex::Set(ts) => {
                if let Some(min_val) = ts.first() {
                    if let Some(max_val) = ts.last() {
                        if min_val >= &w.start && max_val < &w.end {
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
}

impl<'a, T: AsTime> TimeIndexLike<'a> for &'a TimeIndex<T> {
    fn range_iter(&self, w: Range<Self::IndexType>) -> BoxedLIter<'a, Self::IndexType> {
        Box::new((*self).range_iter(w))
    }

    fn last_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        (*self).range_iter(w).next_back()
    }
}

#[derive(Clone, Debug)]
pub enum TimeIndexWindow<'a, T: AsTime, TI> {
    Empty,
    TimeIndexRange { timeindex: &'a TI, range: Range<T> },
    All(&'a TI),
}

impl<'a, T: AsTime, TI> TimeIndexWindow<'a, T, TI>
where
    &'a TI: TimeIndexLike<'a, IndexType = T>,
{
    pub fn len(&self) -> usize {
        match self {
            TimeIndexWindow::Empty => 0,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.range_iter(range.clone()).count()
            }
            TimeIndexWindow::All(ts) => ts.len(),
        }
    }

    pub fn with_range(&self, w: Range<T>) -> TimeIndexWindow<'a, T, TI> {
        match self {
            TimeIndexWindow::Empty => TimeIndexWindow::Empty,
            TimeIndexWindow::TimeIndexRange { range, timeindex } => {
                let start = range.start.max(w.start);
                let end = range.start.min(w.end);
                if end <= start {
                    TimeIndexWindow::Empty
                } else {
                    TimeIndexWindow::TimeIndexRange {
                        timeindex: *timeindex,
                        range: start..end,
                    }
                }
            }
            TimeIndexWindow::All(ts) => {
                if ts.len() == 0 {
                    TimeIndexWindow::Empty
                } else {
                    ts.first()
                        .zip(ts.last())
                        .map(|(min_val, max_val)| {
                            if min_val >= w.start && max_val < w.end {
                                TimeIndexWindow::All(*ts)
                            } else {
                                TimeIndexWindow::TimeIndexRange {
                                    timeindex: *ts,
                                    range: w,
                                }
                            }
                        })
                        .unwrap_or(TimeIndexWindow::Empty)
                }
            }
        }
    }
}

impl<'a, T: AsTime, TI: TimeIndexLike<'a, IndexType = T>> TimeIndexIntoOps
    for TimeIndexWindow<'a, T, TI>
{
    type IndexType = T;
    type RangeType = Self;

    fn into_range(self, w: Range<T>) -> Self {
        match self {
            TimeIndexWindow::Empty => TimeIndexWindow::Empty,
            TimeIndexWindow::TimeIndexRange { range, timeindex } => {
                let start = range.start.max(w.start);
                let end = range.start.min(w.end);
                if end <= start {
                    TimeIndexWindow::Empty
                } else {
                    TimeIndexWindow::TimeIndexRange {
                        timeindex,
                        range: start..end,
                    }
                }
            }
            TimeIndexWindow::All(ts) => {
                if ts.len() == 0 {
                    TimeIndexWindow::Empty
                } else {
                    ts.first()
                        .zip(ts.last())
                        .map(|(min_val, max_val)| {
                            if min_val >= w.start && max_val < w.end {
                                TimeIndexWindow::All(ts)
                            } else {
                                TimeIndexWindow::TimeIndexRange {
                                    timeindex: ts,
                                    range: w,
                                }
                            }
                        })
                        .unwrap_or(TimeIndexWindow::Empty)
                }
            }
        }
    }

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync {
        match self {
            TimeIndexWindow::Empty => Box::new(iter::empty()),
            TimeIndexWindow::TimeIndexRange { timeindex, range } => timeindex.range_iter(range),
            TimeIndexWindow::All(timeindex) => timeindex.iter(),
        }
    }
}

impl<'a, T: AsTime> TimeIndexOps<'a> for &'a TimeIndex<T> {
    type IndexType = T;

    #[inline(always)]
    fn active(&self, w: Range<T>) -> bool {
        match &self {
            TimeIndex::Empty => false,
            TimeIndex::One(t) => w.contains(t),
            TimeIndex::Set(ts) => ts.range(w).next().is_some(),
        }
    }

    fn range(&self, w: Range<T>) -> Box<dyn TimeIndexOps<'a, IndexType = Self::IndexType> + 'a> {
        let range = match self {
            TimeIndex::Empty => TimeIndexWindow::Empty,
            TimeIndex::One(t) => {
                if w.contains(t) {
                    TimeIndexWindow::All(*self)
                } else {
                    TimeIndexWindow::Empty
                }
            }
            TimeIndex::Set(ts) => {
                if let Some(min_val) = ts.first() {
                    if let Some(max_val) = ts.last() {
                        if min_val >= &w.start && max_val < &w.end {
                            TimeIndexWindow::All(*self)
                        } else {
                            TimeIndexWindow::TimeIndexRange {
                                timeindex: *self,
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
        };
        Box::new(range)
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

    fn iter(&self) -> BoxedLIter<'a, Self::IndexType> {
        match self {
            TimeIndex::Empty => Box::new(iter::empty()),
            TimeIndex::One(t) => Box::new(iter::once(*t)),
            TimeIndex::Set(ts) => Box::new(ts.iter().copied()),
        }
    }

    fn len(&self) -> usize {
        match self {
            TimeIndex::Empty => 0,
            TimeIndex::One(_) => 1,
            TimeIndex::Set(ts) => ts.len(),
        }
    }
}

impl<'b, T: AsTime, TI> TimeIndexOps<'b> for TimeIndexWindow<'b, T, TI>
where
    &'b TI: TimeIndexLike<'b, IndexType = T>,
    Self: 'b,
{
    type IndexType = T;

    #[inline(always)]
    fn active(&self, w: Range<T>) -> bool {
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

    fn range(&self, w: Range<T>) -> Box<dyn TimeIndexOps<'b, IndexType = Self::IndexType> + 'b> {
        let range = match self {
            TimeIndexWindow::Empty => TimeIndexWindow::Empty,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                let start = max(range.start, w.start);
                let end = min(range.start, w.start);
                if end <= start {
                    TimeIndexWindow::Empty
                } else {
                    TimeIndexWindow::TimeIndexRange {
                        timeindex: *timeindex,
                        range: start..end,
                    }
                }
            }
            TimeIndexWindow::All(timeindex) => TimeIndexWindow::TimeIndexRange {
                timeindex: *timeindex,
                range: w,
            },
        };
        Box::new(range)
    }

    fn first(&self) -> Option<T> {
        match self {
            TimeIndexWindow::Empty => None,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.first_range(range.clone())
            }
            TimeIndexWindow::All(timeindex) => timeindex.first(),
        }
    }

    fn last(&self) -> Option<T> {
        match self {
            TimeIndexWindow::Empty => None,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.last_range(range.clone())
            }
            TimeIndexWindow::All(timeindex) => timeindex.last(),
        }
    }

    fn iter(&self) -> BoxedLIter<'b, T> {
        match self {
            TimeIndexWindow::Empty => Box::new(iter::empty()),
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                Box::new(timeindex.range_iter(range.clone()))
            }
            TimeIndexWindow::All(timeindex) => Box::new(timeindex.iter()),
        }
    }

    fn len(&self) -> usize {
        match self {
            TimeIndexWindow::Empty => 0,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.range_iter(range.clone()).count()
            }
            TimeIndexWindow::All(ts) => ts.len(),
        }
    }
}
