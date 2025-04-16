use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::BTreeSet,
    fmt::Debug,
    iter,
    ops::Range,
};

pub use raphtory_api::core::storage::timeindex::*;
use raphtory_api::iter::{BoxedLIter, IntoDynBoxed};

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
}

impl<'a, T: AsTime> TimeIndexLike<'a> for &'a TimeIndex<T> {
    fn range_iter(&self, w: Range<Self::IndexType>) -> BoxedLIter<'a, Self::IndexType> {
        Box::new((*self).range_iter(w))
    }

    fn range_iter_rev(&self, w: Range<Self::IndexType>) -> BoxedLIter<'a, Self::IndexType> {
        (*self).range_iter(w).rev().into_dyn_boxed()
    }

    fn last_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        (*self).range_iter(w).next_back()
    }
}

#[derive(Clone, Debug)]
pub enum TimeIndexWindow<'a, T: AsTime, TI> {
    Empty,
    Range { timeindex: &'a TI, range: Range<T> },
    All(&'a TI),
}

impl<'a, T: AsTime, TI> TimeIndexWindow<'a, T, TI>
where
    &'a TI: TimeIndexLike<'a, IndexType = T>,
{
    pub fn len(&self) -> usize {
        match self {
            TimeIndexWindow::Empty => 0,
            TimeIndexWindow::Range { timeindex, range } => {
                timeindex.range_iter(range.clone()).count()
            }
            TimeIndexWindow::All(ts) => ts.len(),
        }
    }

    pub fn with_range(&self, w: Range<T>) -> TimeIndexWindow<'a, T, TI> {
        match self {
            TimeIndexWindow::Empty => TimeIndexWindow::Empty,
            TimeIndexWindow::Range { range, timeindex } => {
                let start = range.start.max(w.start);
                let end = range.start.min(w.end);
                if end <= start {
                    TimeIndexWindow::Empty
                } else {
                    TimeIndexWindow::Range {
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
                                TimeIndexWindow::Range {
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

impl<'a, T: AsTime> TimeIndexOps<'a> for &'a TimeIndex<T> {
    type IndexType = T;
    type RangeType = TimeIndexWindow<'a, T, TimeIndex<T>>;

    #[inline(always)]
    fn active(&self, w: Range<T>) -> bool {
        match &self {
            TimeIndex::Empty => false,
            TimeIndex::One(t) => w.contains(t),
            TimeIndex::Set(ts) => ts.range(w).next().is_some(),
        }
    }

    fn range(&self, w: Range<T>) -> Self::RangeType {
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
                            TimeIndexWindow::Range {
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
        range
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
            TimeIndex::Empty => iter::empty().into_dyn_boxed(),
            TimeIndex::One(t) => iter::once(*t).into_dyn_boxed(),
            TimeIndex::Set(ts) => ts.iter().copied().into_dyn_boxed(),
        }
    }

    fn iter_rev(&self) -> BoxedLIter<'a, Self::IndexType> {
        match self {
            TimeIndex::Empty => iter::empty().into_dyn_boxed(),
            TimeIndex::One(t) => iter::once(*t).into_dyn_boxed(),
            TimeIndex::Set(ts) => ts.iter().rev().copied().into_dyn_boxed(),
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
    type RangeType = Self;

    #[inline(always)]
    fn active(&self, w: Range<T>) -> bool {
        match self {
            TimeIndexWindow::Empty => false,
            TimeIndexWindow::Range { timeindex, range } => {
                w.start < range.end
                    && w.end > range.start
                    && (timeindex.active(max(w.start, range.start)..min(w.end, range.end)))
            }
            TimeIndexWindow::All(timeindex) => timeindex.active(w),
        }
    }

    fn range(&self, w: Range<T>) -> Self {
        let range = match self {
            TimeIndexWindow::Empty => TimeIndexWindow::Empty,
            TimeIndexWindow::Range { timeindex, range } => {
                let start = max(range.start, w.start);
                let end = min(range.start, w.start);
                if end <= start {
                    TimeIndexWindow::Empty
                } else {
                    TimeIndexWindow::Range {
                        timeindex: *timeindex,
                        range: start..end,
                    }
                }
            }
            TimeIndexWindow::All(timeindex) => TimeIndexWindow::Range {
                timeindex: *timeindex,
                range: w,
            },
        };
        range
    }

    fn first(&self) -> Option<T> {
        match self {
            TimeIndexWindow::Empty => None,
            TimeIndexWindow::Range { timeindex, range } => timeindex.first_range(range.clone()),
            TimeIndexWindow::All(timeindex) => timeindex.first(),
        }
    }

    fn last(&self) -> Option<T> {
        match self {
            TimeIndexWindow::Empty => None,
            TimeIndexWindow::Range { timeindex, range } => timeindex.last_range(range.clone()),
            TimeIndexWindow::All(timeindex) => timeindex.last(),
        }
    }

    fn iter(&self) -> BoxedLIter<'b, Self::IndexType> {
        match self {
            TimeIndexWindow::Empty => iter::empty().into_dyn_boxed(),
            TimeIndexWindow::Range { timeindex, range } => {
                timeindex.range_iter(range.clone()).into_dyn_boxed()
            }
            TimeIndexWindow::All(timeindex) => timeindex.iter().into_dyn_boxed(),
        }
    }

    fn iter_rev(&self) -> BoxedLIter<'b, Self::IndexType> {
        match self {
            TimeIndexWindow::Empty => iter::empty().into_dyn_boxed(),
            TimeIndexWindow::Range { timeindex, range } => {
                timeindex.range_iter(range.clone()).into_dyn_boxed()
            }
            TimeIndexWindow::All(timeindex) => timeindex.iter().into_dyn_boxed(),
        }
    }

    fn len(&self) -> usize {
        match self {
            TimeIndexWindow::Empty => 0,
            TimeIndexWindow::Range { timeindex, range } => {
                timeindex.range_iter(range.clone()).count()
            }
            TimeIndexWindow::All(ts) => ts.len(),
        }
    }
}
