use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::collections::btree_set::Iter;
use std::collections::BTreeSet;
use std::ops::Range;

#[repr(transparent)]
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimeIndex(BTreeSet<i64>);

impl TimeIndex {
    pub fn one(t: i64) -> Self {
        let mut s = Self::default();
        s.insert(t);
        s
    }
    pub fn insert(&mut self, t: i64) -> bool {
        self.0.insert(t)
    }

    pub(crate) fn range_iter(&self, w: Range<i64>) -> std::collections::btree_set::Range<'_, i64> {
        self.0.range(w)
    }
}

pub enum TimeIndexWindow<'a> {
    Empty,
    TimeIndexRange {
        timeindex: &'a TimeIndex,
        range: Range<i64>,
    },
    All(&'a TimeIndex),
}

pub enum WindowIter<'a> {
    Empty,
    TimeIndexRange(std::collections::btree_set::Range<'a, i64>),
    All(Iter<'a, i64>),
}

impl<'a> Iterator for WindowIter<'a> {
    type Item = &'a i64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            WindowIter::Empty => None,
            WindowIter::TimeIndexRange(iter) => iter.next(),
            WindowIter::All(iter) => iter.next(),
        }
    }
}

pub trait TimeIndexOps {
    type IterType<'a>: Iterator<Item = &'a i64> + 'a
    where
        Self: 'a;

    fn active(&self, w: Range<i64>) -> bool;

    fn range(&self, w: Range<i64>) -> TimeIndexWindow;

    fn first(&self) -> Option<i64>;

    fn last(&self) -> Option<i64>;

    fn iter(&self) -> Self::IterType<'_>;
}

impl TimeIndexOps for TimeIndex {
    type IterType<'a> = Iter<'a, i64> where Self: 'a ;

    fn active(&self, w: Range<i64>) -> bool {
        self.0.range(w).next().is_some()
    }

    fn range(&self, w: Range<i64>) -> TimeIndexWindow<'_> {
        TimeIndexWindow::TimeIndexRange {
            timeindex: self,
            range: w,
        }
    }

    fn first(&self) -> Option<i64> {
        self.0.first().copied()
    }

    fn last(&self) -> Option<i64> {
        self.0.last().copied()
    }

    fn iter(&self) -> Iter<'_, i64> {
        self.0.iter()
    }
}

impl<'b> TimeIndexOps for TimeIndexWindow<'b> {
    type IterType<'a> = WindowIter<'a> where Self: 'a;

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

    fn range(&self, w: Range<i64>) -> TimeIndexWindow {
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

    fn first(&self) -> Option<i64> {
        match self {
            TimeIndexWindow::Empty => None,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.range_iter(range.clone()).next().copied()
            }
            TimeIndexWindow::All(timeindex) => timeindex.first(),
        }
    }

    fn last(&self) -> Option<i64> {
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
            TimeIndexWindow::Empty => WindowIter::Empty,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                WindowIter::TimeIndexRange(timeindex.range_iter(range.clone()))
            }
            TimeIndexWindow::All(timeindex) => WindowIter::All(timeindex.iter()),
        }
    }
}
