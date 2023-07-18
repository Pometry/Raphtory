use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::BTreeSet,
    ops::Range,
    sync::Arc,
};

use super::locked_view::LockedView;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TimeIndex {
    #[default]
    Empty,
    One(i64),
    Set(BTreeSet<i64>),
}

impl TimeIndex {

    pub fn is_empty(&self) -> bool {
        match self {
            TimeIndex::Empty => true,
            _ => false,
        }
    }

    pub fn one(t: i64) -> Self {
        Self::One(t)
    }
    pub fn insert(&mut self, t: i64) -> bool {
        match self {
            TimeIndex::Empty => {
                *self = TimeIndex::One(t);
                true
            }
            TimeIndex::One(t0) => {
                if *t0 == t {
                    false
                } else {
                    *self = TimeIndex::Set([*t0, t].into_iter().collect());
                    true
                }
            }
            TimeIndex::Set(ts) => ts.insert(t),
        }
    }

    pub(crate) fn range_iter(
        &self,
        w: Range<i64>,
    ) -> Box<dyn DoubleEndedIterator<Item = &i64> + Send + '_> {
        match self {
            TimeIndex::Empty => Box::new(std::iter::empty()),
            TimeIndex::One(t) => {
                if w.contains(t) {
                    Box::new(std::iter::once(t))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TimeIndex::Set(ts) => Box::new(ts.range(w)),
        }
    }

    // = note: see issue #65991 <https://github.com/rust-lang/rust/issues/65991> for more information
    // = note: required when coercing `Box<dyn DoubleEndedIterator<Item = &i64> + Send>` into `Box<dyn Iterator<Item = &i64> + Send>`
    pub(crate) fn range_iter_forward(
        &self,
        w: Range<i64>,
    ) -> Box<dyn Iterator<Item = &i64> + Send + '_> {
        match self {
            TimeIndex::Empty => Box::new(std::iter::empty()),
            TimeIndex::One(t) => {
                if w.contains(t) {
                    Box::new(std::iter::once(t))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TimeIndex::Set(ts) => Box::new(ts.range(w)),
        }
    }
}

pub enum TimeIndexWindow<'a> {
    Empty,
    TimeIndexRange {
        timeindex: &'a TimeIndex,
        range: Range<i64>,
    },
    LayeredTimeIndex {
        timeindex: &'a LockedLayeredIndex<'a>,
        range: Range<i64>,
    },
    All(&'a TimeIndex),
}

pub enum WindowIter<'a> {
    Empty,
    TimeIndexRange(Box<dyn Iterator<Item = &'a i64> + Send + 'a>),
    All(Box<dyn Iterator<Item = &'a i64> + Send + 'a>),
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

pub struct LockedLayeredIndex<'a> {
    layers: Vec<usize>,
    view: LockedView<'a, Vec<TimeIndex>>,
}

impl<'a> LockedLayeredIndex<'a> {
    pub fn new(layers: Vec<usize>, view: LockedView<'a, Vec<TimeIndex>>) -> Self {
        Self { layers, view }
    }

    pub fn range_iter(&'a self, w: Range<i64>) -> Box<dyn Iterator<Item = &i64> + Send + '_> {
        let iter = self
            .view
            .iter()
            .enumerate()
            .filter(|(i, _)| self.layers.contains(i))
            .map(|(_, t)| t.range_iter(w.clone()))
            .kmerge()
            .dedup();
        Box::new(iter)
    }

    pub fn first(&self) -> Option<i64> {
        self.view
            .iter()
            .enumerate()
            .filter(|(i, _)| self.layers.contains(i))
            .map(|(_, t)| t.first())
            .min()
            .flatten()
    }

    pub fn active(&self, w: Range<i64>) -> bool {
        self.view
            .iter()
            .enumerate()
            .filter(|(i, _)| self.layers.contains(i))
            .any(|(_, t)| t.active(w.clone()))
    }

    fn last_window(&self, w: Range<i64>) -> Option<i64> {
        self.view
            .iter()
            .enumerate()
            .filter(|(i, _)| self.layers.contains(i))
            .map(|(_, t)| t.range_iter(w.clone()).next_back().copied())
            .max()
            .flatten()
    }
}

impl<'a> TimeIndexOps for LockedLayeredIndex<'a> {
    type IterType<'b> = Box<dyn Iterator<Item = &'b i64> + Send + 'b> where Self: 'b;

    fn active(&self, w: Range<i64>) -> bool {
        self.view.iter().any(|t| t.active(w.clone()))
    }

    fn range(&self, w: Range<i64>) -> TimeIndexWindow {
        TimeIndexWindow::LayeredTimeIndex {
            timeindex: self,
            range: w,
        }
    }

    fn first(&self) -> Option<i64> {
        self.view.iter().map(|t| t.first()).min().flatten()
    }

    fn last(&self) -> Option<i64> {
        self.view.iter().map(|t| t.last()).max().flatten()
    }

    fn iter(&self) -> Self::IterType<'_> {
        let iter = self.view.iter().map(|t| t.iter()).kmerge().dedup();
        Box::new(iter)
    }
}

pub trait TimeIndexOps {
    type IterType<'a>: Iterator<Item = &'a i64> + Send + 'a
    where
        Self: 'a;

    fn active(&self, w: Range<i64>) -> bool;

    fn range(&self, w: Range<i64>) -> TimeIndexWindow;

    fn first(&self) -> Option<i64>;

    fn last(&self) -> Option<i64>;

    fn iter(&self) -> Self::IterType<'_>;
}

impl TimeIndexOps for TimeIndex {
    type IterType<'a> = Box<dyn Iterator<Item = &'a i64> + Send + 'a>;

    fn active(&self, w: Range<i64>) -> bool {
        self.range_iter(w).next().is_some()
    }

    fn range(&self, w: Range<i64>) -> TimeIndexWindow<'_> {
        TimeIndexWindow::TimeIndexRange {
            timeindex: self,
            range: w,
        }
    }

    fn first(&self) -> Option<i64> {
        match self {
            TimeIndex::Empty => None,
            TimeIndex::One(t) => Some(*t),
            TimeIndex::Set(ts) => ts.first().copied(),
        }
    }

    fn last(&self) -> Option<i64> {
        match self {
            TimeIndex::Empty => None,
            TimeIndex::One(t) => Some(*t),
            TimeIndex::Set(ts) => ts.last().copied(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = &i64> + Send + '_> {
        match self {
            TimeIndex::Empty => Box::new(std::iter::empty()),
            TimeIndex::One(t) => Box::new(std::iter::once(t)),
            TimeIndex::Set(ts) => Box::new(ts.iter()),
        }
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
            TimeIndexWindow::LayeredTimeIndex { timeindex, range } => {
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
            TimeIndexWindow::LayeredTimeIndex { timeindex, range } => {
                let start = max(range.start, w.start);
                let end = min(range.start, w.start);
                if end <= start {
                    TimeIndexWindow::Empty
                } else {
                    TimeIndexWindow::LayeredTimeIndex {
                        timeindex: timeindex.clone(),
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

            TimeIndexWindow::LayeredTimeIndex { timeindex, range } => {
                timeindex.range_iter(range.clone()).min().copied()
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
            TimeIndexWindow::LayeredTimeIndex { timeindex, range } => {
                timeindex.last_window(range.clone())
            }
            TimeIndexWindow::All(timeindex) => timeindex.last(),
        }
    }

    fn iter(&self) -> Self::IterType<'_> {
        match self {
            TimeIndexWindow::Empty => WindowIter::Empty,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                WindowIter::TimeIndexRange(timeindex.range_iter_forward(range.clone()))
            }
            TimeIndexWindow::LayeredTimeIndex { timeindex, range } => {
                WindowIter::TimeIndexRange(timeindex.range_iter(range.clone()))
            }
            TimeIndexWindow::All(timeindex) => WindowIter::All(timeindex.iter()),
        }
    }
}
