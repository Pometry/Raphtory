use super::locked_view::LockedView;
use crate::core::entities::LayerIds;
use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::BTreeSet,
    fmt::Debug,
    iter,
    marker::PhantomData,
    ops::Range,
};

pub use raphtory_api::core::storage::timeindex::*;

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

    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = T> + Send + '_> {
        match self {
            TimeIndex::Empty => Box::new(iter::empty()),
            TimeIndex::One(t) => Box::new(iter::once(*t)),
            TimeIndex::Set(ts) => Box::new(ts.iter().copied()),
        }
    }

    pub(crate) fn range_iter(
        &self,
        w: Range<T>,
    ) -> Box<dyn DoubleEndedIterator<Item = T> + Send + '_> {
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

impl<T: AsTime> TimeIndexLike for TimeIndex<T> {
    fn range_iter(
        &self,
        w: Range<Self::IndexType>,
    ) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
        Box::new(self.range_iter(w))
    }

    fn first_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        self.range_iter(w).next()
    }

    fn last_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        self.range_iter(w).next_back()
    }
}

#[derive(Clone, Debug)]
pub enum TimeIndexWindow<'a, T: AsTime, TI> {
    Empty,
    TimeIndexRange { timeindex: &'a TI, range: Range<T> },
    All(&'a TI),
}

impl<'a, T: AsTime, TI: TimeIndexLike<IndexType = T>> TimeIndexWindow<'a, T, TI> {
    pub fn len(&self) -> usize {
        match self {
            TimeIndexWindow::Empty => 0,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.range_iter(range.clone()).count()
            }
            TimeIndexWindow::All(ts) => ts.len(),
        }
    }
}

impl<'a, T: AsTime, TI: TimeIndexLike<IndexType = T>> TimeIndexIntoOps
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
                    if let Some(min_val) = ts.first() {
                        if let Some(max_val) = ts.last() {
                            if min_val >= w.start && max_val < w.end {
                                TimeIndexWindow::All(ts)
                            } else {
                                TimeIndexWindow::TimeIndexRange {
                                    timeindex: ts,
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

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send {
        match self {
            TimeIndexWindow::Empty => Box::new(iter::empty()),
            TimeIndexWindow::TimeIndexRange { timeindex, range } => timeindex.range_iter(range),
            TimeIndexWindow::All(timeindex) => timeindex.iter(),
        }
    }
}

pub struct LayeredTimeIndexWindow<'a, Ops> {
    timeindex: Vec<Ops>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, Ops: TimeIndexOps + 'a> LayeredTimeIndexWindow<'a, Ops> {
    fn new(timeindex: Vec<Ops>) -> Self {
        Self {
            timeindex,
            _marker: PhantomData,
        }
    }
}

pub type LockedLayeredIndex<'a, T> =
    LayeredIndex<'a, TimeIndex<T>, LockedView<'a, Vec<TimeIndex<T>>>>;

pub struct LayeredIndex<'a, Ops: TimeIndexOps + 'a, V: AsRef<Vec<Ops>>> {
    layers: LayerIds,
    view: V,
    _marker: PhantomData<&'a Ops>,
}

impl<'a, Ops: TimeIndexOps + 'a, V: AsRef<Vec<Ops>>> LayeredIndex<'a, Ops, V> {
    pub fn new(layers: LayerIds, view: V) -> Self {
        Self {
            layers,
            view,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: AsTime, Ops: TimeIndexOps<IndexType = T>, V: AsRef<Vec<Ops>> + Send + Sync> TimeIndexOps
    for LayeredIndex<'a, Ops, V>
{
    type IndexType = Ops::IndexType;
    type RangeType<'b>
        = LayeredTimeIndexWindow<'b, Ops::RangeType<'b>>
    where
        Self: 'b;

    #[inline(always)]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.view.as_ref().iter().any(|t| t.active(w.clone()))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType<'_> {
        let timeindex = self
            .view
            .as_ref()
            .iter()
            .enumerate()
            .filter(|&(l, _)| self.layers.contains(&l))
            .map(|(_, t)| t.range(w.clone()))
            .collect_vec();
        LayeredTimeIndexWindow::new(timeindex)
    }

    fn first(&self) -> Option<T> {
        self.view
            .as_ref()
            .iter()
            .enumerate()
            .filter(|&(l, _)| self.layers.contains(&l))
            .flat_map(|(_, t)| t.first())
            .min()
    }

    fn last(&self) -> Option<T> {
        self.view
            .as_ref()
            .iter()
            .enumerate()
            .filter(|&(l, _)| self.layers.contains(&l))
            .flat_map(|(_, t)| t.last())
            .max()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = T> + Send + '_> {
        Box::new(self.view.as_ref().iter().map(|t| t.iter()).kmerge().dedup())
    }

    fn len(&self) -> usize {
        self.iter().count()
    }
}

impl<T: AsTime> TimeIndexOps for TimeIndex<T> {
    type IndexType = T;
    type RangeType<'a>
        = TimeIndexWindow<'a, T, Self>
    where
        Self: 'a;

    #[inline(always)]
    fn active(&self, w: Range<T>) -> bool {
        match &self {
            TimeIndex::Empty => false,
            TimeIndex::One(t) => w.contains(t),
            TimeIndex::Set(ts) => ts.range(w).next().is_some(),
        }
    }

    fn range(&self, w: Range<T>) -> TimeIndexWindow<T, Self> {
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

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
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

impl<'b, T: AsTime, TI: TimeIndexLike<IndexType = T>> TimeIndexOps for TimeIndexWindow<'b, T, TI>
where
    Self: 'b,
{
    type IndexType = T;
    type RangeType<'a>
        = TimeIndexWindow<'a, T, TI>
    where
        Self: 'a;

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

    fn range(&self, w: Range<T>) -> Self::RangeType<'_> {
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
            TimeIndexWindow::All(timeindex) => TimeIndexWindow::TimeIndexRange {
                timeindex,
                range: w,
            },
        }
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

    fn iter(&self) -> Box<dyn Iterator<Item = T> + Send + '_> {
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

impl<'a, Ops: TimeIndexOps + 'a> TimeIndexOps for LayeredTimeIndexWindow<'a, Ops> {
    type IndexType = Ops::IndexType;
    type RangeType<'b>
        = LayeredTimeIndexWindow<'b, Ops::RangeType<'b>>
    where
        Self: 'b;

    #[inline(always)]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.timeindex.iter().any(|t| t.active(w.clone()))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType<'_> {
        let timeindex = self
            .timeindex
            .iter()
            .map(|t| t.range(w.clone()))
            .collect_vec();
        LayeredTimeIndexWindow::new(timeindex)
    }

    fn first(&self) -> Option<Self::IndexType> {
        self.timeindex.iter().flat_map(|t| t.first()).min()
    }

    fn last(&self) -> Option<Self::IndexType> {
        self.timeindex.iter().flat_map(|t| t.last()).max()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
        Box::new(self.timeindex.iter().map(|t| t.iter()).kmerge())
    }

    fn len(&self) -> usize {
        self.timeindex.par_iter().map(|ts| ts.len()).sum()
    }
}
