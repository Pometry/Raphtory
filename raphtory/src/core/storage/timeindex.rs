use super::locked_view::LockedView;
use crate::{
    core::{entities::LayerIds, utils::time::error::ParseTimeError},
    db::api::mutation::{internal::InternalAdditionOps, InputTime, TryIntoInputTime},
};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use num_traits::Saturating;
use raphtory_api::core::entities::VID;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::BTreeSet,
    fmt::Debug,
    iter,
    marker::PhantomData,
    ops::{Deref, Range},
};

use crate::core::utils::iter::GenLockedIter;
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

    // = note: see issue #65991 <https://github.com/rust-lang/rust/issues/65991> for more information
    // = note: required when coercing `Box<dyn DoubleEndedIterator<Item = &i64> + Send>` into `Box<dyn Iterator<Item = &i64> + Send>`
    pub(crate) fn range_iter_forward(
        &self,
        w: Range<T>,
    ) -> Box<dyn Iterator<Item = T> + Send + '_> {
        Box::new(self.range_iter(w))
    }

    pub(crate) fn range_inner(&self, w: Range<T>) -> TimeIndexWindow<T> {
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

#[derive(Clone, Debug)]
pub enum TimeIndexWindow<'a, T: AsTime> {
    Empty,
    TimeIndexRange {
        timeindex: &'a TimeIndex<T>,
        range: Range<T>,
    },
    All(&'a TimeIndex<T>),
}

impl<'a, T: AsTime> TimeIndexWindow<'a, T> {
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

impl<'a, T: AsTime> TimeIndexIntoOps for TimeIndexWindow<'a, T> {
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
            TimeIndexWindow::All(timeindex) => timeindex.range_inner(w),
        }
    }

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send {
        match self {
            TimeIndexWindow::Empty => Box::new(iter::empty()),
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.range_iter_forward(range)
            }
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

    = LayeredTimeIndexWindow<'b, Ops::RangeType<'b>> where
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
            .filter(|&(l, t)| self.layers.contains(&l))
            .map(|(l, t)| t.range(w.clone()))
            .collect_vec();
        LayeredTimeIndexWindow::new(timeindex)
    }

    fn first(&self) -> Option<T> {
        self.view
            .as_ref()
            .iter()
            .enumerate()
            .filter(|&(l, t)| self.layers.contains(&l))
            .flat_map(|(_, t)| t.first())
            .min()
    }

    fn last(&self) -> Option<T> {
        self.view
            .as_ref()
            .iter()
            .enumerate()
            .filter(|&(l, t)| self.layers.contains(&l))
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

pub trait TimeIndexOps: Send + Sync {
    type IndexType: AsTime;
    type RangeType<'a>: TimeIndexOps<IndexType = Self::IndexType> + 'a
    where
        Self: 'a;

    fn active(&self, w: Range<Self::IndexType>) -> bool;

    fn active_t(&self, w: Range<i64>) -> bool {
        self.active(Self::IndexType::range(w))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType<'_>;

    fn range_t(&self, w: Range<i64>) -> Self::RangeType<'_> {
        self.range(Self::IndexType::range(w))
    }

    fn first_t(&self) -> Option<i64> {
        self.first().map(|ti| ti.t())
    }

    fn first(&self) -> Option<Self::IndexType>;

    fn last_t(&self) -> Option<i64> {
        self.last().map(|ti| ti.t())
    }

    fn last(&self) -> Option<Self::IndexType>;

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_>;

    fn iter_t(&self) -> Box<dyn Iterator<Item = i64> + Send + '_> {
        Box::new(self.iter().map(|time| time.t()))
    }

    fn len(&self) -> usize;
}

pub trait TimeIndexIntoOps: Sized {
    type IndexType: AsTime;
    type RangeType: TimeIndexIntoOps<IndexType = Self::IndexType>;

    fn into_range(self, w: Range<Self::IndexType>) -> Self::RangeType;

    fn into_range_t(self, w: Range<i64>) -> Self::RangeType {
        self.into_range(Self::IndexType::range(w))
    }

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send;

    fn into_iter_t(self) -> impl Iterator<Item = i64> + Send {
        self.into_iter().map(|time| time.t())
    }
}

impl<T: AsTime> TimeIndexOps for TimeIndex<T> {
    type IndexType = T;
    type RangeType<'a> = TimeIndexWindow<'a, T> where Self: 'a,;

    #[inline(always)]
    fn active(&self, w: Range<T>) -> bool {
        match &self {
            TimeIndex::Empty => false,
            TimeIndex::One(t) => w.contains(t),
            TimeIndex::Set(ts) => ts.range(w).next().is_some(),
        }
    }

    fn range(&self, w: Range<T>) -> TimeIndexWindow<T> {
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

impl<'b, T: AsTime> TimeIndexOps for TimeIndexWindow<'b, T>
where
    Self: 'b,
{
    type IndexType = T;
    type RangeType<'a> = TimeIndexWindow<'a, T> where Self: 'a;

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
            TimeIndexWindow::All(timeindex) => timeindex.range(w),
        }
    }

    fn first(&self) -> Option<T> {
        match self {
            TimeIndexWindow::Empty => None,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.range_iter(range.clone()).next()
            }
            TimeIndexWindow::All(timeindex) => timeindex.first(),
        }
    }

    fn last(&self) -> Option<T> {
        match self {
            TimeIndexWindow::Empty => None,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                timeindex.range_iter(range.clone()).next_back()
            }
            TimeIndexWindow::All(timeindex) => timeindex.last(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = T> + Send + '_> {
        match self {
            TimeIndexWindow::Empty => Box::new(iter::empty()),
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                Box::new(timeindex.range_iter_forward(range.clone()))
            }
            TimeIndexWindow::All(timeindex) => Box::new(timeindex.iter()),
        }
    }

    fn len(&self) -> usize {
        match self {
            TimeIndexWindow::Empty => 0,
            TimeIndexWindow::TimeIndexRange { timeindex, range } => match timeindex {
                TimeIndex::Empty => 0,
                TimeIndex::One(t) => {
                    if range.contains(t) {
                        1
                    } else {
                        0
                    }
                }
                TimeIndex::Set(ts) => ts.range(range.clone()).count(),
            },
            TimeIndexWindow::All(ts) => ts.len(),
        }
    }
}

impl<'a, Ops: TimeIndexOps + 'a> TimeIndexOps for LayeredTimeIndexWindow<'a, Ops> {
    type IndexType = Ops::IndexType;
    type RangeType<'b> = LayeredTimeIndexWindow<'b, Ops::RangeType<'b>> where Self: 'b;

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

impl<'a, T: AsTime> TimeIndexIntoOps for LockedView<'a, TimeIndex<T>> {
    type IndexType = T;

    type RangeType = LockedTimeIndexWindow<'a, T>;

    fn into_range(self, w: Range<Self::IndexType>) -> Self::RangeType {
        LockedTimeIndexWindow {
            timeindex: self,
            range: w,
        }
    }

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send {
        GenLockedIter::from(self, |t| t.iter())
    }
}

#[derive(Debug)]
pub struct LockedTimeIndexWindow<'a, T: AsTime> {
    timeindex: LockedView<'a, TimeIndex<T>>,
    range: Range<T>,
}

impl<'a, T: AsTime> TimeIndexIntoOps for LockedTimeIndexWindow<'a, T> {
    type IndexType = T;
    type RangeType = Self;

    fn into_range(self, w: Range<Self::IndexType>) -> Self::RangeType {
        let start = w.start.max(self.range.start);
        let end = w.end.min(self.range.end);
        LockedTimeIndexWindow {
            timeindex: self.timeindex,
            range: start..end,
        }
    }

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send {
        GenLockedIter::from(self.timeindex, |t| t.range_iter_forward(self.range.clone()))
    }
}

impl<'a, T: AsTime> TimeIndexOps for LockedTimeIndexWindow<'a, T> {
    type IndexType = T;
    type RangeType<'b> = TimeIndexWindow<'b, T> where Self: 'b;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        let Self { timeindex, range } = self;
        w.start < range.end
            && w.end > range.start
            && (timeindex.active(max(w.start, range.start)..min(w.end, range.end)))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType<'_> {
        let Self { timeindex, range } = self;
        let start = max(range.start, w.start);
        let end = min(range.start, w.start);
        TimeIndexWindow::TimeIndexRange {
            timeindex: timeindex.deref(),
            range: start..end,
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        let Self { timeindex, range } = self;
        timeindex.range_iter(range.clone()).next()
    }

    fn last(&self) -> Option<Self::IndexType> {
        let Self { timeindex, range } = self;
        timeindex.range_iter(range.clone()).next_back()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Self::IndexType> + Send + '_> {
        todo!()
    }

    fn len(&self) -> usize {
        let Self { timeindex, range } = self;
        timeindex.range_iter(range.clone()).count()
    }
}
