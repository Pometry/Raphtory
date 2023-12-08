use super::locked_view::LockedView;
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
    marker::PhantomData,
    ops::{Deref, Range},
    sync::Arc,
};

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
            TimeIndex::One(t) => w.contains(t.t()),
            TimeIndex::Set(ts) => ts.range(T::range(w)).next().is_some(),
        }
    }

    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = &T> + Send + '_> {
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
    timeindex: Vec<Box<dyn TimeIndexOps<IndexType = T> + 'a>>,
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

pub enum LockedLayeredIndex<'a, T: AsTime>{
    LayeredIndex(LayeredIndex<'a, T, LockedView<'a, Vec<TimeIndex<T>>>>),
    External(Box<dyn TimeIndexOps<IndexType = T> + 'a>),
}

impl <'a, T: AsTime> TimeIndexOps for LockedLayeredIndex<'a, T> {
    type IndexType = T;

    fn active(&self, w: Range<i64>) -> bool {
        match self {
            LockedLayeredIndex::LayeredIndex(t) => t.active(w),
            LockedLayeredIndex::External(t) => t.active(w),
        }
    }

    fn range(
        &self,
        w: Range<i64>,
    ) -> Box<dyn TimeIndexOps<IndexType = Self::IndexType> + '_> {
        match self {
            LockedLayeredIndex::LayeredIndex(t) => t.range(w),
            LockedLayeredIndex::External(t) => t.range(w),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            LockedLayeredIndex::LayeredIndex(t) => t.first(),
            LockedLayeredIndex::External(t) => t.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            LockedLayeredIndex::LayeredIndex(t) => t.last(),
            LockedLayeredIndex::External(t) => t.last(),
        }
    }

    fn iter_t(&self) -> Box<dyn Iterator<Item = &i64> + Send + '_> {
        match self {
            LockedLayeredIndex::LayeredIndex(t) => t.iter_t(),
            LockedLayeredIndex::External(t) => t.iter_t(),
        }
    }
}

pub struct LayeredIndex<'a, T: AsTime, V: Deref<Target = Vec<TimeIndex<T>>> + 'a> {
    layers: LayerIds,
    view: V,
    marker: PhantomData<&'a Vec<TimeIndex<T>>>,
}

impl<'a, T: AsTime, V: Deref<Target = Vec<TimeIndex<T>>> + 'a> LayeredIndex<'a, T, V> {
    pub fn new(layers: LayerIds, view: V) -> Self {
        Self {
            layers,
            view,
            marker: PhantomData,
        }
    }

}

impl<'a, T: AsTime, V: Deref<Target = Vec<TimeIndex<T>>> + 'a> TimeIndexOps
    for LayeredIndex<'a, T, V>
{
    // type IterType<'b> = Box<dyn Iterator<Item = &'b i64> + Send + 'b> where Self: 'b;
    // type WindowType<'b> = LayeredTimeIndexWindow<'b, T> where Self: 'b;
    type IndexType = T;

    fn active(&self, w: Range<i64>) -> bool {
        self.view.iter().any(|t| t.active(w.clone()))
    }

    fn range(&self, w: Range<i64>) -> Box<dyn TimeIndexOps<IndexType = Self::IndexType> + '_> {
        let timeindex = self
            .view
            .iter()
            .enumerate()
            .filter_map(|(l, t)| self.layers.contains(&l).then(|| t.range(w.clone())))
            .collect_vec();
        Box::new(LayeredTimeIndexWindow { timeindex })
    }

    fn first(&self) -> Option<T> {
        self.view.iter().flat_map(|t| t.first()).min()
    }

    fn last(&self) -> Option<T> {
        self.view.iter().flat_map(|t| t.last()).max()
    }

    fn iter_t(&self) -> Box<dyn Iterator<Item = &i64> + Send + '_> {
        let iter = self.view.iter().map(|t| t.iter_t()).kmerge().dedup();
        Box::new(iter)
    }
}

pub trait TimeIndexOps {
    type IndexType: AsTime;

    fn active(&self, w: Range<i64>) -> bool;

    fn range<'a>(
        &'a self,
        w: Range<i64>,
    ) -> Box<dyn TimeIndexOps<IndexType = Self::IndexType> + '_>;

    fn first_t(&self) -> Option<i64> {
        self.first().map(|ti| *ti.t())
    }

    fn first(&self) -> Option<Self::IndexType>;

    fn last_t(&self) -> Option<i64> {
        self.last().map(|ti| *ti.t())
    }

    fn last(&self) -> Option<Self::IndexType>;

    fn iter_t(&self) -> Box<dyn Iterator<Item = &i64> + Send + '_>;
}

impl<T: AsTime> TimeIndexOps for TimeIndex<T> {
    type IndexType = T;

    #[inline(always)]
    fn active(&self, w: Range<i64>) -> bool {
        match &self {
            TimeIndex::Empty => false,
            TimeIndex::One(t) => w.contains(t.t()),
            TimeIndex::Set(ts) => ts.range(T::range(w)).next().is_some(),
        }
    }

    fn range(&self, w: Range<i64>) -> Box<dyn TimeIndexOps<IndexType = Self::IndexType> + '_> {
        match &self {
            TimeIndex::Empty => Box::new(TimeIndexWindow::Empty),
            TimeIndex::One(t) => {
                if w.contains(t.t()) {
                    Box::new(TimeIndexWindow::All(self))
                } else {
                    Box::new(TimeIndexWindow::Empty)
                }
            }
            TimeIndex::Set(ts) => {
                if let Some(min_val) = ts.first() {
                    if let Some(max_val) = ts.last() {
                        if min_val.t() >= &w.start && max_val.t() < &w.end {
                            Box::new(TimeIndexWindow::All(self))
                        } else {
                            Box::new(TimeIndexWindow::TimeIndexRange {
                                timeindex: self,
                                range: w,
                            })
                        }
                    } else {
                        Box::new(TimeIndexWindow::Empty)
                    }
                } else {
                    Box::new(TimeIndexWindow::Empty)
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

    fn iter_t(&self) -> Box<dyn Iterator<Item = &i64> + Send + '_> {
        match self {
            TimeIndex::Empty => Box::new(std::iter::empty()),
            TimeIndex::One(t) => Box::new(std::iter::once(t.t())),
            TimeIndex::Set(ts) => Box::new(ts.iter().map(|ti| ti.t())),
        }
    }
}

impl<'b, T: AsTime> TimeIndexOps for TimeIndexWindow<'b, T>
where
    Self: 'b,
{
    type IndexType = T;

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

    fn range(&self, w: Range<i64>) -> Box<dyn TimeIndexOps<IndexType = Self::IndexType> + '_> {
        match self {
            TimeIndexWindow::Empty => Box::new(TimeIndexWindow::Empty),
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                let start = max(range.start, w.start);
                let end = min(range.start, w.start);
                if end <= start {
                    Box::new(TimeIndexWindow::Empty)
                } else {
                    Box::new(TimeIndexWindow::TimeIndexRange {
                        timeindex,
                        range: start..end,
                    })
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

    fn iter_t(&self) -> Box<dyn Iterator<Item = &i64> + Send + '_> {
        match self {
            TimeIndexWindow::Empty => Box::new(WindowIter::Empty),
            TimeIndexWindow::TimeIndexRange { timeindex, range } => {
                Box::new(WindowIter::TimeIndexRange(Box::new(
                    timeindex.range_iter_forward(range.clone()).map(|t| t.t()),
                )))
            }
            TimeIndexWindow::All(timeindex) => Box::new(WindowIter::All(timeindex.iter_t())),
        }
    }
}

impl<'b, T: AsTime> TimeIndexOps for LayeredTimeIndexWindow<'b, T>
where
    Self: 'b,
{
    type IndexType = T;

    fn active(&self, w: Range<i64>) -> bool {
        self.timeindex.iter().any(|t| t.active(w.clone()))
    }

    fn range<'a>(
        &'a self,
        w: Range<i64>,
    ) -> Box<dyn TimeIndexOps<IndexType = Self::IndexType> + 'a> {
        let timeindex = self
            .timeindex
            .iter()
            .map(|t| t.range(w.clone()))
            .collect_vec();
        Box::new(LayeredTimeIndexWindow { timeindex })
    }

    fn first(&self) -> Option<T> {
        self.timeindex.iter().flat_map(|t| t.first()).min()
    }

    fn last(&self) -> Option<T> {
        self.timeindex.iter().flat_map(|t| t.last()).max()
    }

    fn iter_t(&self) -> Box<dyn Iterator<Item = &i64> + Send + '_> {
        Box::new(self.timeindex.iter().map(|t| t.iter_t()).kmerge())
    }
}
