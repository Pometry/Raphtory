use super::locked_view::LockedView;
use crate::{
    core::{entities::LayerIds, utils::time::error::ParseTimeError},
    db::api::mutation::{internal::InternalAdditionOps, InputTime, TryIntoInputTime},
};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::{Itertools, KMerge};
use num_traits::Saturating;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    collections::BTreeSet,
    fmt::Debug,
    iter,
    marker::PhantomData,
    ops::{Deref, Range},
    sync::Arc,
};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Ord, PartialOrd, Eq)]
pub struct TimeIndexEntry(pub i64, pub usize);

pub trait AsTime: Debug + Copy + Ord + Eq + Send + Sync + 'static {
    fn t(&self) -> i64;

    fn dt(&self) -> Option<DateTime<Utc>> {
        let t = self.t();
        NaiveDateTime::from_timestamp_millis(t).map(|dt| dt.and_utc())
    }

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
    fn t(&self) -> i64 {
        *self
    }

    fn range(w: Range<i64>) -> Range<Self> {
        w
    }
}

impl AsTime for TimeIndexEntry {
    fn t(&self) -> i64 {
        self.0
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
        w: Range<i64>,
    ) -> Box<dyn DoubleEndedIterator<Item = T> + Send + '_> {
        match self {
            TimeIndex::Empty => Box::new(iter::empty()),
            TimeIndex::One(t) => {
                if w.contains(&t.t()) {
                    Box::new(iter::once(*t))
                } else {
                    Box::new(iter::empty())
                }
            }
            TimeIndex::Set(ts) => Box::new(ts.range(T::range(w)).copied()),
        }
    }

    // = note: see issue #65991 <https://github.com/rust-lang/rust/issues/65991> for more information
    // = note: required when coercing `Box<dyn DoubleEndedIterator<Item = &i64> + Send>` into `Box<dyn Iterator<Item = &i64> + Send>`
    pub(crate) fn range_iter_forward(
        &self,
        w: Range<i64>,
    ) -> Box<dyn Iterator<Item = T> + Send + '_> {
        Box::new(self.range_iter(w))
    }

    pub(crate) fn range_inner(&self, w: Range<i64>) -> TimeIndexWindow<T> {
        match &self {
            TimeIndex::Empty => TimeIndexWindow::Empty,
            TimeIndex::One(t) => {
                if w.contains(&t.t()) {
                    TimeIndexWindow::All(self)
                } else {
                    TimeIndexWindow::Empty
                }
            }
            TimeIndex::Set(ts) => {
                if let Some(min_val) = ts.first() {
                    if let Some(max_val) = ts.last() {
                        if min_val.t() >= w.start && max_val.t() < w.end {
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
        range: Range<i64>,
    },
    All(&'a TimeIndex<T>),
}

impl<'a, T: AsTime> TimeIndexIntoOps for TimeIndexWindow<'a, T> {
    type IndexType = T;
    type RangeType = Self;

    fn into_range(self, w: Range<i64>) -> Self {
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

pub type LockedLayeredIndex<'a, T: AsTime> =
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

    = LayeredTimeIndexWindow<'b, Ops::RangeType<'b>>    where
        Self: 'b;

    fn active(&self, w: Range<i64>) -> bool {
        self.view.as_ref().iter().any(|t| t.active(w.clone()))
    }

    fn range<'b>(&'b self, w: Range<i64>) -> Self::RangeType<'b> {
        let timeindex = self
            .view
            .as_ref()
            .iter()
            .enumerate()
            .filter_map(|(l, t)| self.layers.contains(&l).then(|| t.range(w.clone())))
            .collect_vec();
        LayeredTimeIndexWindow::new(timeindex)
    }

    fn first(&self) -> Option<T> {
        self.view.as_ref().iter().flat_map(|t| t.first()).min()
    }

    fn last(&self) -> Option<T> {
        self.view.as_ref().iter().flat_map(|t| t.last()).max()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = T> + Send + '_> {
        Box::new(self.view.as_ref().iter().map(|t| t.iter()).kmerge().dedup())
    }
}

pub trait TimeIndexOps: Send + Sync {
    type IndexType: AsTime;
    type RangeType<'a>: TimeIndexOps<IndexType = Self::IndexType> + 'a
    where
        Self: 'a;

    fn active(&self, w: Range<i64>) -> bool;

    fn range<'a>(&'a self, w: Range<i64>) -> Self::RangeType<'a>;

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
}

pub trait TimeIndexIntoOps: Sized {
    type IndexType: AsTime;
    type RangeType: TimeIndexIntoOps<IndexType = Self::IndexType>;

    fn into_range(self, w: Range<i64>) -> Self::RangeType;

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send;

    fn into_iter_t(self) -> impl Iterator<Item = i64> + Send {
        self.into_iter().map(|time| time.t())
    }
}

impl<T: AsTime> TimeIndexOps for TimeIndex<T> {
    type IndexType = T;
    type RangeType<'a> = TimeIndexWindow<'a, T> where Self: 'a,;

    #[inline(always)]
    fn active(&self, w: Range<i64>) -> bool {
        match &self {
            TimeIndex::Empty => false,
            TimeIndex::One(t) => w.contains(&t.t()),
            TimeIndex::Set(ts) => ts.range(T::range(w)).next().is_some(),
        }
    }

    fn range(&self, w: Range<i64>) -> TimeIndexWindow<T> {
        match &self {
            TimeIndex::Empty => TimeIndexWindow::Empty,
            TimeIndex::One(t) => {
                if w.contains(&t.t()) {
                    TimeIndexWindow::All(self)
                } else {
                    TimeIndexWindow::Empty
                }
            }
            TimeIndex::Set(ts) => {
                if let Some(min_val) = ts.first() {
                    if let Some(max_val) = ts.last() {
                        if min_val.t() >= w.start && max_val.t() < w.end {
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
}

impl<'b, T: AsTime> TimeIndexOps for TimeIndexWindow<'b, T>
where
    Self: 'b,
{
    type IndexType = T;
    type RangeType<'a> = TimeIndexWindow<'a, T> where Self: 'a;

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

    fn range<'a>(&'a self, w: Range<i64>) -> Self::RangeType<'a> {
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
}

impl<'a, Ops: TimeIndexOps + 'a> TimeIndexOps for LayeredTimeIndexWindow<'a, Ops> {
    type IndexType = Ops::IndexType;
    type RangeType<'b> = LayeredTimeIndexWindow<'b, Ops::RangeType<'b>> where Self: 'b;

    fn active(&self, w: Range<i64>) -> bool {
        self.timeindex.iter().any(|t| t.active(w.clone()))
    }

    fn range<'b>(&'b self, w: Range<i64>) -> Self::RangeType<'b> {
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
}

#[cfg(test)]
#[cfg(feature = "arrow")]
mod test {
    use std::collections::BTreeSet;

    use arrow2::{
        array::{PrimitiveArray, StructArray},
        datatypes::{DataType, Field},
    };
    use itertools::Itertools;

    use crate::{
        arrow::chunked_array::chunked_array::NonNull,
        core::{entities::LayerIds, storage::locked_view::LockedView},
    };

    use crate::{
        arrow::{
            chunked_array::{
                array_ops::{
                    BaseArrayOps, IntoNonNullPrimitiveCol, NonNullPrimitiveCol, PrimitiveCol,
                },
                chunked_array::ChunkedArray,
                ChunkedArraySlice,
            },
            timestamps::TimeStamps,
            Time,
        },
        db::api::view::internal::EdgeUpdates,
    };

    use super::{LayeredIndex, LockedLayeredIndex, TimeIndex, TimeIndexEntry, TimeIndexOps};

    #[test]
    fn time_index_1() {
        check_time_index(vec![3], |vec_t_index, arr_t_index| {
            assert_eq!(vec_t_index.active(0..5), arr_t_index.active(0..5));
            assert_eq!(vec_t_index.active(0..3), arr_t_index.active(0..3));
            assert_eq!(vec_t_index.active(0..2), arr_t_index.active(0..2));
            assert_eq!(vec_t_index.active(3..100), arr_t_index.active(3..100));

            assert_eq!(vec_t_index.first(), arr_t_index.first());
            assert_eq!(vec_t_index.last(), arr_t_index.last());

            assert_eq!(
                vec_t_index.iter_t().collect_vec(),
                arr_t_index.iter_t().collect_vec()
            )
        });
    }

    #[test]
    fn time_index() {
        check_time_index(vec![7, 9, 12, 34], |vec_t_index, arr_t_index| {
            assert_eq!(vec_t_index.active(0..5), arr_t_index.active(0..5));
            assert_eq!(vec_t_index.active(0..7), arr_t_index.active(0..7));
            assert_eq!(vec_t_index.active(7..13), arr_t_index.active(7..13));
            assert_eq!(vec_t_index.active(10..100), arr_t_index.active(10..100));
            assert_eq!(vec_t_index.active(35..200), arr_t_index.active(35..200));

            assert_eq!(vec_t_index.first(), arr_t_index.first());
            assert_eq!(vec_t_index.last(), arr_t_index.last());

            assert_eq!(
                vec_t_index.iter_t().collect_vec(),
                arr_t_index.iter_t().collect_vec()
            );
        })
    }

    #[test]
    fn time_index_range() {
        check_time_index(vec![7, 9, 12, 34], |vec_t_index, arr_t_index| {
            let vec_window = vec_t_index.range(0..5);
            let arr_window = arr_t_index.range(0..5);

            assert_eq!(vec_window.active(0..5), arr_window.active(0..5));
            assert_eq!(vec_window.active(0..7), arr_window.active(0..7));

            assert_eq!(
                vec_window.iter_t().collect_vec(),
                arr_window.iter_t().collect_vec()
            );

            let vec_t_index = vec_t_index.range(0..13);
            let arr_t_index = arr_t_index.range(0..13);

            assert_eq!(vec_t_index.active(0..5), arr_t_index.active(0..5));
            assert_eq!(vec_t_index.active(0..7), arr_t_index.active(0..7));
            assert_eq!(vec_t_index.active(7..13), arr_t_index.active(7..13));
            assert_eq!(vec_t_index.active(10..100), arr_t_index.active(10..100));
            assert_eq!(vec_t_index.active(35..200), arr_t_index.active(35..200));

            assert_eq!(vec_t_index.first(), arr_t_index.first());
            assert_eq!(vec_t_index.last(), arr_t_index.last());

            assert_eq!(
                vec_t_index.iter_t().collect_vec(),
                arr_t_index.iter_t().collect_vec()
            );
        })
    }

    fn check_time_index(times: Vec<i64>, f: impl Fn(&EdgeUpdates, &EdgeUpdates)) {
        let t_index_vec: BTreeSet<_> = times.iter().map(|t| TimeIndexEntry::new(*t, 0)).collect();

        let locked_t_index =
            parking_lot::RwLock::new(vec![TimeIndex::One(TimeIndexEntry::new(3, 0))]);

        let vec_t_index = EdgeUpdates::Mem(LockedLayeredIndex::new(
            LayerIds::One(0),
            LockedView::Locked(locked_t_index.read()),
        ));

        let time_chunks = make_chunks(vec![3], 1);
        let ts: TimeStamps<'_, TimeIndexEntry> = TimeStamps::new(time_chunks.slice(0..), None);
        let arr_t_index = EdgeUpdates::Col(ts);

        f(&vec_t_index, &arr_t_index);
    }

    fn make_chunks(
        data: Vec<i64>,
        chunk_size: usize,
    ) -> ChunkedArray<PrimitiveArray<i64>, NonNull> {
        let data = data
            .into_iter()
            .chunks(chunk_size)
            .into_iter()
            .map(|c| c.collect_vec())
            .collect_vec();

        ChunkedArray::from_non_nulls(
            data.into_iter()
                .map(|c| PrimitiveArray::from_vec(c))
                .collect::<Vec<_>>(),
            chunk_size,
        )
    }
}
