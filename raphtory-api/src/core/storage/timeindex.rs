use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{fmt, ops::Range};

/// Error type for timestamp to chrono::DateTime<Utc> conversion operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeError {
    /// The timestamp value is out of range for chrono::DateTime<Utc> conversion
    OutOfRange(i64),
}

impl fmt::Display for TimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let min = DateTime::<Utc>::MIN_UTC.timestamp_millis();
        let max = DateTime::<Utc>::MAX_UTC.timestamp_millis();
        match self {
            TimeError::OutOfRange(timestamp) => {
                write!(f, "Timestamp '{}' is out of range for DateTime conversion. Valid range is from {} to {}", timestamp, min, max)
            }
        }
    }
}

impl std::error::Error for TimeError {}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct EventTime(pub i64, pub usize);

impl PartialEq<i64> for EventTime {
    fn eq(&self, other: &i64) -> bool {
        self.0 == *other
    }
}

impl fmt::Display for EventTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EventTime[{}, {}]", self.0, self.1)
    }
}

pub trait AsTime: fmt::Debug + Copy + Ord + Eq + Send + Sync + 'static {
    fn t(&self) -> i64;

    /// Tries to convert the timestamp into a UTC DateTime.
    fn dt(&self) -> Result<DateTime<Utc>, TimeError> {
        let t = self.t();
        DateTime::from_timestamp_millis(t).ok_or(TimeError::OutOfRange(t))
    }

    fn range(w: Range<i64>) -> Range<Self>;

    fn i(&self) -> usize {
        0
    }

    fn new(t: i64, s: usize) -> Self;
}

pub trait TimeIndexLike<'a>: TimeIndexOps<'a> {
    fn range_iter(
        self,
        w: Range<Self::IndexType>,
    ) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a;

    fn range_iter_rev(
        self,
        w: Range<Self::IndexType>,
    ) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a;

    fn range_count(&self, w: Range<Self::IndexType>) -> usize;

    fn first_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        self.clone().range_iter(w).next()
    }

    fn last_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        self.clone().range_iter_rev(w).next()
    }
}

pub trait TimeIndexOps<'a>: Sized + Clone + Send + Sync + 'a {
    type IndexType: AsTime;
    type RangeType: TimeIndexOps<'a, IndexType = Self::IndexType> + 'a;

    fn active(&self, w: Range<Self::IndexType>) -> bool;

    #[inline]
    fn active_t(&self, w: Range<i64>) -> bool {
        self.active(<Self::IndexType as AsTime>::range(w))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType;

    fn range_t(&self, w: Range<i64>) -> Self::RangeType {
        self.range(<Self::IndexType as AsTime>::range(w))
    }

    fn first_t(&self) -> Option<i64> {
        self.first().map(|ti| ti.t())
    }

    fn first(&self) -> Option<Self::IndexType> {
        self.clone().iter().next()
    }

    fn last_t(&self) -> Option<i64> {
        self.last().map(|ti| ti.t())
    }

    fn last(&self) -> Option<Self::IndexType> {
        self.clone().iter_rev().next()
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a;

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a;

    fn iter_t(self) -> impl Iterator<Item = i64> + Send + Sync + 'a {
        self.iter().map(|time| time.t())
    }

    fn iter_rev_t(self) -> impl Iterator<Item = i64> + Send + Sync + 'a {
        self.iter_rev().map(|time| time.t())
    }

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.clone().iter().next().is_none()
    }

    fn merge<R: TimeIndexOps<'a, IndexType = Self::IndexType>>(
        self,
        other: R,
    ) -> MergedTimeIndex<Self, R> {
        MergedTimeIndex(self, other)
    }
}

impl<'a, T: TimeIndexOps<'a> + Clone> TimeIndexOps<'a> for &'a T {
    type IndexType = T::IndexType;
    type RangeType = T::RangeType;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        T::active(*self, w)
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        T::range(*self, w)
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        T::iter(self.clone())
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        T::iter_rev(self.clone())
    }

    fn len(&self) -> usize {
        T::len(*self)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct MergedTimeIndex<L, R>(pub L, pub R);

impl<'a, L: TimeIndexOps<'a>, R: TimeIndexOps<'a, IndexType = L::IndexType>> TimeIndexOps<'a>
    for MergedTimeIndex<L, R>
{
    type IndexType = L::IndexType;
    type RangeType = MergedTimeIndex<L::RangeType, R::RangeType>;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.0.active(w.clone()) || self.1.active(w.clone())
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        MergedTimeIndex(self.0.range(w.clone()), self.1.range(w.clone()))
    }

    fn first(&self) -> Option<Self::IndexType> {
        self.0.first().into_iter().chain(self.1.first()).min()
    }

    fn last(&self) -> Option<Self::IndexType> {
        self.0.last().into_iter().chain(self.1.last()).max()
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.0.iter().merge(self.1.iter())
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.0.iter_rev().merge_by(self.1.iter_rev(), |l, r| l >= r)
    }

    fn len(&self) -> usize {
        self.0.len() + self.1.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty() && self.1.is_empty()
    }
}

impl From<i64> for EventTime {
    fn from(value: i64) -> Self {
        Self::start(value)
    }
}

impl EventTime {
    pub const MIN: EventTime = EventTime(i64::MIN, 0);

    pub const MAX: EventTime = EventTime(i64::MAX, usize::MAX);
    pub fn new(t: i64, s: usize) -> Self {
        Self(t, s)
    }

    /// Sets the event id of the EventTime.
    /// Note that this mutates the EventTime in place rather than create and return a new one.
    pub fn set_event_id(mut self, i: usize) -> Self {
        self.1 = i;
        self
    }

    pub fn as_tuple(&self) -> (i64, usize) {
        (self.0, self.1)
    }

    pub fn start(t: i64) -> Self {
        Self(t, 0)
    }

    pub fn next(&self) -> Self {
        if self.1 < usize::MAX {
            Self(self.0, self.1 + 1)
        } else if self.0 < i64::MAX {
            Self(self.0 + 1, 0)
        } else {
            *self
        }
    }

    pub fn previous(&self) -> Self {
        if self.1 > 0 {
            Self(self.0, self.1 - 1)
        } else if self.0 > i64::MIN {
            Self(self.0 - 1, 0)
        } else {
            *self
        }
    }

    pub fn end(t: i64) -> Self {
        Self(t, usize::MAX)
    }
}

impl AsTime for i64 {
    fn t(&self) -> i64 {
        *self
    }

    fn range(w: Range<i64>) -> Range<Self> {
        w
    }

    fn new(t: i64, _s: usize) -> Self {
        t
    }
}

impl AsTime for EventTime {
    fn t(&self) -> i64 {
        self.0
    }
    fn range(w: Range<i64>) -> Range<Self> {
        Self::start(w.start)..Self::start(w.end)
    }

    fn i(&self) -> usize {
        self.1
    }

    fn new(t: i64, s: usize) -> Self {
        Self(t, s)
    }
}
