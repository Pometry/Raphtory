use std::{fmt, ops::Range};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct TimeIndexEntry(pub i64, pub usize);

pub trait AsTime: fmt::Debug + Copy + Ord + Eq + Send + Sync + 'static {
    fn t(&self) -> i64;

    fn dt(&self) -> Option<DateTime<Utc>> {
        let t = self.t();
        DateTime::from_timestamp_millis(t)
    }

    fn range(w: Range<i64>) -> Range<Self>;

    fn i(&self) -> usize {
        0
    }

    fn new(t: i64, s: usize) -> Self;
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

impl AsTime for TimeIndexEntry {
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
