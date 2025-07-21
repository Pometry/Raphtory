use crate::{core::utils::time::TryIntoTime, python::timeindex::PyTime};
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use itertools::Itertools;
use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError},
    prelude::*,
    types::PyDateTime,
};
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
pub struct TimeIndexEntry(pub i64, pub usize);

impl PartialEq<i64> for TimeIndexEntry {
    fn eq(&self, other: &i64) -> bool {
        self.0 == *other
    }
}

impl fmt::Display for TimeIndexEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TimeIndexEntry[{}, {}]", self.0, self.1)
    }
}

fn parse_email_timestamp(timestamp: &str) -> PyResult<TimeIndexEntry> {
    Python::with_gil(|py| {
        let email_utils = PyModule::import(py, "email.utils")?;
        let datetime = email_utils.call_method1("parsedate_to_datetime", (timestamp,))?;
        let py_seconds = datetime.call_method1("timestamp", ())?;
        let seconds = py_seconds.extract::<f64>()?;
        Ok(TimeIndexEntry::from(seconds as i64 * 1000))
    })
}

impl<'source> FromPyObject<'source> for TimeIndexEntry {
    fn extract_bound(time: &Bound<'source, PyAny>) -> PyResult<Self> {
        if let Ok(string) = time.extract::<String>() {
            let timestamp = string.as_str();
            let parsing_result = timestamp
                .try_into_time()
                .or_else(|e| parse_email_timestamp(timestamp).map_err(|_| e))?;
            return Ok(parsing_result);
        }
        if let Ok(number) = time.extract::<i64>() {
            return Ok(TimeIndexEntry::from(number));
        }
        if let Ok(float_time) = time.extract::<f64>() {
            // seconds since Unix epoch as returned by python `timestamp`
            let float_ms = float_time * 1000.0;
            let float_ms_trunc = float_ms.round();
            let rel_err = (float_ms - float_ms_trunc).abs() / (float_ms.abs() + f64::EPSILON);
            if rel_err > 4.0 * f64::EPSILON {
                return Err(PyRuntimeError::new_err(
                    "Float timestamps with more than millisecond precision are not supported.",
                ));
            }
            return Ok(TimeIndexEntry::from(float_ms_trunc as i64));
        }
        if let Ok(parsed_datetime) = time.extract::<DateTime<FixedOffset>>() {
            return Ok(TimeIndexEntry::from(parsed_datetime.timestamp_millis()));
        }
        if let Ok(parsed_datetime) = time.extract::<NaiveDateTime>() {
            // Important, this is needed to ensure that naive DateTime objects are treated as UTC and not local time
            return Ok(TimeIndexEntry::from(
                parsed_datetime.and_utc().timestamp_millis(),
            ));
        }
        if let Ok(py_datetime) = time.downcast::<PyDateTime>() {
            let time = (py_datetime.call_method0("timestamp")?.extract::<f64>()? * 1000.0) as i64;
            return Ok(TimeIndexEntry::from(time));
        }
        if let Ok(py_time) = time.downcast::<PyTime>() {
            return Ok(py_time.get().inner());
        }
        let message = format!("time '{time}' must be a str, datetime, float, or an integer");
        Err(PyTypeError::new_err(message))
    }
}

pub trait AsTime: fmt::Debug + Copy + Ord + Eq + Send + Sync + 'static {
    fn t(&self) -> i64;

    /// Converts the timestamp into a UTC DateTime. Returns TimestampError on out-of-range timestamps.
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
        self.active(Self::IndexType::range(w))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType;

    fn range_t(&self, w: Range<i64>) -> Self::RangeType {
        self.range(Self::IndexType::range(w))
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

    pub fn saturating_add(&self, i: i64) -> Self {
        Self(self.0.saturating_add(i), self.1)
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
