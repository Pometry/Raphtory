use crate::{
    core::storage::timeindex::{AsTime, TimeIndexEntry},
    python::error::adapt_err_value,
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, ParseError, TimeZone};
use pyo3::PyErr;
use std::{convert::Infallible, num::ParseIntError};

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum ParseTimeError {
    #[error("the interval string doesn't contain a complete number of number-unit pairs")]
    InvalidPairs,
    #[error("one of the tokens in the interval string supposed to be a number couldn't be parsed")]
    ParseInt {
        #[from]
        source: ParseIntError,
    },
    #[error("'{0}' is not a valid unit")]
    InvalidUnit(String),
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error("negative interval is not supported")]
    NegativeInt,
    #[error("0 size step is not supported")]
    ZeroSizeStep,
    #[error("'{0}' is not a valid datetime, valid formats are RFC3339, RFC2822, %Y-%m-%d, %Y-%m-%dT%H:%M:%S%.3f, %Y-%m-%dT%H:%M:%S%, %Y-%m-%d %H:%M:%S%.3f and %Y-%m-%d %H:%M:%S%")]
    InvalidDateTimeString(String),
}

impl From<Infallible> for ParseTimeError {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

impl From<ParseTimeError> for PyErr {
    fn from(value: ParseTimeError) -> Self {
        adapt_err_value(&value)
    }
}

pub trait IntoTime {
    fn into_time(self) -> TimeIndexEntry;
}

impl IntoTime for i64 {
    fn into_time(self) -> TimeIndexEntry {
        TimeIndexEntry::from(self)
    }
}

impl<Tz: TimeZone> IntoTime for DateTime<Tz> {
    fn into_time(self) -> TimeIndexEntry {
        TimeIndexEntry::from(self.timestamp_millis())
    }
}

impl IntoTime for NaiveDateTime {
    fn into_time(self) -> TimeIndexEntry {
        TimeIndexEntry::from(self.and_utc().timestamp_millis())
    }
}

impl IntoTime for TimeIndexEntry {
    fn into_time(self) -> TimeIndexEntry {
        self
    }
}

pub trait TryIntoTime {
    fn try_into_time(self) -> Result<TimeIndexEntry, ParseTimeError>;
}

impl<T: IntoTime> TryIntoTime for T {
    fn try_into_time(self) -> Result<TimeIndexEntry, ParseTimeError> {
        Ok(TimeIndexEntry::from(self.into_time()))
    }
}

impl TryIntoTime for &str {
    /// Tries to parse the timestamp as RFC3339 and then as ISO 8601 with local format and all
    /// fields mandatory except for milliseconds and allows replacing the T with a space
    fn try_into_time(self) -> Result<TimeIndexEntry, ParseTimeError> {
        let rfc_result = DateTime::parse_from_rfc3339(self);
        if let Ok(datetime) = rfc_result {
            return Ok(TimeIndexEntry::from(datetime.timestamp_millis()));
        }

        let result = DateTime::parse_from_rfc2822(self);
        if let Ok(datetime) = result {
            return Ok(TimeIndexEntry::from(datetime.timestamp_millis()));
        }

        let result = NaiveDate::parse_from_str(self, "%Y-%m-%d");
        if let Ok(date) = result {
            let timestamp = date
                .and_hms_opt(00, 00, 00)
                .unwrap()
                .and_utc()
                .timestamp_millis();
            return Ok(TimeIndexEntry::from(timestamp));
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%dT%H:%M:%S%.3f");
        if let Ok(datetime) = result {
            return Ok(TimeIndexEntry::from(datetime.and_utc().timestamp_millis()));
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%dT%H:%M:%S%");
        if let Ok(datetime) = result {
            return Ok(TimeIndexEntry::from(datetime.and_utc().timestamp_millis()));
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%d %H:%M:%S%.3f");
        if let Ok(datetime) = result {
            return Ok(TimeIndexEntry::from(datetime.and_utc().timestamp_millis()));
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%d %H:%M:%S%");
        if let Ok(datetime) = result {
            return Ok(TimeIndexEntry::from(datetime.and_utc().timestamp_millis()));
        }

        Err(ParseTimeError::InvalidDateTimeString(self.to_string()))
    }
}

pub trait TryIntoTimeNeedsSecondaryIndex: TryIntoTime {}

impl TryIntoTimeNeedsSecondaryIndex for i64 {}

impl<Tz: TimeZone> TryIntoTimeNeedsSecondaryIndex for DateTime<Tz> {}

impl TryIntoTimeNeedsSecondaryIndex for NaiveDateTime {}

impl TryIntoTimeNeedsSecondaryIndex for &str {}

/// Used to handle automatic injection of secondary index if not explicitly provided
pub enum InputTime {
    Simple(i64),
    Indexed(i64, usize),
}

pub trait AsTimeInput {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError>;
}

impl AsTimeInput for TimeIndexEntry {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        Ok(InputTime::Indexed(self.t(), self.i()))
    }
}

impl<T: TryIntoTimeNeedsSecondaryIndex> AsTimeInput for T {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        Ok(InputTime::Simple(self.try_into_time()?.t()))
    }
}

pub trait TryIntoInputTime {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError>;
}

impl TryIntoInputTime for InputTime {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        Ok(self)
    }
}

impl<T: AsTimeInput> TryIntoInputTime for T {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        self.try_into_input_time()
    }
}

impl<T: TryIntoTimeNeedsSecondaryIndex> TryIntoInputTime for (T, usize) {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        Ok(InputTime::Indexed(self.0.try_into_time()?.t(), self.1))
    }
}

pub trait IntoTimeWithFormat {
    fn parse_time(&self, fmt: &str) -> Result<TimeIndexEntry, ParseTimeError>;
}

impl IntoTimeWithFormat for &str {
    fn parse_time(&self, fmt: &str) -> Result<TimeIndexEntry, ParseTimeError> {
        let timestamp = NaiveDateTime::parse_from_str(self, fmt)?
            .and_utc()
            .timestamp_millis();
        Ok(TimeIndexEntry::from(timestamp))
    }
}
