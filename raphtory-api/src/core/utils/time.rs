use crate::core::storage::timeindex::{AsTime, EventTime};
#[cfg(feature = "python")]
use crate::python::error::adapt_err_value;
use chrono::{DateTime, NaiveDate, NaiveDateTime, ParseError, TimeZone};
#[cfg(feature = "python")]
use pyo3::PyErr;
use std::{convert::Infallible, num::ParseIntError};

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum ParseTimeError {
    #[error("The interval string doesn't contain a complete number of number-unit pairs.")]
    InvalidPairs,
    #[error(
        "One of the tokens in the interval string supposed to be a number couldn't be parsed."
    )]
    ParseInt {
        #[from]
        source: ParseIntError,
    },
    #[error("'{0}' is not a valid unit.")]
    InvalidUnit(String),
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error("Negative interval is not supported.")]
    NegativeInt,
    #[error("0 size step is not supported.")]
    ZeroSizeStep,
    #[error("'{0}' is not a valid datetime, valid formats are RFC3339, RFC2822, %Y-%m-%d, %Y-%m-%dT%H:%M:%S%.3f, %Y-%m-%dT%H:%M:%S%, %Y-%m-%d %H:%M:%S%.3f and %Y-%m-%d %H:%M:%S%.")]
    InvalidDateTimeString(String),
}

impl From<Infallible> for ParseTimeError {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

#[cfg(feature = "python")]
impl From<ParseTimeError> for PyErr {
    fn from(value: ParseTimeError) -> Self {
        adapt_err_value(&value)
    }
}

pub trait IntoTime {
    fn into_time(self) -> EventTime;
}

impl IntoTime for i64 {
    fn into_time(self) -> EventTime {
        EventTime::from(self)
    }
}

impl<Tz: TimeZone> IntoTime for DateTime<Tz> {
    fn into_time(self) -> EventTime {
        EventTime::from(self.timestamp_millis())
    }
}

impl IntoTime for NaiveDateTime {
    fn into_time(self) -> EventTime {
        EventTime::from(self.and_utc().timestamp_millis())
    }
}

impl IntoTime for EventTime {
    fn into_time(self) -> EventTime {
        self
    }
}

impl<T: IntoTime> IntoTime for (T, usize) {
    fn into_time(self) -> EventTime {
        self.0.into_time().set_event_id(self.1)
    }
}

pub trait TryIntoTime {
    fn try_into_time(self) -> Result<EventTime, ParseTimeError>;
}

impl<T: IntoTime> TryIntoTime for T {
    fn try_into_time(self) -> Result<EventTime, ParseTimeError> {
        Ok(self.into_time())
    }
}

impl TryIntoTime for &str {
    /// Tries to parse the timestamp as RFC3339 and then as ISO 8601 with local format and all
    /// fields mandatory except for milliseconds and allows replacing the T with a space
    fn try_into_time(self) -> Result<EventTime, ParseTimeError> {
        let rfc_result = DateTime::parse_from_rfc3339(self);
        if let Ok(datetime) = rfc_result {
            return Ok(EventTime::from(datetime.timestamp_millis()));
        }

        let result = DateTime::parse_from_rfc2822(self);
        if let Ok(datetime) = result {
            return Ok(EventTime::from(datetime.timestamp_millis()));
        }

        let result = NaiveDate::parse_from_str(self, "%Y-%m-%d");
        if let Ok(date) = result {
            let timestamp = date
                .and_hms_opt(00, 00, 00)
                .unwrap()
                .and_utc()
                .timestamp_millis();
            return Ok(EventTime::from(timestamp));
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%dT%H:%M:%S%.3f");
        if let Ok(datetime) = result {
            return Ok(EventTime::from(datetime.and_utc().timestamp_millis()));
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%dT%H:%M:%S%");
        if let Ok(datetime) = result {
            return Ok(EventTime::from(datetime.and_utc().timestamp_millis()));
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%d %H:%M:%S%.3f");
        if let Ok(datetime) = result {
            return Ok(EventTime::from(datetime.and_utc().timestamp_millis()));
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%d %H:%M:%S%");
        if let Ok(datetime) = result {
            return Ok(EventTime::from(datetime.and_utc().timestamp_millis()));
        }

        Err(ParseTimeError::InvalidDateTimeString(self.to_string()))
    }
}

pub trait TryIntoTimeNeedsEventId: TryIntoTime {}

impl TryIntoTimeNeedsEventId for i64 {}

impl<Tz: TimeZone> TryIntoTimeNeedsEventId for DateTime<Tz> {}

impl TryIntoTimeNeedsEventId for NaiveDateTime {}

impl TryIntoTimeNeedsEventId for &str {}

/// Used to handle automatic injection of event id if not explicitly provided.
/// In many cases, we will want different behaviour if an event id was provided or not.
pub enum InputTime {
    Simple(i64),
    Indexed(i64, usize),
}

impl InputTime {
    pub fn set_index(self, index: usize) -> Self {
        match self {
            InputTime::Simple(time) => InputTime::Indexed(time, index),
            InputTime::Indexed(time, _) => InputTime::Indexed(time, index),
        }
    }

    pub fn as_time(&self) -> EventTime {
        match self {
            InputTime::Simple(t) => EventTime::new(*t, 0),
            InputTime::Indexed(t, s) => EventTime::new(*t, *s),
        }
    }
}

/// Single time input only refers to the i64 component of an EventTime (no event id).
pub trait AsSingleTimeInput {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError>;
}

impl<T: TryIntoTimeNeedsEventId> AsSingleTimeInput for T {
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

impl<T: AsSingleTimeInput> TryIntoInputTime for T {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        self.try_into_input_time()
    }
}

impl TryIntoInputTime for EventTime {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        Ok(InputTime::Indexed(self.t(), self.i()))
    }
}

impl<T: AsSingleTimeInput> TryIntoInputTime for (T, usize) {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        Ok(self.0.try_into_input_time()?.set_index(self.1))
    }
}

pub trait IntoTimeWithFormat {
    fn parse_time(&self, fmt: &str) -> Result<i64, ParseTimeError>;
}

impl IntoTimeWithFormat for &str {
    fn parse_time(&self, fmt: &str) -> Result<i64, ParseTimeError> {
        let timestamp = NaiveDateTime::parse_from_str(self, fmt)?
            .and_utc()
            .timestamp_millis();
        Ok(timestamp)
    }
}
