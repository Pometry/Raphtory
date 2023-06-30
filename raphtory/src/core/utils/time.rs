use crate::core::utils::time::error::*;
use chrono::NaiveDate;
use chrono::{DateTime, Duration, Months, NaiveDateTime, TimeZone};
use itertools::{Either, Itertools};
use regex::Regex;
use std::ops::{Add, Sub};

pub mod error {
    use chrono::ParseError;
    use std::num::ParseIntError;

    #[derive(thiserror::Error, Debug, Clone, PartialEq)]
    pub enum ParseTimeError {
        #[error("the interval string doesn't contain a complete number of number-unit pairs")]
        InvalidPairs,
        #[error(
            "one of the tokens in the interval string supposed to be a number couldn't be parsed"
        )]
        ParseInt {
            #[from]
            source: ParseIntError,
        },
        #[error("'{0}' is not a valid unit")]
        InvalidUnit(String),
        #[error(transparent)]
        ParseError(#[from] ParseError),
    }
}

pub trait IntoTime {
    fn into_time(self) -> i64;
}

impl IntoTime for i64 {
    fn into_time(self) -> i64 {
        self
    }
}

impl<Tz: TimeZone> IntoTime for DateTime<Tz> {
    fn into_time(self) -> i64 {
        self.timestamp_millis()
    }
}

impl IntoTime for NaiveDateTime {
    fn into_time(self) -> i64 {
        self.timestamp_millis()
    }
}

pub trait TryIntoTime {
    fn try_into_time(self) -> Result<i64, ParseTimeError>;
}

impl<T: IntoTime> TryIntoTime for T {
    fn try_into_time(self) -> Result<i64, ParseTimeError> {
        Ok(self.into_time())
    }
}

impl TryIntoTime for &str {
    /// Tries to parse the timestamp as RFC3339 and then as ISO 8601 with local format and all
    /// fields mandatory except for milliseconds and allows replacing the T with a space
    fn try_into_time(self) -> Result<i64, ParseTimeError> {
        let rfc_result = DateTime::parse_from_rfc3339(self);
        if let Ok(datetime) = rfc_result {
            return Ok(datetime.timestamp_millis());
        }

        let result = DateTime::parse_from_rfc2822(self);
        if let Ok(datetime) = result {
            return Ok(datetime.timestamp_millis());
        }

        let result = NaiveDate::parse_from_str(self, "%Y-%m-%d");
        if let Ok(date) = result {
            return Ok(date.and_hms_opt(00, 00, 00).unwrap().timestamp_millis());
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%dT%H:%M:%S%.3f");
        if let Ok(datetime) = result {
            return Ok(datetime.timestamp_millis());
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%dT%H:%M:%S%");
        if let Ok(datetime) = result {
            return Ok(datetime.timestamp_millis());
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%d %H:%M:%S%.3f");
        if let Ok(datetime) = result {
            return Ok(datetime.timestamp_millis());
        }

        let result = NaiveDateTime::parse_from_str(self, "%Y-%m-%d %H:%M:%S%");
        if let Ok(datetime) = result {
            return Ok(datetime.timestamp_millis());
        }

        Err(rfc_result.unwrap_err().into())
    }
}

pub(crate) trait IntoTimeWithFormat {
    fn parse_time(&self, fmt: &str) -> Result<i64, ParseTimeError>;
}

impl IntoTimeWithFormat for &str {
    fn parse_time(&self, fmt: &str) -> Result<i64, ParseTimeError> {
        Ok(NaiveDateTime::parse_from_str(self, fmt)?.timestamp_millis())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum IntervalSize {
    Discrete(u64),
    Temporal { millis: u64, months: u32 },
}

impl IntervalSize {
    fn months(months: i64) -> Self {
        Self::Temporal {
            millis: 0,
            months: months as u32,
        }
    }

    fn add_temporal(&self, other: IntervalSize) -> IntervalSize {
        match (self, other) {
            (
                Self::Temporal {
                    millis: ml1,
                    months: mt1,
                },
                Self::Temporal {
                    millis: ml2,
                    months: mt2,
                },
            ) => Self::Temporal {
                millis: ml1 + ml2,
                months: mt1 + mt2,
            },
            _ => panic!("this function is not supposed to be used with discrete intervals"),
        }
    }
}

impl From<Duration> for IntervalSize {
    fn from(value: Duration) -> Self {
        Self::Temporal {
            millis: value.num_milliseconds() as u64,
            months: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Interval {
    pub(crate) epoch_alignment: bool,
    pub(crate) size: IntervalSize,
}

impl Default for Interval {
    fn default() -> Self {
        Self {
            epoch_alignment: false,
            size: IntervalSize::Discrete(1),
        }
    }
}

impl TryFrom<&str> for Interval {
    type Error = ParseTimeError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let trimmed = value.trim();
        let no_and = trimmed.replace("and", "");
        let cleaned = {
            let re = Regex::new(r"[\s&,]+").unwrap();
            re.replace_all(&no_and, " ")
        };

        let tokens = cleaned.split(" ").collect_vec();

        if tokens.len() < 2 || tokens.len() % 2 != 0 {
            return Err(ParseTimeError::InvalidPairs);
        }

        let (intervals, errors): (Vec<IntervalSize>, Vec<ParseTimeError>) = tokens
            .chunks(2)
            .map(|chunk| Self::parse_duration(chunk[0], chunk[1]))
            .partition_map(|d| match d {
                Ok(d) => Either::Left(d),
                Err(e) => Either::Right(e),
            });

        if errors.is_empty() {
            Ok(Self {
                epoch_alignment: true,
                size: intervals
                    .into_iter()
                    .reduce(|a, b| a.add_temporal(b))
                    .unwrap(),
            })
        } else {
            Err(errors.get(0).unwrap().clone())
        }
    }
}

impl TryFrom<u64> for Interval {
    type Error = ParseTimeError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch_alignment: false,
            size: IntervalSize::Discrete(value),
        })
    }
}

impl Interval {
    /// Return an option because there might be no exact translation to millis for some intervals
    pub fn to_millis(&self) -> Option<u64> {
        match self.size {
            IntervalSize::Discrete(millis) => Some(millis),
            IntervalSize::Temporal { millis, months } => (months == 0).then_some(millis),
        }
    }

    fn parse_duration(number: &str, unit: &str) -> Result<IntervalSize, ParseTimeError> {
        let number: i64 = number.parse::<u64>()? as i64;
        let duration = match unit {
            "year" | "years" => IntervalSize::months(number * 12),
            "month" | "months" => IntervalSize::months(number),
            "week" | "weeks" => Duration::weeks(number).into(),
            "day" | "days" => Duration::days(number).into(),
            "hour" | "hours" => Duration::hours(number).into(),
            "minute" | "minutes" => Duration::minutes(number).into(),
            "second" | "seconds" => Duration::seconds(number).into(),
            "millisecond" | "milliseconds" => Duration::milliseconds(number).into(),
            unit => return Err(ParseTimeError::InvalidUnit(unit.to_string())),
        };
        Ok(duration)
    }
}

impl Sub<Interval> for i64 {
    type Output = i64;
    fn sub(self, rhs: Interval) -> Self::Output {
        match rhs.size {
            IntervalSize::Discrete(number) => self - (number as i64),
            IntervalSize::Temporal { millis, months } => {
                // first we subtract the number of milliseconds and then the number of months for
                // consistency with the implementation of Add (we revert back the steps) so we
                // guarantee that:  time + interval - interval = time
                let datetime = NaiveDateTime::from_timestamp_millis(self - millis as i64)
                    .unwrap_or_else(|| {
                        panic!("{self} cannot be interpreted as a milliseconds timestamp")
                    });
                (datetime - Months::new(months)).timestamp_millis()
            }
        }
    }
}

impl Add<Interval> for i64 {
    type Output = i64;
    fn add(self, rhs: Interval) -> Self::Output {
        match rhs.size {
            IntervalSize::Discrete(number) => self + (number as i64),
            IntervalSize::Temporal { millis, months } => {
                // first we add the number of months and then the number of milliseconds for
                // consistency with the implementation of Sub (we revert back the steps) so we
                // guarantee that:  time + interval - interval = time
                let datetime = NaiveDateTime::from_timestamp_millis(self).unwrap_or_else(|| {
                    panic!("{self} cannot be interpreted as a milliseconds timestamp")
                });
                (datetime + Months::new(months)).timestamp_millis() + millis as i64
            }
        }
    }
}

#[cfg(test)]
mod time_tests {
    use crate::core::util::time::{Interval, ParseTimeError, TryIntoTime};

    #[test]
    fn interval_parsing() {
        let second: u64 = 1000;
        let minute = 60 * second;
        let hour = 60 * minute;
        let day = 24 * hour;
        let week = 7 * day;

        let interval: Interval = "1 day".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), day);

        let interval: Interval = "1 week".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), week);

        let interval: Interval = "4 weeks and 1 day".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), 4 * week + day);

        let interval: Interval = "2 days & 1 millisecond".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), 2 * day + 1);

        let interval: Interval = "2 days, 1 hour, and 2 minutes".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), 2 * day + hour + 2 * minute);

        let interval: Interval = "1 weeks ,   1 minute".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), week + minute);

        let interval: Interval = "23 seconds  and 34 millisecond and 1 minute"
            .try_into()
            .unwrap();
        assert_eq!(interval.to_millis().unwrap(), 23 * second + 34 + minute);
    }

    #[test]
    fn interval_parsing_with_months_and_years() {
        let dt = "2020-01-01 00:00:00".try_into_time().unwrap();

        let two_months: Interval = "2 months".try_into().unwrap();
        let dt_plus_2_months = "2020-03-01 00:00:00".try_into_time().unwrap();
        assert_eq!(dt + two_months, dt_plus_2_months);

        let two_years: Interval = "2 years".try_into().unwrap();
        let dt_plus_2_years = "2022-01-01 00:00:00".try_into_time().unwrap();
        assert_eq!(dt + two_years, dt_plus_2_years);

        let mix_interval: Interval = "1 year 1 month and 1 second".try_into().unwrap();
        let dt_mix = "2021-02-01 00:00:01".try_into_time().unwrap();
        assert_eq!(dt + mix_interval, dt_mix);
    }

    #[test]
    fn invalid_intervals() {
        let result: Result<Interval, ParseTimeError> = "".try_into();
        assert_eq!(result, Err(ParseTimeError::InvalidPairs));

        let result: Result<Interval, ParseTimeError> = "1".try_into();
        assert_eq!(result, Err(ParseTimeError::InvalidPairs));

        let result: Result<Interval, ParseTimeError> = "1 day and 5".try_into();
        assert_eq!(result, Err(ParseTimeError::InvalidPairs));

        let result: Result<Interval, ParseTimeError> = "1 daay".try_into();
        assert_eq!(result, Err(ParseTimeError::InvalidUnit("daay".to_string())));

        let result: Result<Interval, ParseTimeError> = "day 1".try_into();

        match result {
            Err(ParseTimeError::ParseInt { .. }) => (),
            _ => panic!(),
        }
    }
}
