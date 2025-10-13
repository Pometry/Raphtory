use chrono::{DateTime, Duration, Months};
use itertools::{Either, Itertools};
use raphtory_api::core::{storage::timeindex::EventTime, utils::time::ParseTimeError};
use regex::Regex;
use std::ops::{Add, Sub};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IntervalSize {
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
    pub epoch_alignment: bool,
    pub size: IntervalSize,
}

impl Default for Interval {
    fn default() -> Self {
        Self {
            epoch_alignment: false,
            size: IntervalSize::Discrete(1),
        }
    }
}

impl TryFrom<String> for Interval {
    type Error = ParseTimeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
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

        let tokens = cleaned.split(' ').collect_vec();

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
            Err(errors.first().unwrap().clone())
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

impl TryFrom<u32> for Interval {
    type Error = ParseTimeError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch_alignment: false,
            size: IntervalSize::Discrete(value as u64),
        })
    }
}

impl TryFrom<i32> for Interval {
    type Error = ParseTimeError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value >= 0 {
            Ok(Self {
                epoch_alignment: false,
                size: IntervalSize::Discrete(value as u64),
            })
        } else {
            Err(ParseTimeError::NegativeInt)
        }
    }
}

impl TryFrom<i64> for Interval {
    type Error = ParseTimeError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value >= 0 {
            Ok(Self {
                epoch_alignment: false,
                size: IntervalSize::Discrete(value as u64),
            })
        } else {
            Err(ParseTimeError::NegativeInt)
        }
    }
}

pub trait TryIntoInterval {
    fn try_into_interval(self) -> Result<Interval, ParseTimeError>;
}

impl<T> TryIntoInterval for T
where
    Interval: TryFrom<T>,
    ParseTimeError: From<<Interval as TryFrom<T>>::Error>,
{
    fn try_into_interval(self) -> Result<Interval, ParseTimeError> {
        Ok(self.try_into()?)
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

    pub fn discrete(num: u64) -> Self {
        Interval {
            epoch_alignment: false,
            size: IntervalSize::Discrete(num),
        }
    }

    pub fn milliseconds(ms: i64) -> Self {
        Interval {
            epoch_alignment: true,
            size: IntervalSize::from(Duration::milliseconds(ms)),
        }
    }

    pub fn seconds(seconds: i64) -> Self {
        Interval {
            epoch_alignment: true,
            size: IntervalSize::from(Duration::seconds(seconds)),
        }
    }

    pub fn minutes(minutes: i64) -> Self {
        Interval {
            epoch_alignment: true,
            size: IntervalSize::from(Duration::minutes(minutes)),
        }
    }

    pub fn hours(hours: i64) -> Self {
        Interval {
            epoch_alignment: true,
            size: IntervalSize::from(Duration::hours(hours)),
        }
    }

    pub fn days(days: i64) -> Self {
        Interval {
            epoch_alignment: true,
            size: IntervalSize::from(Duration::days(days)),
        }
    }

    pub fn weeks(weeks: i64) -> Self {
        Interval {
            epoch_alignment: true,
            size: IntervalSize::from(Duration::weeks(weeks)),
        }
    }

    pub fn months(months: i64) -> Self {
        Interval {
            epoch_alignment: true,
            size: IntervalSize::months(months),
        }
    }

    pub fn years(years: i64) -> Self {
        Interval {
            epoch_alignment: true,
            size: IntervalSize::months(12 * years),
        }
    }

    pub fn and(&self, other: &Self) -> Result<Self, IntervalTypeError> {
        match (self.size, other.size) {
            (IntervalSize::Discrete(l), IntervalSize::Discrete(r)) => Ok(Interval {
                epoch_alignment: false,
                size: IntervalSize::Discrete(l + r),
            }),
            (IntervalSize::Temporal { .. }, IntervalSize::Temporal { .. }) => Ok(Interval {
                epoch_alignment: true,
                size: self.size.add_temporal(other.size),
            }),
            (_, _) => Err(IntervalTypeError()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Discrete and temporal intervals cannot be combined")]
pub struct IntervalTypeError();

impl Sub<Interval> for i64 {
    type Output = i64;
    fn sub(self, rhs: Interval) -> Self::Output {
        match rhs.size {
            IntervalSize::Discrete(number)
            | IntervalSize::Temporal {
                millis: number,
                months: 0,
            } => self - (number as i64),
            IntervalSize::Temporal { millis, months } => {
                // first we subtract the number of milliseconds and then the number of months for
                // consistency with the implementation of Add (we revert back the steps) so we
                // guarantee that:  time + interval - interval = time
                let datetime = DateTime::from_timestamp_millis(self - millis as i64)
                    .unwrap_or_else(|| {
                        panic!("{self} cannot be interpreted as a milliseconds timestamp")
                    })
                    .naive_utc();
                (datetime - Months::new(months))
                    .and_utc()
                    .timestamp_millis()
            }
        }
    }
}

impl Add<Interval> for i64 {
    type Output = i64;
    fn add(self, rhs: Interval) -> Self::Output {
        match rhs.size {
            IntervalSize::Discrete(number)
            | IntervalSize::Temporal {
                millis: number,
                months: 0,
            } => self + (number as i64),
            IntervalSize::Temporal { millis, months } => {
                // first we add the number of months and then the number of milliseconds for
                // consistency with the implementation of Sub (we revert back the steps) so we
                // guarantee that:  time + interval - interval = time
                let datetime = DateTime::from_timestamp_millis(self)
                    .unwrap_or_else(|| {
                        panic!("{self} cannot be interpreted as a milliseconds timestamp")
                    })
                    .naive_utc();
                (datetime + Months::new(months))
                    .and_utc()
                    .timestamp_millis()
                    + millis as i64
            }
        }
    }
}

impl Add<Interval> for EventTime {
    type Output = EventTime;
    fn add(self, rhs: Interval) -> Self::Output {
        match rhs.size {
            IntervalSize::Discrete(number) => EventTime::from(self.0 + (number as i64)),
            IntervalSize::Temporal { millis, months } => {
                // first we add the number of months and then the number of milliseconds for
                // consistency with the implementation of Sub (we revert back the steps) so we
                // guarantee that:  time + interval - interval = time
                let datetime = DateTime::from_timestamp_millis(self.0)
                    .unwrap_or_else(|| {
                        panic!("{self} cannot be interpreted as a milliseconds timestamp")
                    })
                    .naive_utc();
                let timestamp = (datetime + Months::new(months))
                    .and_utc()
                    .timestamp_millis()
                    + millis as i64;
                EventTime(timestamp, self.1)
            }
        }
    }
}

#[cfg(test)]
mod time_tests {
    use crate::utils::time::Interval;
    use raphtory_api::core::utils::time::{ParseTimeError, TryIntoTime};

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
