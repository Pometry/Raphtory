use async_graphql::{Error, Value as GqlValue};
use dynamic_graphql::{
    InputObject, OneOfInput, ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue,
};
use raphtory_api::core::{
    storage::timeindex::{AsTime, TimeIndexEntry},
    utils::time::{IntoTime, ParseTimeError, TryIntoTime},
};

/// Input for primary time component. Int or DateTime formatted String.
/// Valid formats are RFC3339, RFC2822, %Y-%m-%d, %Y-%m-%dT%H:%M:%S%.3f, %Y-%m-%dT%H:%M:%S%, %Y-%m-%d %H:%M:%S%.3f and %Y-%m-%d %H:%M:%S%
#[derive(Scalar, Clone, Debug)]
#[graphql(name = "SimpleTimeInput")]
pub struct GqlSimpleTimeInput(pub i64);

impl ScalarValue for GqlSimpleTimeInput {
    fn from_value(value: GqlValue) -> Result<Self, Error> {
        match value {
            GqlValue::Number(timestamp) => timestamp
                .as_i64()
                .ok_or(Error::new("Expected Int or DateTime formatted String"))
                .map(|timestamp| GqlSimpleTimeInput(timestamp)),
            GqlValue::String(dt) => dt
                .try_into_time()
                .map(|t| GqlSimpleTimeInput(t.t()))
                .map_err(|e| Error::new(e.to_string())),
            _ => Err(Error::new("Expected Int or DateTime formatted String"))
        }
    }

    fn to_value(&self) -> GqlValue {
        self.0.into()
    }
}

/// Input for indexed time entries.
#[derive(InputObject, Clone, Debug)]
#[graphql(name = "IndexedTimeInput")]
pub struct GqlIndexedTimeInput {
    time: GqlSimpleTimeInput,
    secondary_index: usize,
}

/// Input for simple or indexed time entries.
#[derive(OneOfInput, Clone, Debug)]
#[graphql(name = "TimeInput")]
pub enum GqlTimeInput {
    SimpleTime(GqlSimpleTimeInput),
    IndexedTime(GqlIndexedTimeInput),
}

impl From<i64> for GqlSimpleTimeInput {
    fn from(value: i64) -> Self {
        GqlSimpleTimeInput(value)
    }
}

impl IntoTime for GqlSimpleTimeInput {
    fn into_time(self) -> TimeIndexEntry {
        self.0.into_time()
    }
}

impl TryIntoTime for GqlTimeInput {
    fn try_into_time(self) -> Result<TimeIndexEntry, ParseTimeError> {
        match self {
            GqlTimeInput::SimpleTime(simple) => simple.try_into_time(),
            GqlTimeInput::IndexedTime(GqlIndexedTimeInput {
                time,
                secondary_index,
            }) => Ok(TimeIndexEntry::new(
                time.into_time().t(),
                secondary_index,
            )),
        }
    }
}

/// Represents a time index entry with timestamp and secondary index for ordering
#[derive(ResolvedObject, Clone)]
#[graphql(name = "TimeIndexEntry")]
pub struct GqlTimeIndexEntry {
    pub(crate) entry: TimeIndexEntry,
}

impl From<TimeIndexEntry> for GqlTimeIndexEntry {
    fn from(entry: TimeIndexEntry) -> Self {
        Self { entry }
    }
}

impl From<GqlTimeIndexEntry> for TimeIndexEntry {
    fn from(entry: GqlTimeIndexEntry) -> Self {
        entry.entry
    }
}

impl IntoTime for GqlTimeIndexEntry {
    fn into_time(self) -> TimeIndexEntry {
        self.entry
    }
}

#[ResolvedObjectFields]
impl GqlTimeIndexEntry {
    /// The timestamp as milliseconds since Unix epoch
    async fn timestamp(&self) -> i64 {
        self.entry.t()
    }

    /// The secondary index of the TimeIndexEntry
    async fn secondary_index(&self) -> u64 {
        self.entry.i() as u64
    }

    /// Returns a datetime string representation if timestamp is valid, or else returns an error message string.
    /// Uses RFC 3339 format by default (e.g., "2023-12-25T10:30:45.123Z").
    /// Optionally takes a format string as argument to format the output of datetimes.
    /// Refer to chrono::format::strftime for formatting specifiers and escape sequences.
    async fn datetime(&self, format_string: Option<String>) -> String {
        let fmt_string = format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
        match self.entry.dt() {
            Ok(dt) => dt.format(fmt_string).to_string(),
            Err(e) => format!("Time conversion error: {}", e.to_string()),
        }
    }
}
