use async_graphql::{Error, Value as GqlValue};
use chrono::{
    format::{Item, StrftimeItems},
    DateTime, Utc,
};
use dynamic_graphql::{
    InputObject, OneOfInput, ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue,
};
use raphtory_api::core::{
    storage::timeindex::{AsTime, TimeError, TimeIndexEntry},
    utils::time::{IntoTime, ParseTimeError, TryIntoTime},
};

/// Input for primary time component. Expects Int, DateTime formatted String, or Object {epoch, index}
/// where epoch is either an Int or a DateTime formatted String, and index is a non-negative Int.
/// Valid string formats are RFC3339, RFC2822, %Y-%m-%d, %Y-%m-%dT%H:%M:%S%.3f, %Y-%m-%dT%H:%M:%S%,
/// %Y-%m-%d %H:%M:%S%.3f and %Y-%m-%d %H:%M:%S%.
#[derive(Scalar, Clone, Debug)]
#[graphql(name = "TimeInput")]
pub struct GqlTimeInput(pub TimeIndexEntry);

impl ScalarValue for GqlTimeInput {
    fn from_value(value: GqlValue) -> Result<Self, Error> {
        match value {
            GqlValue::Number(timestamp) => timestamp
                .as_i64()
                .ok_or(Error::new(
                    "Expected Int, DateTime formatted String, or Object { epoch, index }.",
                ))
                .map(|timestamp| GqlTimeInput(TimeIndexEntry::start(timestamp))),

            GqlValue::String(dt) => dt
                .try_into_time()
                .map(|t| GqlTimeInput(t.set_index(0)))
                .map_err(|e| Error::new(e.to_string())),

            // TimeInput: Object { epoch: Number | String, index: Number }
            GqlValue::Object(obj) => {
                let epoch_val = obj
                    .get("epoch")
                    .or_else(|| obj.get("time")) // optional alias for convenience
                    .ok_or_else(|| Error::new("Object must contain 'epoch' (or 'time')."))?;

                let ts = match epoch_val {
                    GqlValue::Number(n) => n
                        .as_i64()
                        .ok_or(Error::new("epoch must be an Int or a DateTime String."))?,
                    GqlValue::String(s) => s
                        .try_into_time()
                        .map_err(|e| Error::new(e.to_string()))?
                        .t(),
                    _ => return Err(Error::new("epoch must be an Int or a DateTime String.")),
                };

                let idx_val = obj
                    .get("index")
                    .or_else(|| obj.get("secondaryIndex")) // optional alias for convenience
                    .ok_or_else(|| {
                        Error::new("Object must contain 'index' (or 'secondary_index').")
                    })?;
                let idx: usize = match idx_val {
                    GqlValue::Number(n) => {
                        let u = n
                            .as_u64()
                            .ok_or(Error::new("index must be a non-negative Int."))?;
                        usize::try_from(u).map_err(|_| Error::new("index out of range"))?
                    }
                    _ => return Err(Error::new("index must be a non-negative Int.")),
                };

                Ok(GqlTimeInput(TimeIndexEntry::new(ts, idx)))
            }
            _ => Err(Error::new(
                "Expected Int, DateTime formatted String, or Object { epoch, index }.",
            )),
        }
    }

    fn to_value(&self) -> GqlValue {
        self.0.t().into()
    }
}

impl From<i64> for GqlTimeInput {
    fn from(value: i64) -> Self {
        GqlTimeInput(TimeIndexEntry::start(value))
    }
}

impl IntoTime for GqlTimeInput {
    fn into_time(self) -> TimeIndexEntry {
        self.0
    }
}

/// Time index entry with timestamp and secondary index for ordering within the same timestamp.
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
    /// Get the timestamp in milliseconds since Unix epoch.
    async fn timestamp(&self) -> i64 {
        self.entry.t()
    }

    /// Get the secondary index for the time entry. Used for ordering within the same timestamp.
    async fn secondary_index(&self) -> u64 {
        self.entry.i() as u64
    }

    /// Access a datetime representation of the TimeIndexEntry as a String.
    /// Useful for converting millisecond timestamps into easily readable datetime strings.
    /// Optionally, a format string can be passed to format the output.
    /// Defaults to RFC 3339 if not provided (e.g., "2023-12-25T10:30:45.123Z").
    /// Refer to chrono::format::strftime for formatting specifiers and escape sequences.
    /// Raises TimeError if a time conversion fails.
    async fn datetime(&self, format_string: Option<String>) -> Result<String, Error> {
        let fmt_string = format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
        if dt_format_str_is_valid(fmt_string) {
            self.entry
                .dt()
                .map(|dt| dt.format(fmt_string).to_string())
                .map_err(|e| Error::new(e.to_string()))
        } else {
            Err(Error::new(format!(
                "Invalid datetime format string: '{}'",
                fmt_string
            )))
        }
    }
}

pub fn dt_format_str_is_valid(fmt_str: &str) -> bool {
    if StrftimeItems::new(fmt_str).any(|it| matches!(it, Item::Error)) {
        false
    } else {
        true
    }
}
