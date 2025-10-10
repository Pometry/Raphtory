use async_graphql::{Error, Value as GqlValue};
use chrono::format::{Item, StrftimeItems};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue};
use raphtory_api::core::{
    storage::timeindex::{AsTime, EventTime},
    utils::time::{IntoTime, TryIntoTime},
};

/// Input for primary time component. Expects Int, DateTime formatted String, or Object { timestamp, eventId }
/// where the timestamp is either an Int or a DateTime formatted String, and eventId is a non-negative Int.
/// Valid string formats are RFC3339, RFC2822, %Y-%m-%d, %Y-%m-%dT%H:%M:%S%.3f, %Y-%m-%dT%H:%M:%S%,
/// %Y-%m-%d %H:%M:%S%.3f and %Y-%m-%d %H:%M:%S%.
#[derive(Scalar, Clone, Debug)]
#[graphql(name = "TimeInput")]
pub struct GqlTimeInput(pub EventTime);

impl ScalarValue for GqlTimeInput {
    fn from_value(value: GqlValue) -> Result<Self, Error> {
        match value {
            GqlValue::Number(timestamp) => timestamp
                .as_i64()
                .ok_or(Error::new(
                    "Expected Int, DateTime formatted String, or Object { timestamp, eventId }.",
                ))
                .map(|timestamp| GqlTimeInput(EventTime::start(timestamp))),

            GqlValue::String(dt) => dt
                .try_into_time()
                .map(|t| GqlTimeInput(t.set_event_id(0)))
                .map_err(|e| Error::new(e.to_string())),

            // TimeInput: Object { timestamp: Number | String, eventId: Number }
            GqlValue::Object(obj) => {
                let timestamp_val = obj
                    .get("timestamp")
                    .or_else(|| obj.get("time")) // optional alias for convenience
                    .ok_or_else(|| Error::new("Object must contain 'timestamp' (or 'time')."))?;

                let ts = match timestamp_val {
                    GqlValue::Number(n) => n
                        .as_i64()
                        .ok_or(Error::new("timestamp must be an Int or a DateTime String."))?,
                    GqlValue::String(s) => s
                        .try_into_time()
                        .map_err(|e| Error::new(e.to_string()))?
                        .t(),
                    _ => return Err(Error::new("timestamp must be an Int or a DateTime String.")),
                };

                let idx_val = obj
                    .get("eventId")
                    .or_else(|| obj.get("id")) // optional alias for convenience
                    .ok_or_else(|| Error::new("Object must contain 'eventId' (or 'id')."))?;
                let idx: usize = match idx_val {
                    GqlValue::Number(n) => {
                        let u = n
                            .as_u64()
                            .ok_or(Error::new("eventId must be a non-negative Int."))?;
                        usize::try_from(u).map_err(|_| Error::new("index out of range"))?
                    }
                    _ => return Err(Error::new("eventId must be a non-negative Int.")),
                };

                Ok(GqlTimeInput(EventTime::new(ts, idx)))
            }
            _ => Err(Error::new(
                "Expected Int, DateTime formatted String, or Object { timestamp, eventId }.",
            )),
        }
    }

    fn to_value(&self) -> GqlValue {
        self.0.t().into()
    }
}

impl From<i64> for GqlTimeInput {
    fn from(value: i64) -> Self {
        GqlTimeInput(EventTime::start(value))
    }
}

impl IntoTime for GqlTimeInput {
    fn into_time(self) -> EventTime {
        self.0
    }
}

/// Raphtory’s EventTime.
/// Represents a unique timepoint in the graph’s history as (timestamp, event_id).
///
/// - timestamp: number of milliseconds since the Unix epoch.
/// - event_id: id used for ordering between equal timestamps.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "EventTime")]
pub struct GqlEventTime {
    pub(crate) entry: EventTime,
}

impl From<EventTime> for GqlEventTime {
    fn from(entry: EventTime) -> Self {
        Self { entry }
    }
}

impl From<GqlEventTime> for EventTime {
    fn from(entry: GqlEventTime) -> Self {
        entry.entry
    }
}

impl IntoTime for GqlEventTime {
    fn into_time(self) -> EventTime {
        self.entry
    }
}

#[ResolvedObjectFields]
impl GqlEventTime {
    /// Get the timestamp in milliseconds since the Unix epoch.
    async fn timestamp(&self) -> i64 {
        self.entry.t()
    }

    /// Get the event id for the EventTime. Used for ordering within the same timestamp.
    async fn event_id(&self) -> u64 {
        self.entry.i() as u64
    }

    /// Access a datetime representation of the EventTime as a String.
    /// Useful for converting millisecond timestamps into easily readable datetime strings.
    /// Optionally, a format string can be passed to format the output.
    /// Defaults to RFC 3339 if not provided (e.g., "2023-12-25T10:30:45.123Z").
    /// Refer to chrono::format::strftime for formatting specifiers and escape sequences.
    /// Raises an error if a time conversion fails.
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
