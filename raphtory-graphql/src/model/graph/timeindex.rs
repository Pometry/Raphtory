use async_graphql::{indexmap::IndexMap, Error, Name, Value as GqlValue};
use chrono::format::{Item, StrftimeItems};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue};
use raphtory_api::core::{
    storage::timeindex::{AsTime, EventTime},
    utils::time::{InputTime, IntoTime, TryIntoTime},
};
use serde::{
    de::{self, MapAccess, Visitor},
    ser::SerializeMap,
    Deserializer, Serializer,
};
use std::fmt;

/// Input for primary time component. Expects Int, DateTime formatted String, or Object { timestamp, eventId }
/// where the timestamp is either an Int or a DateTime formatted String, and eventId is a non-negative Int.
/// Valid string formats are RFC3339, RFC2822, %Y-%m-%d, %Y-%m-%dT%H:%M:%S%.3f, %Y-%m-%dT%H:%M:%S%,
/// %Y-%m-%d %H:%M:%S%.3f and %Y-%m-%d %H:%M:%S%.
///
/// Internally wraps `InputTime` so write paths (`addNode`, `addEdge`,
/// `addProperties`, etc.) can preserve auto-increment of `event_id` when only
/// a timestamp is given. Pass the object form `{timestamp, eventId}` to lock
/// the event_id explicitly.
#[derive(Scalar, Clone, Debug)]
#[graphql(name = "TimeInput")]
pub struct GqlTimeInput(pub InputTime);

// Serialize to the wire form the `TimeInput` scalar accepts (a bare int, or an
// object `{timestamp, eventId}` for the indexed case) — NOT the derived
// external-tag `{"Simple": n}`, which the server's `ScalarValue::from_value`
// rejects. `StoredGraphFilter` round-trips these via serde, so `Deserialize`
// below mirrors this exactly.
impl serde::Serialize for GqlTimeInput {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            InputTime::Simple(ts) => serializer.serialize_i64(ts),
            InputTime::Indexed(ts, idx) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("timestamp", &ts)?;
                map.serialize_entry("eventId", &idx)?;
                map.end()
            }
        }
    }
}

impl<'de> serde::Deserialize<'de> for GqlTimeInput {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct GqlTimeInputVisitor;

        impl<'de> Visitor<'de> for GqlTimeInputVisitor {
            type Value = GqlTimeInput;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(
                    "an integer timestamp, a datetime string, or an object { timestamp, eventId }",
                )
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(GqlTimeInput(InputTime::Simple(v)))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(GqlTimeInput(InputTime::Simple(v as i64)))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.try_into_time()
                    .map(|t| GqlTimeInput(InputTime::Simple(t.t())))
                    .map_err(E::custom)
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut ts: Option<i64> = None;
                let mut idx: Option<usize> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "timestamp" | "time" => ts = Some(map.next_value()?),
                        "eventId" | "id" => idx = Some(map.next_value()?),
                        // Legacy shapes: stored `GraphAccessFilter` JSON written
                        // before the custom serde used `InputTime`'s derived
                        // external-tag encoding ({"Simple": t} / {"Indexed": [t, i]}).
                        // Keep reading them so old permission stores load.
                        "Simple" => ts = Some(map.next_value()?),
                        "Indexed" => {
                            let (t, i): (i64, usize) = map.next_value()?;
                            ts = Some(t);
                            idx = Some(i);
                        }
                        _ => {
                            map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                let ts = ts.ok_or_else(|| de::Error::missing_field("timestamp"))?;
                match idx {
                    Some(idx) => Ok(GqlTimeInput(InputTime::Indexed(ts, idx))),
                    None => Ok(GqlTimeInput(InputTime::Simple(ts))),
                }
            }
        }

        deserializer.deserialize_any(GqlTimeInputVisitor)
    }
}

impl ScalarValue for GqlTimeInput {
    fn from_value(value: GqlValue) -> Result<Self, Error> {
        match value {
            GqlValue::Number(timestamp) => timestamp
                .as_i64()
                .ok_or(Error::new(
                    "Expected Int, DateTime formatted String, or Object { timestamp, eventId }.",
                ))
                .map(|timestamp| GqlTimeInput(InputTime::Simple(timestamp))),

            GqlValue::String(dt) => dt
                .try_into_time()
                .map(|t| GqlTimeInput(InputTime::Simple(t.t())))
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

                Ok(GqlTimeInput(InputTime::Indexed(ts, idx)))
            }
            _ => Err(Error::new(
                "Expected Int, DateTime formatted String, or Object { timestamp, eventId }.",
            )),
        }
    }

    // The exact inverse of `from_value`: a bare timestamp for `Simple`, the
    // `{timestamp, eventId}` object for `Indexed`. Rendering `Indexed` as a
    // bare timestamp would silently drop the locked event_id.
    fn to_value(&self) -> GqlValue {
        match self.0 {
            InputTime::Simple(t) => t.into(),
            InputTime::Indexed(t, event_id) => {
                let mut obj = IndexMap::new();
                obj.insert(Name::new("timestamp"), t.into());
                obj.insert(Name::new("eventId"), event_id.into());
                GqlValue::Object(obj)
            }
        }
    }
}

impl From<i64> for GqlTimeInput {
    fn from(value: i64) -> Self {
        GqlTimeInput(InputTime::Simple(value))
    }
}

impl GqlTimeInput {
    /// Extract just the timestamp (for read-side query args like `window`,
    /// `at`, `before`, `after`). Auto-increment semantics aren't relevant
    /// when only reading.
    pub fn t(&self) -> i64 {
        match &self.0 {
            InputTime::Simple(t) => *t,
            InputTime::Indexed(t, _) => *t,
        }
    }

    /// Pass the underlying `InputTime` straight through to write paths so
    /// `Simple` causes the graph to allocate a fresh `event_id` and
    /// `Indexed` locks one explicitly.
    pub fn into_input_time(self) -> InputTime {
        self.0
    }
}

impl IntoTime for GqlTimeInput {
    /// Build an `EventTime`. For read-side use only — write paths should call
    /// `into_input_time` instead so auto-increment of `event_id` works.
    fn into_time(self) -> EventTime {
        match self.0 {
            InputTime::Simple(t) => EventTime::start(t),
            InputTime::Indexed(t, e) => EventTime::new(t, e),
        }
    }
}

pub fn dt_format_str_is_valid(fmt_str: &str) -> bool {
    !StrftimeItems::new(fmt_str).any(|it| matches!(it, Item::Error))
}

/// Raphtory’s EventTime.
/// Represents a unique timepoint in the graph’s history as (timestamp, event_id).
///
/// - timestamp: Number of milliseconds since the Unix epoch.
/// - event_id: ID used for ordering between equal timestamps.
///
/// Instances of EventTime may or may not contain time information.
/// This is relevant for functions that may not return data (such as earliest_time and latest_time) because the data is unavailable.
/// When empty, time operations (such as timestamp, datetime, and event_id) will return None.
#[derive(ResolvedObject, Clone, Copy)]
#[graphql(name = "EventTime")]
pub struct GqlEventTime {
    pub(crate) inner: Option<EventTime>,
}

#[ResolvedObjectFields]
impl GqlEventTime {
    /// Get the timestamp in milliseconds since the Unix epoch.
    async fn timestamp(&self) -> Option<i64> {
        self.inner.map(|t| t.t())
    }

    /// Get the event id for the EventTime. Used for ordering within the same timestamp.
    async fn event_id(&self) -> Option<u64> {
        self.inner.map(|t| t.i() as u64)
    }

    /// Access a datetime representation of the EventTime as a String.
    /// Useful for converting millisecond timestamps into easily readable datetime strings.
    /// Optionally, a format string can be passed to format the output.
    /// Defaults to RFC 3339 if not provided (e.g., "2023-12-25T10:30:45.123Z").
    /// Refer to chrono::format::strftime for formatting specifiers and escape sequences.
    /// Raises an error if a time conversion fails.

    async fn datetime(
        &self,
        #[graphql(
            desc = "Optional format string for the rendered datetime. Uses `%`-style specifiers — for example `%Y-%m-%d` for `2024-01-15`, `%Y-%m-%d %H:%M:%S` for `2024-01-15 10:30:00`, or `%H:%M` for `10:30`. Defaults to RFC 3339 (e.g. `2024-01-15T10:30:45.123+00:00`) when omitted."
        )]
        format_string: Option<String>,
    ) -> Result<Option<String>, Error> {
        let fmt_string = format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
        if dt_format_str_is_valid(fmt_string) {
            self.inner
                .map(|t| {
                    t.dt()
                        .map(|dt| dt.format(fmt_string).to_string())
                        .map_err(|e| Error::new(e.to_string()))
                })
                .transpose()
        } else {
            Err(Error::new(format!(
                "Invalid datetime format string: '{}'",
                fmt_string
            )))
        }
    }
}

impl From<Option<EventTime>> for GqlEventTime {
    fn from(value: Option<EventTime>) -> Self {
        Self { inner: value }
    }
}

impl From<EventTime> for GqlEventTime {
    fn from(value: EventTime) -> Self {
        Self { inner: Some(value) }
    }
}

impl From<GqlEventTime> for Option<EventTime> {
    fn from(value: GqlEventTime) -> Self {
        value.inner
    }
}

#[cfg(test)]
mod time_input_serde_tests {
    use super::*;
    use raphtory_api::core::utils::time::InputTime;

    // The wire form must be the scalar `TimeInput` shape the server's
    // `ScalarValue::from_value` accepts — a bare int, or `{timestamp, eventId}`
    // — NOT the derived external-tag `{"Simple": n}`.
    #[test]
    fn simple_serializes_as_bare_int() {
        let v = serde_json::to_value(GqlTimeInput(InputTime::Simple(5))).unwrap();
        assert_eq!(v, serde_json::json!(5));
    }

    #[test]
    fn indexed_serializes_as_timestamp_event_id_object() {
        let v = serde_json::to_value(GqlTimeInput(InputTime::Indexed(5, 2))).unwrap();
        assert_eq!(v, serde_json::json!({ "timestamp": 5, "eventId": 2 }));
    }

    // `to_value` is the scalar's render direction — it must be the exact
    // inverse of `from_value`, or a locked event_id is lost on the way out.
    #[test]
    fn round_trips_through_the_scalar_value_pair() {
        for t in [
            InputTime::Simple(-3),
            InputTime::Simple(0),
            InputTime::Indexed(7, 0),
            InputTime::Indexed(9, 4),
        ] {
            let rendered = GqlTimeInput(t).to_value();
            let back = GqlTimeInput::from_value(rendered).unwrap();
            assert_eq!(back.0, t, "scalar round-trip lost information for {t:?}");
        }
    }

    // The rendered form matches the serde wire form, so both paths agree.
    #[test]
    fn to_value_matches_the_serde_wire_form() {
        for t in [InputTime::Simple(5), InputTime::Indexed(5, 2)] {
            let rendered = serde_json::to_value(GqlTimeInput(t).to_value()).unwrap();
            let serialized = serde_json::to_value(GqlTimeInput(t)).unwrap();
            assert_eq!(
                rendered, serialized,
                "to_value disagrees with serde for {t:?}"
            );
        }
    }

    #[test]
    fn round_trips_through_serde() {
        for t in [
            InputTime::Simple(-3),
            InputTime::Indexed(7, 0),
            InputTime::Indexed(9, 4),
        ] {
            let json = serde_json::to_value(GqlTimeInput(t)).unwrap();
            let back: GqlTimeInput = serde_json::from_value(json).unwrap();
            assert_eq!(back.0, t);
        }
    }

    #[test]
    fn deserializes_bare_int_and_object() {
        let a: GqlTimeInput = serde_json::from_value(serde_json::json!(5)).unwrap();
        assert_eq!(a.0, InputTime::Simple(5));
        let b: GqlTimeInput =
            serde_json::from_value(serde_json::json!({ "timestamp": 5, "eventId": 2 })).unwrap();
        assert_eq!(b.0, InputTime::Indexed(5, 2));
    }

    // Stored `GraphAccessFilter` JSON written before the custom serde carries
    // `InputTime`'s derived external-tag encoding; old permission stores must
    // keep loading.
    #[test]
    fn deserializes_legacy_derived_shapes() {
        let simple: GqlTimeInput =
            serde_json::from_value(serde_json::json!({ "Simple": 5 })).unwrap();
        assert_eq!(simple.0, InputTime::Simple(5));
        let indexed: GqlTimeInput =
            serde_json::from_value(serde_json::json!({ "Indexed": [5, 2] })).unwrap();
        assert_eq!(indexed.0, InputTime::Indexed(5, 2));
    }
}
