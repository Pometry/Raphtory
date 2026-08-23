use crate::{
    model::graph::{
        history::GqlHistory,
        timeindex::{GqlEventTime, GqlTimeInput},
    },
    rayon::blocking_compute,
};
use async_graphql::{Error, Name, Value as GqlValue};
use bigdecimal::BigDecimal;
use dynamic_graphql::{
    Enum, InputObject, OneOfInput, ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue,
};
use itertools::Itertools;
use raphtory::{
    db::api::properties::{
        dyn_props::{DynMetadata, DynProperties, DynProps, DynTemporalProperties},
        TemporalPropertyView,
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::{
    entities::properties::prop::{IntoPropMap, Prop, PropMap, PropType},
    storage::{
        arc_str::ArcStr,
        timeindex::{AsTime, EventTime},
    },
    utils::time::{IntoTime, TryIntoTime},
};
use serde::{ser::Error as SerError, Deserialize, Serialize, Serializer};
use serde_json::Number;
use std::{
    convert::TryFrom,
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
    sync::Arc,
};

#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectEntry {
    /// Key.
    pub key: String,
    /// Value.
    pub value: Value,
}

/// Non-finite float values, which JSON cannot represent as numbers.
///
/// Follows protobuf's JSON mapping convention of spelling these out
/// explicitly rather than silently coercing them to `null`.
#[derive(Enum, Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SpecialFloat {
    Nan,
    Infinity,
    NegInfinity,
}

impl SpecialFloat {
    pub(crate) fn as_f64(self) -> f64 {
        match self {
            SpecialFloat::Nan => f64::NAN,
            SpecialFloat::Infinity => f64::INFINITY,
            SpecialFloat::NegInfinity => f64::NEG_INFINITY,
        }
    }

    /// Classify a non-finite float. Callers must check `!v.is_finite()` first.
    pub(crate) fn of(v: f64) -> SpecialFloat {
        if v.is_nan() {
            SpecialFloat::Nan
        } else if v > 0.0 {
            SpecialFloat::Infinity
        } else {
            SpecialFloat::NegInfinity
        }
    }
}

/// The untagged-output sentinel for a non-finite float, mirroring protobuf's
/// JSON mapping. Parsed back by type-directed decoders (`parse_special_float`).
pub(crate) fn special_float_sentinel(v: f64) -> &'static str {
    match SpecialFloat::of(v) {
        SpecialFloat::Nan => "NaN",
        SpecialFloat::Infinity => "Infinity",
        SpecialFloat::NegInfinity => "-Infinity",
    }
}

/// Parse an untagged-output float sentinel back to the value it encodes.
pub(crate) fn parse_special_float(s: &str) -> Option<f64> {
    match s {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

#[derive(OneOfInput, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Value {
    /// 8 bit unsigned integer.
    U8(u8),
    /// 16 bit unsigned integer.
    U16(u16),
    /// 32 bit unsigned integer.
    U32(u32),
    /// 64 bit unsigned integer.
    U64(u64),
    /// 32 bit signed integer.
    I32(i32),
    /// 64 bit signed integer.
    I64(i64),
    /// 32 bit float.
    #[serde(serialize_with = "serialize_finite_f32")]
    F32(f32),
    /// 64 bit float.
    #[serde(serialize_with = "serialize_finite_f64")]
    F64(f64),
    /// Non-finite 32 bit float (NaN, ±Infinity) — JSON has no number form for these.
    F32Special(SpecialFloat),
    /// Non-finite 64 bit float (NaN, ±Infinity) — JSON has no number form for these.
    F64Special(SpecialFloat),
    /// String.
    Str(String),
    /// Boolean.
    Bool(bool),
    /// List.
    List(Vec<Value>),
    /// Object.
    Object(Vec<ObjectEntry>),
    /// Timezone-aware datetime.
    #[serde(rename = "dtime")]
    DTime(String),
    /// Naive datetime (no timezone).
    #[serde(rename = "ndtime")]
    NDTime(String),
    /// BigDecimal number (string representation, e.g. "3.14159" or "123e-5").
    Decimal(String),
    /// A named placeholder, resolved before the filter is evaluated.
    ///
    /// Lets a filter be written once with per-request values left open — an authorization policy
    /// binds them per caller. A `Var` must be substituted before the filter reaches the engine;
    /// converting one to a `Prop` is an error rather than a silent default.
    Var(String),
    /// A named claim, read straight from the caller's token and substituted before evaluation.
    ///
    /// Like [`Value::Var`] but sourced directly from a token claim rather than a binding, so no
    /// spec is needed. Must be substituted before the filter reaches the engine; converting one to
    /// a `Prop` is an error rather than a silent default.
    Claim(String),
}

// JSON has no NaN/Infinity — `serde_json` would silently coerce them to `null`,
// sending a malformed value. Non-finite floats belong in the `F32Special` /
// `F64Special` variants (`prop_to_value` routes them there); these guards keep
// that invariant loud — a non-finite float reaching the numeric variants is a
// bug, and surfaces as an error instead of a silent `null` on the wire.
fn serialize_finite_f64<S: Serializer>(v: &f64, serializer: S) -> Result<S::Ok, S::Error> {
    if v.is_finite() {
        serializer.serialize_f64(*v)
    } else {
        Err(SerError::custom(
            "non-finite float (NaN/Infinity) is not a valid value",
        ))
    }
}

fn serialize_finite_f32<S: Serializer>(v: &f32, serializer: S) -> Result<S::Ok, S::Error> {
    if v.is_finite() {
        serializer.serialize_f32(*v)
    } else {
        Err(SerError::custom(
            "non-finite float (NaN/Infinity) is not a valid value",
        ))
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Var(name) => write!(f, "Var({})", name),
            Value::Claim(name) => write!(f, "Claim({})", name),
            Value::U8(v) => write!(f, "U8({})", v),
            Value::U16(v) => write!(f, "U16({})", v),
            Value::U32(v) => write!(f, "U32({})", v),
            Value::U64(v) => write!(f, "U64({})", v),
            Value::I32(v) => write!(f, "I32({})", v),
            Value::I64(v) => write!(f, "I64({})", v),
            Value::F32(v) => write!(f, "F32({})", v),
            Value::F64(v) => write!(f, "F64({})", v),
            Value::F32Special(s) => write!(f, "F32({})", s.as_f64()),
            Value::F64Special(s) => write!(f, "F64({})", s.as_f64()),
            Value::Str(v) => write!(f, "Str({})", v),
            Value::Bool(v) => write!(f, "Bool({})", v),
            Value::List(vs) => {
                let inner = vs.iter().map(|v| v.to_string()).join(", ");
                write!(f, "List([{}])", inner)
            }
            Value::Object(entries) => {
                let inner = entries
                    .iter()
                    .map(|entry| format!("{}: {}", entry.key, entry.value))
                    .join(", ");
                write!(f, "Object({{{}}})", inner)
            }
            Value::DTime(v) => write!(f, "DTime({})", v),
            Value::NDTime(v) => write!(f, "NDTime({})", v),
            Value::Decimal(v) => write!(f, "Decimal({})", v),
        }
    }
}

impl TryFrom<Value> for Prop {
    type Error = GraphError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        value_to_prop(value)
    }
}

fn value_to_prop(value: Value) -> Result<Prop, GraphError> {
    match value {
        // A placeholder that reached evaluation was never substituted. Erroring here keeps that a
        // visible bug rather than a filter that quietly matches nothing.
        Value::Var(name) => Err(GraphError::InvalidGqlFilter(format!(
            "unresolved variable '{name}' in filter"
        ))),
        Value::Claim(name) => Err(GraphError::InvalidGqlFilter(format!(
            "unresolved claim '{name}' in filter"
        ))),
        Value::U8(n) => Ok(Prop::U8(n)),
        Value::U16(n) => Ok(Prop::U16(n)),
        Value::U32(n) => Ok(Prop::U32(n)),
        Value::U64(n) => Ok(Prop::U64(n)),
        Value::I32(n) => Ok(Prop::I32(n)),
        Value::I64(n) => Ok(Prop::I64(n)),
        Value::F32(n) => Ok(Prop::F32(n)),
        Value::F64(n) => Ok(Prop::F64(n)),
        Value::F32Special(s) => Ok(Prop::F32(s.as_f64() as f32)),
        Value::F64Special(s) => Ok(Prop::F64(s.as_f64())),
        Value::Str(s) => Ok(Prop::Str(s.into())),
        Value::Bool(b) => Ok(Prop::Bool(b)),
        Value::List(list) => {
            let prop_list: Vec<Prop> = list
                .into_iter()
                .map(value_to_prop)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Prop::List(prop_list.into()))
        }
        Value::Object(object) => {
            let prop_map: PropMap = object
                .into_iter()
                .map(|oe| Ok::<_, GraphError>((ArcStr::from(oe.key), value_to_prop(oe.value)?)))
                .collect::<Result<PropMap, _>>()?;
            Ok(Prop::Map(Arc::new(prop_map)))
        }
        Value::DTime(s) => {
            let t = s.try_into_time().map_err(GraphError::from)?;
            t.dt().map(|dt| Prop::DTime(dt)).map_err(GraphError::from)
        }
        Value::NDTime(s) => {
            let t = s.try_into_time().map_err(GraphError::from)?;
            t.dt()
                .map(|dt| Prop::NDTime(dt.naive_utc()))
                .map_err(GraphError::from)
        }
        Value::Decimal(s) => {
            let bd = BigDecimal::from_str(&s).map_err(|e| GraphError::InvalidProperty {
                reason: format!("Invalid Decimal: {e}"),
            })?;
            Prop::try_from_bd(bd).map_err(|e| GraphError::InvalidProperty {
                reason: format!("Decimal too large: {e}"),
            })
        }
    }
}

impl TryFrom<&Prop> for Value {
    type Error = GraphError;

    fn try_from(prop: &Prop) -> Result<Self, Self::Error> {
        prop_to_value(prop)
    }
}

/// A `Prop` from the engine → GQL wire `Value`. Mirror of [`value_to_prop`];
/// non-lossy for scalars. Naive datetimes truncate to millisecond precision —
/// the server's time parser accepts at most 3 fractional digits.
fn prop_to_value(p: &Prop) -> Result<Value, GraphError> {
    Ok(match p {
        Prop::Str(s) => Value::Str(s.to_string()),
        Prop::U8(v) => Value::U8(*v),
        Prop::U16(v) => Value::U16(*v),
        Prop::U32(v) => Value::U32(*v),
        Prop::U64(v) => Value::U64(*v),
        Prop::I32(v) => Value::I32(*v),
        Prop::I64(v) => Value::I64(*v),
        Prop::F32(v) if !v.is_finite() => Value::F32Special(SpecialFloat::of(*v as f64)),
        Prop::F32(v) => Value::F32(*v),
        Prop::F64(v) if !v.is_finite() => Value::F64Special(SpecialFloat::of(*v)),
        Prop::F64(v) => Value::F64(*v),
        Prop::Bool(v) => Value::Bool(*v),
        Prop::NDTime(v) => Value::NDTime(v.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()),
        Prop::DTime(v) => Value::DTime(v.to_rfc3339()),
        Prop::Decimal(v) => Value::Decimal(v.to_string()),
        Prop::List(arr) => {
            let items: Result<Vec<Value>, GraphError> =
                arr.iter().map(|p| prop_to_value(&p)).collect();
            Value::List(items?)
        }
        Prop::Map(map) => {
            // Map props are insertion-ordered, so iteration order is already
            // deterministic and survives the wire round-trip.
            let entries = map
                .iter()
                .map(|(k, v)| {
                    Ok(ObjectEntry {
                        key: k.to_string(),
                        value: prop_to_value(v)?,
                    })
                })
                .collect::<Result<Vec<_>, GraphError>>()?;
            Value::Object(entries)
        }
    })
}

#[derive(Clone, Debug, Scalar)]
#[graphql(name = "PropertyOutput")]
pub struct GqlPropertyOutputVal(pub Prop);

impl ScalarValue for GqlPropertyOutputVal {
    fn from_value(value: GqlValue) -> Result<GqlPropertyOutputVal, Error> {
        Ok(GqlPropertyOutputVal(gql_to_prop(value)?))
    }

    fn to_value(&self) -> GqlValue {
        prop_to_gql(&self.0)
    }
}

/// A property's type, as `PropType`'s serde JSON form — round-trippable,
/// unlike the human-readable string from `getDtypeOf`. Scalars are bare
/// strings (`"F64"`), containers are tagged objects
/// (`{"List": "F64"}`, `{"Map": {"a": "I64"}}`, `{"Decimal": {"scale": 2}}`).
#[derive(Clone, Debug, Scalar)]
#[graphql(name = "PropertyType")]
pub struct GqlPropTypeOutput(pub PropType);

impl ScalarValue for GqlPropTypeOutput {
    fn from_value(value: GqlValue) -> Result<GqlPropTypeOutput, Error> {
        let json = value.into_json().map_err(|e| Error::new(e.to_string()))?;
        Ok(GqlPropTypeOutput(
            serde_json::from_value(json).map_err(|e| Error::new(e.to_string()))?,
        ))
    }

    fn to_value(&self) -> GqlValue {
        serde_json::to_value(&self.0)
            .ok()
            .and_then(|v| GqlValue::from_json(v).ok())
            .unwrap_or(GqlValue::Null)
    }
}

/// Decode an `async_graphql::Value` into a `Prop` (lossy: number → I64/F64,
/// object → Map). The single source of truth for JSON→`Prop` value semantics —
/// the client's response decoder (`json_to_prop`) delegates here after
/// converting `serde_json::Value` via `Value::from_json`.
pub(crate) fn gql_to_prop(value: GqlValue) -> Result<Prop, Error> {
    match value {
        GqlValue::Number(n) => {
            if let Some(n) = n.as_i64() {
                Ok(Prop::I64(n))
            } else if let Some(n) = n.as_f64() {
                Ok(Prop::F64(n))
            } else {
                Err(Error::new("Unable to convert"))
            }
        }
        GqlValue::Boolean(b) => Ok(Prop::Bool(b)),
        GqlValue::Object(obj) => Ok(obj
            .into_iter()
            .map(|(k, v)| gql_to_prop(v).map(|vv| (k.to_string(), vv)))
            .collect::<Result<Vec<(String, Prop)>, Error>>()?
            .into_prop_map()),
        GqlValue::String(s) => Ok(Prop::Str(s.into())),
        GqlValue::List(arr) => Ok(Prop::List(
            arr.into_iter()
                .map(gql_to_prop)
                .collect::<Result<Vec<Prop>, Error>>()?
                .into(),
        )),
        _ => Err(Error::new("Unable to convert")),
    }
}

fn prop_to_gql(prop: &Prop) -> GqlValue {
    match prop {
        Prop::Str(s) => GqlValue::String(s.to_string()),
        Prop::U8(u) => GqlValue::Number(Number::from(*u)),
        Prop::U16(u) => GqlValue::Number(Number::from(*u)),
        Prop::I32(u) => GqlValue::Number(Number::from(*u)),
        Prop::I64(u) => GqlValue::Number(Number::from(*u)),
        Prop::U32(u) => GqlValue::Number(Number::from(*u)),
        Prop::U64(u) => GqlValue::Number(Number::from(*u)),
        // Non-finite floats have no JSON number form — emit protobuf-style
        // string sentinels ("NaN"/"Infinity"/"-Infinity") instead of a silent
        // null. Type-directed decoders (which know the field is a float)
        // convert these back losslessly.
        Prop::F32(u) => Number::from_f64(*u as f64)
            .map(GqlValue::Number)
            .unwrap_or_else(|| GqlValue::String(special_float_sentinel(*u as f64).to_string())),
        Prop::F64(u) => Number::from_f64(*u)
            .map(GqlValue::Number)
            .unwrap_or_else(|| GqlValue::String(special_float_sentinel(*u).to_string())),
        Prop::Bool(b) => GqlValue::Boolean(*b),
        Prop::List(l) => GqlValue::List(l.iter().map(|pp| prop_to_gql(&pp)).collect()),
        Prop::Map(m) => GqlValue::Object(
            m.iter()
                .map(|(k, v)| (Name::new(k.to_string()), prop_to_gql(v)))
                .collect(),
        ),
        Prop::DTime(t) => GqlValue::Number(t.timestamp_millis().into()),
        Prop::NDTime(t) => GqlValue::Number(t.and_utc().timestamp_millis().into()),
        Prop::Decimal(d) => GqlValue::String(d.to_string()),
    }
}

/// A single `(key, value)` property reading at a point in the graph view.
/// The value is exposed both as a typed scalar (`value`) and as a
/// human-readable string (`asString`).
#[derive(Clone, ResolvedObject)]
#[graphql(name = "Property")]
pub(crate) struct GqlProperty {
    key: String,
    prop: Prop,
}

impl GqlProperty {
    pub(crate) fn new(key: String, prop: Prop) -> Self {
        Self { key, prop }
    }
}

impl From<(String, Prop)> for GqlProperty {
    fn from(value: (String, Prop)) -> Self {
        GqlProperty::new(value.0, value.1)
    }
}

#[ResolvedObjectFields]
impl GqlProperty {
    /// The property key (name).
    async fn key(&self) -> String {
        self.key.clone()
    }

    /// The property value rendered as a human-readable string (e.g. `"10"`, `"hello"`,
    /// `"2024-01-01T00:00:00Z"`). For programmatic access use `value`, which returns
    /// a typed scalar.
    async fn as_string(&self) -> String {
        self.prop.to_string()
    }

    /// The property value as a typed `PropertyOutput` scalar — numbers come back as
    /// numbers, booleans as booleans, strings as strings, etc.
    async fn value(&self) -> GqlPropertyOutputVal {
        GqlPropertyOutputVal(self.prop.clone())
    }

    /// The property's exact type, for type-directed decoding of `value`
    /// (`value` alone collapses e.g. all integer widths to one JSON number).
    async fn dtype(&self) -> GqlPropTypeOutput {
        GqlPropTypeOutput(self.prop.dtype())
    }
}

/// A `(time, value)` pair — the output type of temporal-property accessors
/// that need to report *when* a value was observed (e.g. `min`, `max`,
/// `median`, `orderedDedupe`).
#[derive(ResolvedObject, Clone)]
#[graphql(name = "PropertyTuple")]
pub(crate) struct GqlPropertyTuple {
    time: EventTime,
    prop: Prop,
}

impl GqlPropertyTuple {
    pub(crate) fn new(time: EventTime, prop: Prop) -> Self {
        Self { time, prop }
    }
}

impl From<(EventTime, Prop)> for GqlPropertyTuple {
    fn from(value: (EventTime, Prop)) -> Self {
        GqlPropertyTuple::new(value.0, value.1)
    }
}

#[ResolvedObjectFields]
impl GqlPropertyTuple {
    /// The timestamp at which this value was recorded.
    async fn time(&self) -> GqlEventTime {
        self.time.into()
    }

    /// The value rendered as a human-readable string. For programmatic access use
    /// `value`, which returns a typed scalar.
    async fn as_string(&self) -> String {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.to_string()).await
    }

    /// The value as a typed `PropertyOutput` scalar — numbers come back as numbers,
    /// booleans as booleans, etc.
    async fn value(&self) -> GqlPropertyOutputVal {
        GqlPropertyOutputVal(self.prop.clone())
    }

    /// The value's exact type, for type-directed decoding of `value`.
    async fn dtype(&self) -> GqlPropTypeOutput {
        GqlPropTypeOutput(self.prop.dtype())
    }
}

/// The full timeline of a single property key on one entity. Exposes every
/// update (via `values` / `history` / `orderedDedupe`), point lookups (`at`,
/// `latest`), and aggregates over the timeline (`sum`, `mean`, `min`, `max`,
/// `median`, `count`).
#[derive(ResolvedObject, Clone)]
#[graphql(name = "TemporalProperty")]
pub(crate) struct GqlTemporalProperty {
    key: String,
    prop: TemporalPropertyView<DynProps>,
}

impl GqlTemporalProperty {
    pub(crate) fn new(key: String, prop: TemporalPropertyView<DynProps>) -> Self {
        Self { key, prop }
    }
}

impl From<(String, TemporalPropertyView<DynProps>)> for GqlTemporalProperty {
    fn from(value: (String, TemporalPropertyView<DynProps>)) -> Self {
        GqlTemporalProperty::new(value.0, value.1)
    }
}

#[ResolvedObjectFields]
impl GqlTemporalProperty {
    /// The property key (name).
    async fn key(&self) -> String {
        self.key.clone()
    }

    /// The property's declared type, for type-directed decoding of stored
    /// values (`values`, `at`, `latest`, `unique`, `min`, `max`, `median`,
    /// `orderedDedupe`). Aggregates (`sum`, `mean`, `average`) may widen.
    async fn dtype(&self) -> GqlPropTypeOutput {
        GqlPropTypeOutput(self.prop.dtype())
    }

    /// Event history for this property — one entry per temporal update, in
    /// insertion order. Use this to navigate the full timeline: access the
    /// raw `timestamps` / `datetimes` / `eventId` lists, analyse gaps between
    /// updates via `intervals` (mean/median/min/max), ask `isEmpty`, or
    /// paginate the events.
    async fn history(&self) -> GqlHistory {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.history().into()).await
    }

    /// All values this property has ever taken, in temporal order (one per update).
    /// Typed as `PropertyOutput` so numeric values stay numeric.
    async fn values(&self) -> Vec<GqlPropertyOutputVal> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.values().map(GqlPropertyOutputVal).collect()).await
    }

    /// The value at or before time `t` (latest update on or before `t`). Returns null
    /// if no update exists on or before `t`.

    async fn at(
        &self,
        #[graphql(
            desc = "A TimeInput (epoch millis integer, RFC3339 string, or `{timestamp, eventId}` object)."
        )]
        t: GqlTimeInput,
    ) -> Option<GqlPropertyOutputVal> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.at(t.into_time()).map(GqlPropertyOutputVal)).await
    }

    /// The most recent value, or null if the property has never been set in this view.
    async fn latest(&self) -> Option<GqlPropertyOutputVal> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.latest().map(GqlPropertyOutputVal)).await
    }

    /// The set of distinct values this property has ever taken (order not guaranteed).
    async fn unique(&self) -> Vec<GqlPropertyOutputVal> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .prop
                .unique()
                .into_iter()
                .map(GqlPropertyOutputVal)
                .collect_vec()
        })
        .await
    }

    /// Collapses runs of consecutive-equal updates into a single `(time, value)` pair.

    async fn ordered_dedupe(
        &self,
        #[graphql(
            desc = "If true, each run is represented by its *last* timestamp; if false, by its *first*. Useful for compressing chatter in a timeline."
        )]
        latest_time: bool,
    ) -> Vec<GqlPropertyTuple> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .prop
                .ordered_dedupe(latest_time)
                .into_iter()
                .map(|(k, p)| (k, p).into())
                .collect()
        })
        .await
    }

    /// Sum of all updates. Returns null if the dtype is not additive or the property is empty.
    async fn sum(&self) -> Option<GqlPropertyOutputVal> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.sum().map(GqlPropertyOutputVal)).await
    }

    /// Mean of all updates as an F64. Returns null if any value is non-numeric or the property is
    /// empty.
    async fn mean(&self) -> Option<GqlPropertyOutputVal> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.mean().map(GqlPropertyOutputVal)).await
    }

    /// Alias for `mean` — same F64 average, same null cases.
    async fn average(&self) -> Option<GqlPropertyOutputVal> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.average().map(GqlPropertyOutputVal)).await
    }

    /// Minimum `(time, value)` pair. Returns null if the dtype is not comparable or the property is
    /// empty.
    async fn min(&self) -> Option<GqlPropertyTuple> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.min().map(GqlPropertyTuple::from)).await
    }

    /// Maximum `(time, value)` pair. Returns null if the dtype is not comparable or the property is
    /// empty.
    async fn max(&self) -> Option<GqlPropertyTuple> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.max().map(GqlPropertyTuple::from)).await
    }

    /// Median `(time, value)` pair (lower median on even-length inputs). Returns null if the dtype
    /// is not comparable or the property is empty.
    async fn median(&self) -> Option<GqlPropertyTuple> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.median().map(GqlPropertyTuple::from)).await
    }

    /// Number of updates recorded for this property in the current view.
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.prop.count()).await
    }
}

/// All temporal properties of an entity (metadata is exposed separately).
/// Look up individual properties via `get` / `contains`, enumerate via
/// `keys` / `values`, or drop into `temporal` for time-aware accessors.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Properties")]
pub(crate) struct GqlProperties {
    props: DynProperties,
}

impl GqlProperties {
    #[allow(dead_code)]
    pub(crate) fn new(props: DynProperties) -> Self {
        Self { props }
    }
}

impl<P: Into<DynProperties>> From<P> for GqlProperties {
    fn from(value: P) -> Self {
        Self {
            props: value.into(),
        }
    }
}

/// The temporal-only view of an entity's properties. Each entry is a
/// `TemporalProperty` carrying the full timeline for that key — use this when
/// you need per-update iteration, time-indexed lookups, or aggregates.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "TemporalProperties")]
pub(crate) struct GqlTemporalProperties {
    props: DynTemporalProperties,
}

impl GqlTemporalProperties {
    pub(crate) fn new(props: DynTemporalProperties) -> Self {
        Self { props }
    }
}

impl From<DynTemporalProperties> for GqlTemporalProperties {
    fn from(value: DynTemporalProperties) -> Self {
        GqlTemporalProperties::new(value)
    }
}

/// Constant key/value metadata attached to an entity (node, edge, or graph).
/// Metadata has no timeline — each key maps to exactly one value for the
/// lifetime of the entity. Separate from `Properties`, which carries
/// time-varying data.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Metadata")]
pub(crate) struct GqlMetadata {
    props: DynMetadata,
}

impl GqlMetadata {
    pub(crate) fn new(props: DynMetadata) -> Self {
        Self { props }
    }
}

impl<P: Into<DynMetadata>> From<P> for GqlMetadata {
    fn from(value: P) -> Self {
        GqlMetadata::new(value.into())
    }
}

#[ResolvedObjectFields]
impl GqlProperties {
    /// Look up a single property by key. Returns null if no property with that key
    /// exists in the current view.

    async fn get(
        &self,
        #[graphql(desc = "The property name.")] key: String,
    ) -> Option<GqlProperty> {
        self.props
            .get(key.as_str())
            .map(|p| (key.to_string(), p).into())
    }

    /// Returns true if a property with the given key exists in this view.

    async fn contains(
        &self,
        #[graphql(desc = "The property name to look up.")] key: String,
    ) -> bool {
        self.props.get(&key).is_some()
    }

    /// The data-type of the property's latest value by key, as its `PropType`
    /// display string (e.g. `"I64"`, `"Str"`, `"List<F64>"`). Returns null when
    /// the key isn't present. Mirrors the local `Properties.get_dtype_of`.

    async fn get_dtype_of(
        &self,
        #[graphql(desc = "The property name.")] key: String,
    ) -> Option<String> {
        self.props.get(key.as_str()).map(|p| p.dtype().to_string())
    }

    /// All property keys present in the current view. Does not include metadata
    /// — metadata is exposed separately via the entity's `metadata` field.
    async fn keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .props
                .iter_filtered()
                .map(|(k, _)| k.into())
                .collect()
        })
        .await
    }

    /// Snapshot of property values, one `{key, value}` entry per property.

    async fn values(
        &self,
        #[graphql(
            desc = "Optional whitelist. If provided, only properties with these keys are returned; if omitted or null, every property in the view is returned."
        )]
        keys: Option<Vec<String>>,
    ) -> Vec<GqlProperty> {
        let self_clone = self.clone();
        blocking_compute(move || match keys {
            Some(keys) => self_clone
                .props
                .iter_filtered()
                .filter_map(|(k, prop)| {
                    let key = k.to_string();
                    keys.contains(&key).then_some((key, prop).into())
                })
                .collect(),
            None => self_clone
                .props
                .iter_filtered()
                .map(|(k, prop)| (k.to_string(), prop).into())
                .collect(),
        })
        .await
    }

    /// The temporal-only view of these properties — excludes metadata (which has no
    /// history) and lets you drill into per-key timelines and aggregates.
    async fn temporal(&self) -> GqlTemporalProperties {
        self.props.temporal().into()
    }
}

#[ResolvedObjectFields]
impl GqlMetadata {
    /// Look up a single metadata value by key. Returns null if no metadata with that
    /// key exists.

    async fn get(
        &self,
        #[graphql(desc = "The metadata name.")] key: String,
    ) -> Option<GqlProperty> {
        self.props
            .get(key.as_str())
            .map(|p| (key.to_string(), p).into())
    }

    /// Returns true if a metadata entry with the given key exists.

    async fn contains(
        &self,
        #[graphql(desc = "The metadata name to look up.")] key: String,
    ) -> bool {
        self.props.contains(key.as_str())
    }

    /// All metadata keys present on this entity.
    async fn keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.props.keys().map(|k| k.clone().into()).collect()).await
    }

    /// All metadata values as `{key, value}` entries.

    pub(crate) async fn values(
        &self,
        #[graphql(
            desc = "Optional whitelist. If provided, only metadata with these keys is returned; if omitted, every metadata entry is returned."
        )]
        keys: Option<Vec<String>>,
    ) -> Vec<GqlProperty> {
        let self_clone = self.clone();
        blocking_compute(move || match keys {
            Some(keys) => self_clone
                .props
                .iter_filtered()
                .filter_map(|(k, p)| {
                    let key = k.to_string();
                    keys.contains(&key).then_some((key, p).into())
                })
                .collect(),
            None => self_clone
                .props
                .iter_filtered()
                .map(|(k, p)| (k.to_string(), p).into())
                .collect(),
        })
        .await
    }
}

#[ResolvedObjectFields]
impl GqlTemporalProperties {
    /// Look up a single temporal property by key. Returns null if there's no temporal
    /// property with that key.

    async fn get(
        &self,
        #[graphql(desc = "The property name.")] key: String,
    ) -> Option<GqlTemporalProperty> {
        self.props.get(key.as_str()).map(move |p| (key, p).into())
    }

    /// Returns true if a temporal property with the given key exists.

    async fn contains(
        &self,
        #[graphql(desc = "The property name to look up.")] key: String,
    ) -> bool {
        self.props.get(&key).is_some()
    }

    /// All temporal-property keys present in this view.
    async fn keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .props
                .iter_filtered()
                .map(|(k, _)| k.into())
                .collect()
        })
        .await
    }

    /// All temporal properties, each as a `TemporalProperty` with its full timeline
    /// available. Use `history`, `values`, `latest`, `at`, etc. on each entry.

    async fn values(
        &self,
        #[graphql(
            desc = "Optional whitelist. If provided, only temporal properties with these keys are returned; if omitted, every temporal property in the view is returned."
        )]
        keys: Option<Vec<String>>,
    ) -> Vec<GqlTemporalProperty> {
        let self_clone = self.clone();
        blocking_compute(move || match keys {
            Some(keys) => self_clone
                .props
                .iter_filtered()
                .filter_map(|(k, p)| {
                    let key = k.to_string();
                    keys.contains(&key).then_some((key, p).into())
                })
                .collect(),
            None => self_clone
                .props
                .iter_filtered()
                .map(|(k, p)| (k.to_string(), p).into())
                .collect(),
        })
        .await
    }
}

#[cfg(test)]
mod value_serde_tests {
    use super::*;

    // Datetime variants serialize to the schema field names `dtime`/`ndtime`
    // (the OneOfInput @oneOf shape) — the same spelling on every path.
    #[test]
    fn datetime_variants_use_schema_field_names() {
        let d = serde_json::to_value(Value::DTime("2020-01-01T00:00:00Z".to_owned())).unwrap();
        assert_eq!(d, serde_json::json!({ "dtime": "2020-01-01T00:00:00Z" }));
        let nd = serde_json::to_value(Value::NDTime("2020-01-01T00:00:00".to_owned())).unwrap();
        assert_eq!(nd, serde_json::json!({ "ndtime": "2020-01-01T00:00:00" }));
    }

    #[test]
    fn scalar_variants_keep_lowercase_names() {
        assert_eq!(
            serde_json::to_value(Value::F64(6.0)).unwrap(),
            serde_json::json!({ "f64": 6.0 })
        );
        assert_eq!(
            serde_json::to_value(Value::Str("x".to_owned())).unwrap(),
            serde_json::json!({ "str": "x" })
        );
    }

    // Map-valued props are writable: they convert to the wire `object` form
    // (in insertion order) and round-trip back to the same `Prop::Map` with
    // key order intact.
    #[test]
    fn map_prop_round_trips_as_object() {
        let map: PropMap = [
            (ArcStr::from("b"), Prop::I64(2)),
            (ArcStr::from("a"), Prop::str("x")),
        ]
        .into_iter()
        .collect();
        let prop = Prop::Map(Arc::new(map));

        let value = Value::try_from(&prop).unwrap();
        let Value::Object(entries) = &value else {
            panic!("expected Object, got {value:?}");
        };
        let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, vec!["b", "a"]);

        let round_tripped = value_to_prop(value.clone()).unwrap();
        assert_eq!(round_tripped, prop);
        let Prop::Map(rt_map) = round_tripped else {
            panic!("expected Map");
        };
        let rt_keys: Vec<&str> = rt_map.keys().map(|k| k.as_ref()).collect();
        assert_eq!(rt_keys, vec!["b", "a"]);
    }

    // Non-finite floats have no JSON number form — they ride in the
    // `f64Special`/`f32Special` variants and convert back to real floats.
    #[test]
    fn non_finite_floats_round_trip_via_special_variants() {
        for (input, expected) in [
            (f64::NAN, SpecialFloat::Nan),
            (f64::INFINITY, SpecialFloat::Infinity),
            (f64::NEG_INFINITY, SpecialFloat::NegInfinity),
        ] {
            let value = Value::try_from(&Prop::F64(input)).unwrap();
            let Value::F64Special(s) = &value else {
                panic!("expected F64Special, got {value:?}");
            };
            assert_eq!(*s, expected);

            let Prop::F64(back) = value_to_prop(value).unwrap() else {
                panic!("expected F64 back");
            };
            assert!(back.is_nan() == input.is_nan() && (input.is_nan() || back == input));
        }

        let value = Value::try_from(&Prop::F32(f32::NEG_INFINITY)).unwrap();
        assert_eq!(
            serde_json::to_value(&value).unwrap(),
            serde_json::json!({ "f32Special": "NEG_INFINITY" })
        );
        let Prop::F32(back) = value_to_prop(value).unwrap() else {
            panic!("expected F32 back");
        };
        assert_eq!(back, f32::NEG_INFINITY);

        // Finite floats keep the plain numeric form.
        assert_eq!(
            serde_json::to_value(Value::try_from(&Prop::F64(1.5)).unwrap()).unwrap(),
            serde_json::json!({ "f64": 1.5 })
        );
    }

    // The server's time parser accepts at most 3 fractional digits, so naive
    // datetimes truncate to millis on the wire — and the truncated form must
    // parse back server-side.
    #[test]
    fn ndtime_prop_truncates_to_millis_on_the_wire() {
        use chrono::NaiveDateTime;

        let dt: NaiveDateTime = "2020-01-01T00:00:00.123456".parse().unwrap();
        let value = Value::try_from(&Prop::NDTime(dt)).unwrap();
        let Value::NDTime(s) = &value else {
            panic!("expected NDTime, got {value:?}");
        };
        assert_eq!(s, "2020-01-01T00:00:00.123");

        let round_tripped = value_to_prop(value.clone()).unwrap();
        let expected: NaiveDateTime = "2020-01-01T00:00:00.123".parse().unwrap();
        assert_eq!(round_tripped, Prop::NDTime(expected));
    }
}
