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
    InputObject, OneOfInput, ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue,
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
    entities::properties::prop::{IntoPropMap, Prop},
    storage::{
        arc_str::ArcStr,
        timeindex::{AsTime, EventTime},
    },
    utils::time::{IntoTime, TryIntoTime},
};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
    sync::Arc,
};

#[derive(InputObject, Clone, Debug, Serialize, Deserialize)]
pub struct ObjectEntry {
    /// Key.
    pub key: String,
    /// Value.
    pub value: Value,
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
    F32(f32),
    /// 64 bit float.
    F64(f64),
    /// String.
    Str(String),
    /// Boolean.
    Bool(bool),
    /// List.
    List(Vec<Value>),
    /// Object.
    Object(Vec<ObjectEntry>),
    /// Timezone-aware datetime.
    DTime(String),
    /// Naive datetime (no timezone).
    NDTime(String),
    /// BigDecimal number (string representation, e.g. "3.14159" or "123e-5").
    Decimal(String),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::U8(v) => write!(f, "U8({})", v),
            Value::U16(v) => write!(f, "U16({})", v),
            Value::U32(v) => write!(f, "U32({})", v),
            Value::U64(v) => write!(f, "U64({})", v),
            Value::I32(v) => write!(f, "I32({})", v),
            Value::I64(v) => write!(f, "I64({})", v),
            Value::F32(v) => write!(f, "F32({})", v),
            Value::F64(v) => write!(f, "F64({})", v),
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
        Value::U8(n) => Ok(Prop::U8(n)),
        Value::U16(n) => Ok(Prop::U16(n)),
        Value::U32(n) => Ok(Prop::U32(n)),
        Value::U64(n) => Ok(Prop::U64(n)),
        Value::I32(n) => Ok(Prop::I32(n)),
        Value::I64(n) => Ok(Prop::I64(n)),
        Value::F32(n) => Ok(Prop::F32(n)),
        Value::F64(n) => Ok(Prop::F64(n)),
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
            let prop_map: FxHashMap<ArcStr, Prop> = object
                .into_iter()
                .map(|oe| Ok::<_, GraphError>((ArcStr::from(oe.key), value_to_prop(oe.value)?)))
                .collect::<Result<FxHashMap<_, _>, _>>()?;
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

fn gql_to_prop(value: GqlValue) -> Result<Prop, Error> {
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
            .collect::<Result<HashMap<String, Prop>, Error>>()?
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
        Prop::F32(u) => Number::from_f64(*u as f64)
            .map(|number| GqlValue::Number(number))
            .unwrap_or(GqlValue::Null),
        Prop::F64(u) => Number::from_f64(*u as f64)
            .map(|number| GqlValue::Number(number))
            .unwrap_or(GqlValue::Null),
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
}

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
}

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
    ///
    /// * `t` — a TimeInput (epoch millis integer, RFC3339 string, or `{timestamp, eventId}` object).
    async fn at(&self, t: GqlTimeInput) -> Option<GqlPropertyOutputVal> {
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
    ///
    /// * `latestTime` — if true, each run is represented by its *last* timestamp; if
    ///   false, by its *first*. Useful for compressing chatter in a timeline.
    async fn ordered_dedupe(&self, latest_time: bool) -> Vec<GqlPropertyTuple> {
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

#[derive(ResolvedObject, Clone)]
#[graphql(name = "Properties")]
pub(crate) struct GqlProperties {
    props: DynProperties,
}

impl GqlProperties {
    #[allow(dead_code)] //This is actually being used, but for some reason cargo complains
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
    ///
    /// * `key` — the property name.
    async fn get(&self, key: String) -> Option<GqlProperty> {
        self.props
            .get(key.as_str())
            .map(|p| (key.to_string(), p).into())
    }

    /// Returns true if a property with the given key exists in this view.
    ///
    /// * `key` — the property name to look up.
    async fn contains(&self, key: String) -> bool {
        self.props.get(&key).is_some()
    }

    /// All property keys present in the current view. Does not include metadata
    /// — metadata is a separate surface accessed via the entity's `metadata`
    /// field.
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
    ///
    /// * `keys` — optional whitelist. If provided, only properties with these keys are
    ///   returned; if omitted or null, every property in the view is returned.
    async fn values(&self, keys: Option<Vec<String>>) -> Vec<GqlProperty> {
        let self_clone = self.clone();
        blocking_compute(move || match keys {
            Some(keys) => self_clone
                .props
                .iter_filtered()
                .filter_map(|(k, prop)| {
                    let key = k.to_string();
                    if keys.contains(&key) {
                        Some((key, prop).into())
                    } else {
                        None
                    }
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
    ///
    /// * `key` — the metadata name.
    async fn get(&self, key: String) -> Option<GqlProperty> {
        self.props
            .get(key.as_str())
            .map(|p| (key.to_string(), p).into())
    }

    /// Returns true if a metadata entry with the given key exists.
    ///
    /// * `key` — the metadata name to look up.
    async fn contains(&self, key: String) -> bool {
        self.props.contains(key.as_str())
    }

    /// All metadata keys present on this entity.
    async fn keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.props.keys().map(|k| k.clone().into()).collect()).await
    }

    /// All metadata values as `{key, value}` entries.
    ///
    /// * `keys` — optional whitelist. If provided, only metadata with these keys is
    ///   returned; if omitted, every metadata entry is returned.
    pub(crate) async fn values(&self, keys: Option<Vec<String>>) -> Vec<GqlProperty> {
        let self_clone = self.clone();
        blocking_compute(move || match keys {
            Some(keys) => self_clone
                .props
                .iter_filtered()
                .filter_map(|(k, p)| {
                    let key = k.to_string();
                    if keys.contains(&key) {
                        Some((key, p).into())
                    } else {
                        None
                    }
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
    ///
    /// * `key` — the property name.
    async fn get(&self, key: String) -> Option<GqlTemporalProperty> {
        self.props.get(key.as_str()).map(move |p| (key, p).into())
    }

    /// Returns true if a temporal property with the given key exists.
    ///
    /// * `key` — the property name to look up.
    async fn contains(&self, key: String) -> bool {
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
    ///
    /// * `keys` — optional whitelist. If provided, only temporal properties with these
    ///   keys are returned; if omitted, every temporal property in the view is returned.
    async fn values(&self, keys: Option<Vec<String>>) -> Vec<GqlTemporalProperty> {
        let self_clone = self.clone();
        blocking_compute(move || match keys {
            Some(keys) => self_clone
                .props
                .iter_filtered()
                .filter_map(|(k, p)| {
                    let key = k.to_string();
                    if keys.contains(&key) {
                        Some((key, p).into())
                    } else {
                        None
                    }
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
