use async_graphql::{Error, Name, Value as GqlValue};
use dynamic_graphql::{InputObject, OneOfInput, ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue};
use itertools::Itertools;
use raphtory::{
    db::api::properties::{
        dyn_props::{DynConstProperties, DynProperties, DynProps, DynTemporalProperties},
        TemporalPropertyView,
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::properties::prop::{IntoPropMap, Prop},
    storage::arc_str::ArcStr,
};
use rustc_hash::FxHashMap;
use serde_json::Number;
use std::{collections::HashMap, convert::TryFrom, sync::Arc};
use tokio::task::spawn_blocking;

#[derive(InputObject, Clone, Debug)]
pub struct ObjectEntry {
    pub key: String,
    pub value: Value,
}

#[derive(OneOfInput, Clone, Debug)]
pub enum Value {
    U64(u64),
    I64(i64),
    F64(f64),
    Str(String),
    Bool(bool),
    List(Vec<Value>),
    Object(Vec<ObjectEntry>),
}

impl TryFrom<Value> for Prop {
    type Error = GraphError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        value_to_prop(value)
    }
}

fn value_to_prop(value: Value) -> Result<Prop, GraphError> {
    if let Value::U64(n) = value.clone() {
        return Ok(Prop::U64(n));
    }
    if let Value::I64(n) = value.clone() {
        return Ok(Prop::I64(n));
    }
    if let Value::F64(n) = value.clone() {
        return Ok(Prop::F64(n));
    }
    if let Value::Str(s) = value.clone() {
        return Ok(Prop::Str(s.into()));
    }
    if let Value::Bool(b) = value.clone() {
        return Ok(Prop::Bool(b));
    }
    if let Value::List(list) = value.clone() {
        let prop_list: Vec<Prop> = list
            .into_iter()
            .map(value_to_prop)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Prop::List(prop_list.into()));
    }
    if let Value::Object(object) = value.clone() {
        let prop_map: FxHashMap<ArcStr, Prop> = object
            .into_iter()
            .map(|oe| Ok::<_, GraphError>((ArcStr::from(oe.key), value_to_prop(oe.value)?)))
            .collect::<Result<FxHashMap<_, _>, _>>()?;
        return Ok(Prop::Map(Arc::new(prop_map)));
    }
    Err(GraphError::EmptyValue)
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
        Prop::List(l) => GqlValue::List(l.iter().map(|pp| prop_to_gql(pp)).collect()),
        Prop::Map(m) => GqlValue::Object(
            m.iter()
                .map(|(k, v)| (Name::new(k.to_string()), prop_to_gql(v)))
                .collect(),
        ),
        Prop::DTime(t) => GqlValue::Number(t.timestamp_millis().into()),
        Prop::NDTime(t) => GqlValue::Number(t.and_utc().timestamp_millis().into()),
        Prop::Array(a) => GqlValue::List(a.iter_prop().map(|p| prop_to_gql(&p)).collect()),
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
    async fn key(&self) -> String {
        self.key.clone()
    }

    async fn as_string(&self) -> String {
        self.prop.to_string()
    }

    async fn value(&self) -> GqlPropertyOutputVal {
        GqlPropertyOutputVal(self.prop.clone())
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "PropertyTuple")]
pub(crate) struct GqlPropertyTuple {
    time: i64,
    prop: Prop,
}

impl GqlPropertyTuple {
    pub(crate) fn new(time: i64, prop: Prop) -> Self {
        Self { time, prop }
    }
}

impl From<(i64, Prop)> for GqlPropertyTuple {
    fn from(value: (i64, Prop)) -> Self {
        GqlPropertyTuple::new(value.0, value.1)
    }
}

#[ResolvedObjectFields]
impl GqlPropertyTuple {
    async fn time(&self) -> i64 {
        self.time
    }

    async fn as_string(&self) -> String {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.prop.to_string())
            .await
            .unwrap()
    }

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
    async fn key(&self) -> String {
        self.key.clone()
    }

    async fn history(&self) -> Vec<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.prop.history().collect())
            .await
            .unwrap()
    }

    async fn values(&self) -> Vec<String> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.prop.values().map(|x| x.to_string()).collect())
            .await
            .unwrap()
    }

    async fn at(&self, t: i64) -> Option<String> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.prop.at(t).map(|x| x.to_string()))
            .await
            .unwrap()
    }

    async fn latest(&self) -> Option<String> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.prop.latest().map(|x| x.to_string()))
            .await
            .unwrap()
    }

    async fn unique(&self) -> Vec<String> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            self_clone
                .prop
                .unique()
                .into_iter()
                .map(|x| x.to_string())
                .collect_vec()
        })
        .await
        .unwrap()
    }

    async fn ordered_dedupe(&self, latest_time: bool) -> Vec<GqlPropertyTuple> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            self_clone
                .prop
                .ordered_dedupe(latest_time)
                .into_iter()
                .map(|(k, p)| (k, p).into())
                .collect()
        })
        .await
        .unwrap()
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
#[graphql(name = "ConstantProperties")]
pub(crate) struct GqlConstantProperties {
    props: DynConstProperties,
}
impl GqlConstantProperties {
    pub(crate) fn new(props: DynConstProperties) -> Self {
        Self { props }
    }
}
impl From<DynConstProperties> for GqlConstantProperties {
    fn from(value: DynConstProperties) -> Self {
        GqlConstantProperties::new(value)
    }
}

#[ResolvedObjectFields]
impl GqlProperties {
    async fn get(&self, key: String) -> Option<GqlProperty> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            self_clone
                .props
                .get(key.as_str())
                .map(|p| (key.to_string(), p).into())
        })
        .await
        .unwrap()
    }

    async fn contains(&self, key: String) -> bool {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.props.contains(key.as_str()))
            .await
            .unwrap()
    }

    async fn keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.props.keys().map(|k| k.into()).collect())
            .await
            .unwrap()
    }

    async fn values(&self, keys: Option<Vec<String>>) -> Vec<GqlProperty> {
        let self_clone = self.clone();
        spawn_blocking(move || match keys {
            Some(keys) => self_clone
                .props
                .iter()
                .filter_map(|(k, p)| {
                    let key = k.to_string();
                    if keys.contains(&key) {
                        p.map(|prop| (key, prop).into())
                    } else {
                        None
                    }
                })
                .collect(),
            None => self_clone
                .props
                .iter()
                .filter_map(|(k, p)| p.map(|prop| (k.to_string(), prop).into()))
                .collect(),
        })
        .await
        .unwrap()
    }

    async fn temporal(&self) -> GqlTemporalProperties {
        self.props.temporal().into()
    }

    async fn constant(&self) -> GqlConstantProperties {
        self.props.constant().into()
    }
}

#[ResolvedObjectFields]
impl GqlConstantProperties {
    async fn get(&self, key: String) -> Option<GqlProperty> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            self_clone
                .props
                .get(key.as_str())
                .map(|p| (key.to_string(), p).into())
        })
        .await
        .unwrap()
    }

    async fn contains(&self, key: String) -> bool {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.props.contains(key.as_str()))
            .await
            .unwrap()
    }

    async fn keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.props.keys().map(|k| k.clone().into()).collect())
            .await
            .unwrap()
    }

    pub(crate) async fn values(&self, keys: Option<Vec<String>>) -> Vec<GqlProperty> {
        let self_clone = self.clone();
        spawn_blocking(move || match keys {
            Some(keys) => self_clone
                .props
                .iter()
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
                .iter()
                .map(|(k, p)| (k.to_string(), p).into())
                .collect(),
        })
        .await
        .unwrap()
    }
}

#[ResolvedObjectFields]
impl GqlTemporalProperties {
    async fn get(&self, key: String) -> Option<GqlTemporalProperty> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            self_clone
                .props
                .get(key.as_str())
                .map(|p| (key.to_string(), p).into())
        })
        .await
        .unwrap()
    }

    async fn contains(&self, key: String) -> bool {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.props.contains(key.as_str()))
            .await
            .unwrap()
    }

    async fn keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.props.keys().map(|k| k.into()).collect())
            .await
            .unwrap()
    }

    async fn values(&self, keys: Option<Vec<String>>) -> Vec<GqlTemporalProperty> {
        let self_clone = self.clone();
        spawn_blocking(move || match keys {
            Some(keys) => self_clone
                .props
                .iter()
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
                .iter()
                .map(|(k, p)| (k.to_string(), p).into())
                .collect(),
        })
        .await
        .unwrap()
    }
}
