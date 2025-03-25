use async_graphql::{registry::MetaType, Error, Name, Value as GqlValue};
use dynamic_graphql::{
    internal::{GetOutputTypeRef, InputObject, Register, Registry, Resolve},
    Enum, InputObject, ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue,
};
use itertools::Itertools;
use raphtory::{
    core::{utils::errors::GraphError, IntoPropMap, Prop},
    db::api::properties::{
        dyn_props::{DynConstProperties, DynProperties, DynProps, DynTemporalProperties},
        TemporalPropertyView,
    },
};
use raphtory_api::core::storage::arc_str::ArcStr;
use rustc_hash::FxHashMap;
use serde_json::Number;
use std::{collections::HashMap, convert::TryFrom, sync::Arc};

#[derive(InputObject, Clone, Debug, Default)]
pub struct ObjectEntry {
    pub key: String,
    pub value: Value,
}

#[derive(InputObject, Clone, Debug, Default)]
pub struct Value {
    pub u64: Option<u64>,
    pub i64: Option<i64>,
    pub f64: Option<f64>,
    pub str: Option<String>,
    pub bool: Option<bool>,
    pub list: Option<Vec<Value>>,
    pub object: Option<Vec<ObjectEntry>>,
}

impl TryFrom<Value> for Prop {
    type Error = GraphError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        value_to_prop(value)
    }
}

fn value_to_prop(value: Value) -> Result<Prop, GraphError> {
    if let Some(n) = value.u64 {
        return Ok(Prop::U64(n));
    }
    if let Some(n) = value.i64 {
        return Ok(Prop::I64(n));
    }
    if let Some(n) = value.f64 {
        return Ok(Prop::F64(n));
    }
    if let Some(s) = value.str {
        return Ok(Prop::Str(s.into()));
    }
    if let Some(b) = value.bool {
        return Ok(Prop::Bool(b));
    }
    if let Some(list) = value.list {
        let prop_list: Vec<Prop> = list
            .into_iter()
            .map(value_to_prop)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Prop::List(prop_list.into()));
    }
    if let Some(object) = value.object {
        let prop_map: FxHashMap<ArcStr, Prop> = object
            .into_iter()
            .map(|oe| Ok::<_, GraphError>((ArcStr::from(oe.key), value_to_prop(oe.value)?)))
            .collect::<Result<FxHashMap<_, _>, _>>()?;
        return Ok(Prop::Map(Arc::new(prop_map)));
    }
    Err(GraphError::EmptyValue)
}

#[derive(Clone, Debug, Scalar)]
pub struct GqlPropOutputVal(pub Prop);

impl ScalarValue for GqlPropOutputVal {
    fn from_value(value: GqlValue) -> Result<GqlPropOutputVal, Error> {
        Ok(GqlPropOutputVal(gql_to_prop(value)?))
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
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlProp {
    key: String,
    prop: Prop,
}

impl GqlProp {
    pub(crate) fn new(key: String, prop: Prop) -> Self {
        Self { key, prop }
    }
}

impl From<(String, Prop)> for GqlProp {
    fn from(value: (String, Prop)) -> Self {
        GqlProp::new(value.0, value.1)
    }
}

#[ResolvedObjectFields]
impl GqlProp {
    async fn key(&self) -> String {
        self.key.clone()
    }

    async fn as_string(&self) -> String {
        self.prop.to_string()
    }

    async fn value(&self) -> GqlPropOutputVal {
        GqlPropOutputVal(self.prop.clone())
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlPropTuple {
    time: i64,
    prop: Prop,
}

impl GqlPropTuple {
    pub(crate) fn new(time: i64, prop: Prop) -> Self {
        Self { time, prop }
    }
}

impl From<(i64, Prop)> for GqlPropTuple {
    fn from(value: (i64, Prop)) -> Self {
        GqlPropTuple::new(value.0, value.1)
    }
}

#[ResolvedObjectFields]
impl GqlPropTuple {
    async fn time(&self) -> i64 {
        self.time
    }

    async fn as_string(&self) -> String {
        self.prop.to_string()
    }

    async fn value(&self) -> GqlPropOutputVal {
        GqlPropOutputVal(self.prop.clone())
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlTemporalProp {
    key: String,
    prop: TemporalPropertyView<DynProps>,
}
impl GqlTemporalProp {
    pub(crate) fn new(key: String, prop: TemporalPropertyView<DynProps>) -> Self {
        Self { key, prop }
    }
}
impl From<(String, TemporalPropertyView<DynProps>)> for GqlTemporalProp {
    fn from(value: (String, TemporalPropertyView<DynProps>)) -> Self {
        GqlTemporalProp::new(value.0, value.1)
    }
}

#[ResolvedObjectFields]
impl GqlTemporalProp {
    async fn key(&self) -> String {
        self.key.clone()
    }

    async fn history(&self) -> Vec<i64> {
        self.prop.history().collect()
    }

    async fn values(&self) -> Vec<String> {
        self.prop.values().map(|x| x.to_string()).collect()
    }

    async fn at(&self, t: i64) -> Option<String> {
        self.prop.at(t).map(|x| x.to_string())
    }

    async fn latest(&self) -> Option<String> {
        self.prop.latest().map(|x| x.to_string())
    }

    async fn unique(&self) -> Vec<String> {
        self.prop
            .unique()
            .into_iter()
            .map(|x| x.to_string())
            .collect_vec()
    }

    async fn ordered_dedupe(&self, latest_time: bool) -> Vec<GqlPropTuple> {
        self.prop
            .ordered_dedupe(latest_time)
            .into_iter()
            .map(|(k, p)| (k, p).into())
            .collect()
    }
}

#[derive(ResolvedObject)]
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

#[derive(ResolvedObject)]
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

#[derive(ResolvedObject)]
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
    async fn get(&self, key: &str) -> Option<GqlProp> {
        self.props.get(key).map(|p| (key.to_string(), p).into())
    }

    async fn contains(&self, key: &str) -> bool {
        self.props.contains(key)
    }

    async fn keys(&self) -> Vec<String> {
        self.props.keys().map(|k| k.into()).collect()
    }

    async fn values(&self, keys: Option<Vec<String>>) -> Vec<GqlProp> {
        match keys {
            Some(keys) => self
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
            None => self
                .props
                .iter()
                .filter_map(|(k, p)| p.map(|prop| (k.to_string(), prop).into()))
                .collect(),
        }
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
    async fn get(&self, key: &str) -> Option<GqlProp> {
        self.props.get(key).map(|p| (key.to_string(), p).into())
    }

    async fn contains(&self, key: &str) -> bool {
        self.props.contains(key)
    }

    async fn keys(&self) -> Vec<String> {
        self.props.keys().map(|k| k.clone().into()).collect()
    }

    async fn values(&self, keys: Option<Vec<String>>) -> Vec<GqlProp> {
        match keys {
            Some(keys) => self
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
            None => self
                .props
                .iter()
                .map(|(k, p)| (k.to_string(), p).into())
                .collect(),
        }
    }
}

#[ResolvedObjectFields]
impl GqlTemporalProperties {
    async fn get(&self, key: &str) -> Option<GqlTemporalProp> {
        self.props.get(key).map(|p| (key.to_string(), p).into())
    }

    async fn contains(&self, key: &str) -> bool {
        self.props.contains(key)
    }

    async fn keys(&self) -> Vec<String> {
        self.props.keys().map(|k| k.into()).collect()
    }

    async fn values(&self, keys: Option<Vec<String>>) -> Vec<GqlTemporalProp> {
        match keys {
            Some(keys) => self
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
            None => self
                .props
                .iter()
                .map(|(k, p)| (k.to_string(), p).into())
                .collect(),
        }
    }
}
