use async_graphql::{Error, Name, Value as GqlValue};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue};
use itertools::Itertools;
use raphtory::{
    core::{IntoPropMap, Prop},
    db::api::properties::{
        dyn_props::{DynConstProperties, DynProperties, DynProps, DynTemporalProperties},
        TemporalPropertyView,
    },
};
use serde_json::Number;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Scalar)]
pub struct GqlPropValue(pub Prop);

impl ScalarValue for GqlPropValue {
    fn from_value(value: GqlValue) -> Result<GqlPropValue, Error> {
        Ok(GqlPropValue(gql_to_prop(value)?))
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
        Prop::F32(u) => GqlValue::Number(Number::from_f64(*u as f64).unwrap()),
        Prop::F64(u) => GqlValue::Number(Number::from_f64(*u).unwrap()),
        Prop::Bool(b) => GqlValue::Boolean(*b),
        Prop::List(l) => GqlValue::List(l.iter().map(|pp| prop_to_gql(pp)).collect()),
        Prop::Map(m) => GqlValue::Object(
            m.iter()
                .map(|(k, v)| (Name::new(k.to_string()), prop_to_gql(v)))
                .collect(),
        ),
        Prop::DTime(t) => GqlValue::Number(t.timestamp_millis().into()),
        Prop::NDTime(t) => GqlValue::Number(t.timestamp_millis().into()),
        Prop::Graph(g) => GqlValue::String(g.to_string()),
        Prop::PersistentGraph(g) => GqlValue::String(g.to_string()),
        Prop::Document(d) => GqlValue::String(d.content.to_owned()), // TODO: return GqlValue::Object ??
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

    async fn value(&self) -> GqlPropValue {
        GqlPropValue(self.prop.clone())
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
        self.prop.history()
    }
    async fn values(&self) -> Vec<String> {
        self.prop.values().iter().map(|x| x.to_string()).collect()
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

    async fn values(&self) -> Vec<GqlProp> {
        self.props
            .iter()
            .map(|(k, p)| (k.to_string(), p).into())
            .collect()
    }

    async fn temporal(&self) -> GqlTemporalProperties {
        self.props.temporal().into()
    }

    pub fn constant(&self) -> GqlConstantProperties {
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
        self.props.keys().iter().map(|k| k.clone().into()).collect()
    }

    async fn values(&self) -> Vec<GqlProp> {
        self.props
            .iter()
            .map(|(k, p)| (k.to_string(), p).into())
            .collect()
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
    async fn values(&self) -> Vec<GqlTemporalProp> {
        self.props
            .iter()
            .map(|(k, p)| (k.to_string(), p).into())
            .collect()
    }
}
