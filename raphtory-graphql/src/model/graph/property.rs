use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue};
use raphtory::{
    core::Prop,
    db::api::properties::{
        dyn_props::{DynConstProperties, DynProperties, DynProps, DynTemporalProperties},
        TemporalPropertyView,
    },
};
use async_graphql::{Error, Value as GqlValue};
use serde_json::Value as JsonValue;
use serde_json::json;

#[derive(Clone, Debug, Scalar)]
pub struct GqlJson(JsonValue);

impl ScalarValue for GqlJson {
    fn from_value(value: GqlValue) -> Result<GqlJson, async_graphql::Error> {
        match value {
            GqlValue::Object(obj) => {
                let json_value: JsonValue = json!(obj);
                Ok(GqlJson(json_value))
            },
            _ => Err(async_graphql::Error::new("Unable to convert")),
        }
    }

    fn to_value(&self) -> GqlValue {
        match serde_json::to_string(&self.0) {
            Ok(str) => GqlValue::String(str),
            Err(_) => GqlValue::Null,
        }
    }
}

impl From<JsonValue> for GqlJson {
    fn from(value: JsonValue) -> Self {
        GqlJson(value)
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

    async fn to_json(&self) -> GqlJson {
        let json_value: JsonValue = self.prop.to_json();
        GqlJson::from(json_value)
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
