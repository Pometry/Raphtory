use minijinja::{Environment, Value};
use pyo3::{pyclass, pymethods};
use raphtory::{
    core::{
        utils::{errors::GraphError, time::IntoTime},
        DocumentInput, Prop,
    },
    python::utils::PyTime,
};
use raphtory_api::core::entities::GID;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serde_json::json;
use std::collections::HashMap;

pub mod raphtory_client;
pub mod remote_edge;
pub mod remote_graph;
pub mod remote_node;

#[derive(Clone)]
#[pyclass(name = "RemoteUpdate", module = "raphtory.graphql")]
pub struct PyUpdate {
    time: PyTime,
    properties: Option<HashMap<String, Prop>>,
}

impl Serialize for PyUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut count = 1;
        if self.properties.is_some() {
            count += 1;
        }
        let mut state = serializer.serialize_struct("PyUpdate", count)?;

        let time = &self.time;
        let time = time.clone().into_time();
        state.serialize_field("time", &time)?;
        if let Some(ref properties) = self.properties {
            let properties_list: Vec<serde_json::Value> = properties
                .iter()
                .map(|(key, value)| {
                    json!({
                        "key": key,
                        "value": inner_collection(value),
                    })
                })
                .collect();
            state.serialize_field("properties", &properties_list)?;
        }

        state.end()
    }
}

#[pymethods]
impl PyUpdate {
    #[new]
    #[pyo3(signature = (time, properties=None))]
    pub(crate) fn new(time: PyTime, properties: Option<HashMap<String, Prop>>) -> Self {
        Self { time, properties }
    }
}
#[derive(Clone)]
#[pyclass(name = "RemoteNodeAddition", module = "raphtory.graphql")]
pub struct PyNodeAddition {
    name: GID,
    node_type: Option<String>,
    constant_properties: Option<HashMap<String, Prop>>,
    updates: Option<Vec<PyUpdate>>,
}

impl Serialize for PyNodeAddition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut count = 1;
        if self.node_type.is_some() {
            count += 1;
        }
        if self.constant_properties.is_some() {
            count += 1;
        }
        if self.updates.is_some() {
            count += 1;
        }
        let mut state = serializer.serialize_struct("PyNodeAddition", count)?;

        state.serialize_field("name", &self.name.to_string())?;

        if let Some(node_type) = &self.node_type {
            state.serialize_field("node_type", node_type)?;
        }

        if let Some(ref constant_properties) = self.constant_properties {
            let properties_list: Vec<serde_json::Value> = constant_properties
                .iter()
                .map(|(key, value)| {
                    json!({
                        "key": key,
                        "value": inner_collection(value),
                    })
                })
                .collect();
            state.serialize_field("constant_properties", &properties_list)?;
        }
        if let Some(updates) = &self.updates {
            state.serialize_field("updates", updates)?;
        }
        state.end()
    }
}

#[pymethods]
impl PyNodeAddition {
    #[new]
    #[pyo3(signature = (name, node_type=None, constant_properties=None, updates=None))]
    pub(crate) fn new(
        name: GID,
        node_type: Option<String>,
        constant_properties: Option<HashMap<String, Prop>>,
        updates: Option<Vec<PyUpdate>>,
    ) -> Self {
        Self {
            name,
            node_type,
            constant_properties,
            updates,
        }
    }
}

#[derive(Clone)]
#[pyclass(name = "RemoteEdgeAddition", module = "raphtory.graphql")]
pub struct PyEdgeAddition {
    src: GID,
    dst: GID,
    layer: Option<String>,
    constant_properties: Option<HashMap<String, Prop>>,
    updates: Option<Vec<PyUpdate>>,
}

impl Serialize for PyEdgeAddition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut count = 2;
        if self.layer.is_some() {
            count += 1;
        }
        if self.constant_properties.is_some() {
            count += 1;
        }
        if self.updates.is_some() {
            count += 1;
        }
        let mut state = serializer.serialize_struct("PyEdgeAddition", count)?;

        state.serialize_field("src", &self.src.to_string())?;
        state.serialize_field("dst", &self.dst.to_string())?;

        if let Some(layer) = &self.layer {
            state.serialize_field("layer", layer)?;
        }

        if let Some(ref constant_properties) = self.constant_properties {
            let properties_list: Vec<serde_json::Value> = constant_properties
                .iter()
                .map(|(key, value)| {
                    json!({
                        "key": key,
                        "value": inner_collection(value),
                    })
                })
                .collect();
            state.serialize_field("constant_properties", &properties_list)?;
        }
        if let Some(updates) = &self.updates {
            state.serialize_field("updates", updates)?;
        }
        state.end()
    }
}

#[pymethods]
impl PyEdgeAddition {
    #[new]
    #[pyo3(signature = (src, dst, layer=None, constant_properties=None, updates=None))]
    pub(crate) fn new(
        src: GID,
        dst: GID,
        layer: Option<String>,
        constant_properties: Option<HashMap<String, Prop>>,
        updates: Option<Vec<PyUpdate>>,
    ) -> Self {
        Self {
            src,
            dst,
            layer,
            constant_properties,
            updates,
        }
    }
}

fn inner_collection(value: &Prop) -> String {
    match value {
        Prop::Str(value) => format!("\"{}\"", value.to_string()),
        Prop::U8(value) => value.to_string(),
        Prop::U16(value) => value.to_string(),
        Prop::I32(value) => value.to_string(),
        Prop::I64(value) => value.to_string(),
        Prop::U32(value) => value.to_string(),
        Prop::U64(value) => value.to_string(),
        Prop::F32(value) => value.to_string(),
        Prop::F64(value) => value.to_string(),
        Prop::Bool(value) => value.to_string(),
        Prop::List(value) => {
            let vec: Vec<String> = value.iter().map(|v| inner_collection(v)).collect();
            format!("[{}]", vec.join(", "))
        }
        Prop::Array(value) => {
            let vec: Vec<_> = value.iter_prop().map(|v| inner_collection(&v)).collect();
            format!("[{}]", vec.join(", "))
        }
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| format!("{}:{}", k, inner_collection(v)))
                .collect();
            format!("{}{}{}", "{", properties_array.join(" "), "}")
        }
        Prop::DTime(value) => format!("\"{}\"", value.to_string()),
        Prop::NDTime(value) => format!("\"{}\"", value.to_string()),
        Prop::Document(DocumentInput { content, .. }) => content.to_owned().to_string(), // TODO: return Value::Object ??
    }
}

fn to_graphql_valid(key: &String, value: &Prop) -> String {
    match value {
        Prop::Str(value) => format!("{{ key: \"{}\", value: \"{}\" }}", key, value.to_string()),
        Prop::U8(value) => format!("{{ key: \"{}\", value: {} }}", key, value),
        Prop::U16(value) => format!("{{ key: \"{}\", value: {} }}", key, value),
        Prop::I32(value) => format!("{{ key: \"{}\", value: {} }}", key, value),
        Prop::I64(value) => format!("{{ key: \"{}\", value: {} }}", key, value),
        Prop::U32(value) => format!("{{ key: \"{}\", value: {} }}", key, value),
        Prop::U64(value) => format!("{{ key: \"{}\", value: {} }}", key, value),
        Prop::F32(value) => format!("{{ key: \"{}\", value: {} }}", key, value),
        Prop::F64(value) => format!("{{ key: \"{}\", value: {} }}", key, value),
        Prop::Bool(value) => format!("{{ key: \"{}\", value: {} }}", key, value),
        Prop::List(value) => {
            let vec: Vec<_> = value.iter().map(|v| inner_collection(v)).collect();
            format!("{{ key: \"{}\", value: [{}] }}", key, vec.join(", "))
        }
        Prop::Array(value) => {
            let vec: Vec<_> = value.iter_prop().map(|v| inner_collection(&v)).collect();
            format!("{{ key: \"{}\", value: [{}] }}", key, vec.join(", "))
        }
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| format!("{}:{}", k, inner_collection(v)))
                .collect();
            format!(
                "{}key:\"{}\",value:{}{}{}{}",
                "{",
                key,
                "{",
                properties_array.join(" "),
                "}",
                "}"
            )
        }
        Prop::DTime(value) => format!("{{ key: \"{}\", value: \"{}\" }}", key, value.to_string()),
        Prop::NDTime(value) => format!("{{ key: \"{}\", value: \"{}\" }}", key, value.to_string()),
        Prop::Document(_) => "Document cannot be converted to JSON".to_string(), // TODO: return Value::Object ??
    }
}

pub(crate) fn build_property_string(properties: HashMap<String, Prop>) -> String {
    let properties_array: Vec<String> = properties
        .iter()
        .map(|(k, v)| to_graphql_valid(k, v))
        .collect();

    format!("[{}]", properties_array.join(", "))
}

pub(crate) fn build_query(template: &str, context: Value) -> Result<String, GraphError> {
    let mut env = Environment::new();
    env.add_template("template", template)
        .map_err(|e| GraphError::JinjaError(e.to_string()))?;
    let query = env
        .get_template("template")
        .map_err(|e| GraphError::JinjaError(e.to_string()))?
        .render(context)
        .map_err(|e| GraphError::JinjaError(e.to_string()))?;
    Ok(query)
}
