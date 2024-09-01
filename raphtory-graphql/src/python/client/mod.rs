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
#[pyclass(name = "RemoteUpdate")]
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
                        "value": to_graphql_batch_valid(value),
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
    pub(crate) fn new(time: PyTime, properties: Option<HashMap<String, Prop>>) -> Self {
        Self { time, properties }
    }
}
#[derive(Clone)]
#[pyclass(name = "RemoteNodeAddition")]
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
                        "value": to_graphql_batch_valid(value),
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
#[pyclass(name = "RemoteEdgeAddition")]
pub struct PyEdgeAddition {
    src: GID,
    dst: GID,
    layer: Option<String>,
    constant_properties: Option<HashMap<String, Prop>>,
    updates: Option<Vec<PyUpdate>>,
}

#[pymethods]
impl PyEdgeAddition {
    #[new]
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
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| format!("{}:{}", k, inner_collection(v)))
                .collect();
            format!("{}{}{}", "{", properties_array.join(" "), "}")
        }
        Prop::DTime(value) => format!("\"{}\"", value.to_string()),
        Prop::NDTime(value) => format!("\"{}\"", value.to_string()),
        Prop::Graph(_) => "Graph cannot be converted to JSON".to_string(),
        Prop::PersistentGraph(_) => "Persistent Graph cannot be converted to JSON".to_string(),
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
            let vec: Vec<String> = value.iter().map(|v| inner_collection(v)).collect();
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
        Prop::Graph(_) => "Graph cannot be converted to JSON".to_string(),
        Prop::PersistentGraph(_) => "Persistent Graph cannot be converted to JSON".to_string(),
        Prop::Document(DocumentInput { content, .. }) => {
            "Document cannot be converted to JSON".to_string()
        } // TODO: return Value::Object ??
    }
}

fn to_graphql_batch_valid(prop: &Prop) -> serde_json::Value {
    match prop {
        Prop::Str(value) => {
            serde_json::Value::String(format!("{}{}{}", "\"", value.to_string(), "\""))
        }
        Prop::U8(value) => serde_json::Value::Number((*value).into()),
        Prop::U16(value) => serde_json::Value::Number((*value).into()),
        Prop::I32(value) => serde_json::Value::Number((*value).into()),
        Prop::I64(value) => serde_json::Value::Number((*value).into()),
        Prop::U32(value) => serde_json::Value::Number((*value).into()),
        Prop::U64(value) => serde_json::Value::Number((*value).into()),
        Prop::F32(value) => {
            serde_json::Value::Number(serde_json::Number::from_f64(*value as f64).unwrap())
        }
        Prop::F64(value) => {
            serde_json::Value::Number(serde_json::Number::from_f64(*value).unwrap())
        }
        Prop::Bool(value) => serde_json::Value::Bool(*value),
        Prop::List(value) => {
            let vec: Vec<serde_json::Value> =
                value.iter().map(|v| to_graphql_batch_valid(v)).collect();
            serde_json::Value::Array(vec)
        }
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| format!("{}:{}", k, to_graphql_batch_valid(v)))
                .collect();
            serde_json::Value::String(format!("{}{}{}", "{", properties_array.join(", "), "}"))
        }
        Prop::DTime(value) => {
            serde_json::Value::String(format!("{}{}{}", "\"", value.to_string(), "\""))
        }
        Prop::NDTime(value) => {
            serde_json::Value::String(format!("{}{}{}", "\"", value.to_string(), "\""))
        }
        Prop::Graph(_) => {
            serde_json::Value::String("Graph cannot be converted to JSON".to_string())
        }
        Prop::PersistentGraph(_) => {
            serde_json::Value::String("Persistent Graph cannot be converted to JSON".to_string())
        }
        Prop::Document(DocumentInput { content, .. }) => {
            serde_json::Value::String(content.to_owned())
        } // TODO: return Value::Object ??
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
