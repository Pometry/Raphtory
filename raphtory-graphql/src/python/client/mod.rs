use minijinja::{Environment, Value};
use pyo3::{pyclass, pymethods};
use raphtory::{
    core::{
        utils::{errors::GraphError, time::IntoTime},
        Prop,
    },
    python::utils::PyTime,
};
use raphtory_api::core::entities::GID;
use serde::{ser::SerializeStruct, Serialize, Serializer};
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
            let properties_list = properties
                .iter()
                .filter_map(|(key, value)| to_json_value(key, value).ok())
                .collect::<Vec<_>>();
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
                .filter_map(|(key, value)| to_json_value(key, value).ok())
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
#[pyclass(name = "RemoteEdgeAddition")]
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
                .filter_map(|(key, value)| to_json_value(key, value).ok())
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

#[derive(Serialize)]
struct Pair<'a> {
    key: &'a str,
    value: &'a Prop,
}

fn to_json_value(key: &str, value: &Prop) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(Pair { key, value })
}

fn to_graphql_valid(key: &String, value: &Prop) -> String {
    serde_json::to_string(&Pair { key, value }).unwrap()
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
