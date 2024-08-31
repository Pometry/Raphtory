use minijinja::{Environment, Value};
use pyo3::pyclass;
use raphtory::{
    core::{utils::errors::GraphError, Prop},
    python::utils::PyTime,
};
use raphtory_api::core::entities::GID;
use serde::Serialize;
use std::collections::HashMap;

pub mod raphtory_client;
pub mod remote_edge;
pub mod remote_graph;
pub mod remote_node;

#[derive(Clone, Serialize)]
#[pyclass(name = "RemoteUpdate")]
pub struct PyUpdate {
    time: PyTime,
    properties: Option<HashMap<String, Prop>>,
}
#[derive(Clone, Serialize)]
#[pyclass(name = "RemoteNodeAddition")]
pub struct PyNodeAddition {
    name: GID,
    node_type: Option<String>,
    constant_properties: Option<HashMap<String, Prop>>,
    updates: Option<Vec<PyUpdate>>,
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

pub(crate) fn build_property_string(properties: HashMap<String, Prop>) -> String {
    let properties_array: Vec<String> = properties
        .iter()
        .map(|(k, v)| format!("{{ key: \"{}\", value: {} }}", k, v.to_graphql_valid()))
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
