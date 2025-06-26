use minijinja::{Environment, Value};
use pyo3::{exceptions::PyValueError, prelude::*, pyclass, pymethods};
use raphtory::{core::utils::time::IntoTime, errors::GraphError, python::utils::PyTime};
use raphtory_api::core::entities::{properties::prop::Prop, GID};
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serde_json::json;
use std::collections::HashMap;

pub mod raphtory_client;
pub mod remote_edge;
pub mod remote_graph;
pub mod remote_node;

/// A temporal update
///
/// Arguments:
///     time (TimeInput): the timestamp for the update
///     properties (PropInput, optional): the properties for the update
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

/// Node addition update
///
/// Arguments:
///     name (GID): the id of the node
///     node_type (str, optional): the node type
///     constant_properties (PropInput, optional): the constant properties
///     updates: (list[RemoteUpdate], optional): the temporal updates
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

/// An edge update
///
/// Arguments:
///     src (GID): the id of the source node
///     dst (GID): the id of the destination node
///     layer (str, optional): the layer for the update
///     constant_properties (PropInput, optional): the constant properties for the edge
///     updates (list[RemoteUpdate], optional): the temporal updates for the edge
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
        Prop::Str(value) => format!("{{ str: \"{}\" }}", value),
        Prop::U8(value) => format!("{{ u64: {} }}", value),
        Prop::U16(value) => format!("{{ u64: {} }}", value),
        Prop::I32(value) => format!("{{ i64: {} }}", value),
        Prop::I64(value) => format!("{{ i64: {} }}", value),
        Prop::U32(value) => format!("{{ u64: {} }}", value),
        Prop::U64(value) => format!("{{ u64: {} }}", value),
        Prop::F32(value) => format!("{{ f64: {} }}", value),
        Prop::F64(value) => format!("{{ f64: {} }}", value),
        Prop::Bool(value) => format!("{{ bool: {} }}", value),
        Prop::List(value) => {
            let vec: Vec<String> = value.iter().map(inner_collection).collect();
            format!("{{ list: [{}] }}", vec.join(", "))
        }
        Prop::Array(value) => {
            let vec: Vec<String> = value.iter_prop().map(|v| inner_collection(&v)).collect();
            format!("{{ list: [{}] }}", vec.join(", "))
        }
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| format!("{{ key: \"{}\", value: {} }}", k, inner_collection(v)))
                .collect();
            format!("{{ object: [{}] }}", properties_array.join(", "))
        }
        Prop::DTime(value) => format!("{{ str: \"{}\" }}", value),
        Prop::NDTime(value) => format!("{{ str: \"{}\" }}", value),
        Prop::Decimal(value) => format!("{{ decimal: {} }}", value),
    }
}

fn to_graphql_valid(key: &String, value: &Prop) -> String {
    match value {
        Prop::Str(value) => format!("{{ key: \"{}\", value: {{ str: \"{}\" }} }}", key, value),
        Prop::U8(value) => format!("{{ key: \"{}\", value: {{ u64: {} }} }}", key, value),
        Prop::U16(value) => format!("{{ key: \"{}\", value: {{ u64: {} }} }}", key, value),
        Prop::I32(value) => format!("{{ key: \"{}\", value: {{ i64: {} }} }}", key, value),
        Prop::I64(value) => format!("{{ key: \"{}\", value: {{ i64: {} }} }}", key, value),
        Prop::U32(value) => format!("{{ key: \"{}\", value: {{ u64: {} }} }}", key, value),
        Prop::U64(value) => format!("{{ key: \"{}\", value: {{ u64: {} }} }}", key, value),
        Prop::F32(value) => format!("{{ key: \"{}\", value: {{ f64: {} }} }}", key, value),
        Prop::F64(value) => format!("{{ key: \"{}\", value: {{ f64: {} }} }}", key, value),
        Prop::Bool(value) => format!("{{ key: \"{}\", value: {{ bool: {} }} }}", key, value),
        Prop::List(value) => {
            let vec: Vec<String> = value.iter().map(inner_collection).collect();
            format!(
                "{{ key: \"{}\", value: {{ list: [{}] }} }}",
                key,
                vec.join(", ")
            )
        }
        Prop::Array(value) => {
            let vec: Vec<String> = value.iter_prop().map(|v| inner_collection(&v)).collect();
            format!(
                "{{ key: \"{}\", value: {{ list: [{}] }} }}",
                key,
                vec.join(", ")
            )
        }
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| format!("{{ key: \"{}\", value: {} }}", k, inner_collection(v)))
                .collect();
            format!(
                "{{ key: \"{}\", value: {{ object: [{}] }} }}",
                key,
                properties_array.join(", ")
            )
        }
        Prop::DTime(value) => format!("{{ key: \"{}\", value: {{ str: \"{}\" }} }}", key, value),
        Prop::NDTime(value) => format!("{{ key: \"{}\", value: {{ str: \"{}\" }} }}", key, value),
        Prop::Decimal(value) => format!(
            "{{ key: \"{}\", value: {{ decimal: \"{}\" }} }}",
            key, value
        ),
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

/// Specifies that **all** properties should be included when creating an index.
/// Use one of the predefined variants: `ALL`, `ALL_CONSTANT`, or `ALL_TEMPORAL`.
#[derive(Clone, Serialize)]
#[pyclass(name = "AllPropertySpec", module = "raphtory.graphql")]
pub enum PyAllPropertySpec {
    /// Include all properties (both constant and temporal).
    #[serde(rename = "ALL")]
    All,
    /// Include only constant properties.
    #[serde(rename = "ALL_CONSTANT")]
    AllConstant,
    /// Include only temporal properties.
    #[serde(rename = "ALL_TEMPORAL")]
    AllTemporal,
}

/// Create a `SomePropertySpec` by explicitly listing constant and/or temporal property names.
///
/// Arguments:
///     constant (List[str]): Constant property names. Defaults to `[]`.
///     temporal (List[str]): Temporal property names. Defaults to `[]`.
#[derive(Clone, Serialize)]
#[pyclass(name = "SomePropertySpec", module = "raphtory.graphql")]
pub struct PySomePropertySpec {
    /// Constant property names to include in the index.
    pub constant: Vec<String>,
    /// Temporal property names to include in the index.
    pub temporal: Vec<String>,
}

#[pymethods]
impl PySomePropertySpec {
    #[new]
    #[pyo3(signature = (constant = vec![], temporal = vec![]))]
    fn new(constant: Vec<String>, temporal: Vec<String>) -> Self {
        Self { constant, temporal }
    }
}

/// Create a `PropsInput` by choosing to include all/some properties explicitly.
///
/// Arguments:
///     all (AllPropertySpec, optional): Use a predefined spec to include all properties of a kind.
///     some (SomePropertySpec, optional): Explicitly list the properties to include.
///
/// Raises:
///     ValueError: If neither `all` and `some` are specified.
#[derive(Clone, Serialize)]
#[pyclass(name = "PropsInput", module = "raphtory.graphql")]
pub struct PyPropsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all: Option<PyAllPropertySpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub some: Option<PySomePropertySpec>,
}

#[pymethods]
impl PyPropsInput {
    #[new]
    #[pyo3(signature = (all=None, some=None))]
    fn new(all: Option<PyAllPropertySpec>, some: Option<PySomePropertySpec>) -> PyResult<Self> {
        if all.is_none() && some.is_none() {
            Err(PyValueError::new_err(
                "PropsInput must have exactly one of 'all' or 'some'",
            ))
        } else {
            Ok(Self { all, some })
        }
    }
}

/// Create a `RemoteIndexSpec` specifying which node and edge properties to index.
///
/// Arguments:
///     node_props (PropsInput): Property spec for nodes.
///     edge_props (PropsInput): Property spec for edges.
#[derive(Clone, Serialize)]
#[pyclass(name = "RemoteIndexSpec", module = "raphtory.graphql")]
pub struct PyRemoteIndexSpec {
    /// Property inclusion specification for nodes.
    #[serde(rename = "nodeProps")]
    pub node_props: PyPropsInput,
    /// Property inclusion specification for edges.
    #[serde(rename = "edgeProps")]
    pub edge_props: PyPropsInput,
}

#[pymethods]
impl PyRemoteIndexSpec {
    #[new]
    #[pyo3(signature = (node_props, edge_props))]
    fn new(node_props: PyPropsInput, edge_props: PyPropsInput) -> Self {
        Self {
            node_props,
            edge_props,
        }
    }
}
