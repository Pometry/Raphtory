use crate::client::{inner_collection, ClientError};
use pyo3::{exceptions::PyValueError, prelude::*, pyclass, pymethods};
use raphtory_api::{
    core::{
        entities::{properties::prop::Prop, GID},
        storage::timeindex::EventTime,
        utils::time::IntoTime,
    },
    python::{error::adapt_err_value, timeindex::PyEventTime},
};
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
    time: PyEventTime,
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
    pub(crate) fn new(time: EventTime, properties: Option<HashMap<String, Prop>>) -> Self {
        Self {
            time: PyEventTime::new(time),
            properties,
        }
    }
}

/// Node addition update
///
/// Arguments:
///     name (GID): the id of the node
///     node_type (str, optional): the node type
///     metadata (PropInput, optional): the metadata
///     updates (list[RemoteUpdate], optional): the temporal updates
#[derive(Clone)]
#[pyclass(name = "RemoteNodeAddition", module = "raphtory.graphql")]
pub struct PyNodeAddition {
    name: GID,
    node_type: Option<String>,
    metadata: Option<HashMap<String, Prop>>,
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
        if self.metadata.is_some() {
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

        if let Some(ref metadata) = self.metadata {
            let properties_list: Vec<serde_json::Value> = metadata
                .iter()
                .map(|(key, value)| {
                    json!({
                        "key": key,
                        "value": inner_collection(value),
                    })
                })
                .collect();
            state.serialize_field("metadata", &properties_list)?;
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
    #[pyo3(signature = (name, node_type=None, metadata=None, updates=None))]
    pub(crate) fn new(
        name: GID,
        node_type: Option<String>,
        metadata: Option<HashMap<String, Prop>>,
        updates: Option<Vec<PyUpdate>>,
    ) -> Self {
        Self {
            name,
            node_type,
            metadata,
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
///     metadata (PropInput, optional): the metadata for the edge
///     updates (list[RemoteUpdate], optional): the temporal updates for the edge
#[derive(Clone)]
#[pyclass(name = "RemoteEdgeAddition", module = "raphtory.graphql")]
pub struct PyEdgeAddition {
    src: GID,
    dst: GID,
    layer: Option<String>,
    metadata: Option<HashMap<String, Prop>>,
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
        if self.metadata.is_some() {
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

        if let Some(ref metadata) = self.metadata {
            let properties_list: Vec<serde_json::Value> = metadata
                .iter()
                .map(|(key, value)| {
                    json!({
                        "key": key,
                        "value": inner_collection(value),
                    })
                })
                .collect();
            state.serialize_field("metadata", &properties_list)?;
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
    #[pyo3(signature = (src, dst, layer=None, metadata=None, updates=None))]
    pub(crate) fn new(
        src: GID,
        dst: GID,
        layer: Option<String>,
        metadata: Option<HashMap<String, Prop>>,
        updates: Option<Vec<PyUpdate>>,
    ) -> Self {
        Self {
            src,
            dst,
            layer,
            metadata,
            updates,
        }
    }
}

/// Specifies that **all** properties should be included when creating an index.
/// Use one of the predefined variants: ALL , ALL_METADATA , or ALL_TEMPORAL .
#[derive(Clone, Serialize, PartialEq)]
#[pyclass(name = "AllPropertySpec", module = "raphtory.graphql", eq, eq_int)]
pub enum PyAllPropertySpec {
    /// Include all properties (both metadata and temporal).
    #[serde(rename = "ALL")]
    All,
    /// Include only metadata.
    #[serde(rename = "ALL_METADATA")]
    AllMetadata,
    /// Include only temporal properties.
    #[serde(rename = "ALL_PROPERTIES")]
    AllProperties,
}

/// Create a SomePropertySpec by explicitly listing metadata and/or temporal property names.
///
/// Arguments:
///     metadata (list[str]): Metadata property names. Defaults to [].
///     properties (list[str]): Temporal property names. Defaults to [].
#[derive(Clone, Serialize)]
#[pyclass(name = "SomePropertySpec", module = "raphtory.graphql")]
pub struct PySomePropertySpec {
    /// Metadata property names to include in the index.
    pub metadata: Vec<String>,
    /// Temporal property names to include in the index.
    pub properties: Vec<String>,
}

#[pymethods]
impl PySomePropertySpec {
    #[new]
    #[pyo3(signature = (metadata = vec![], properties = vec![]))]
    fn new(metadata: Vec<String>, properties: Vec<String>) -> Self {
        Self {
            metadata,
            properties,
        }
    }
}

/// Create a PropsInput by choosing to include all/some properties explicitly.
///
/// Arguments:
///     all (AllPropertySpec, optional): Use a predefined spec to include all properties of a kind.
///     some (SomePropertySpec, optional): Explicitly list the properties to include.
///
/// Raises:
///     ValueError: If neither all and some are specified.
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

/// Create a RemoteIndexSpec specifying which node and edge properties to index.
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

// Takes care of the ClientError -> PyException conversion
impl From<ClientError> for PyErr {
    fn from(err: ClientError) -> Self {
        adapt_err_value(&err)
    }
}
