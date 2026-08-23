use crate::{
    client::{
        op::{EdgeAddition, NodeAddition, TemporalUpdate},
        ClientError,
    },
    python::pymodule::RemotePermissionError,
};
use pyo3::{exceptions::PyValueError, prelude::*, pyclass, pymethods};
use raphtory::{
    db::graph::views::filter::model::FilterTree, errors::GraphError,
    python::filter::filter_expr::PyFilterExpr,
};
use raphtory_api::{
    core::{
        entities::{properties::prop::Prop, GID},
        storage::timeindex::{AsTime, EventTime},
        utils::time::IntoTime,
    },
    python::{error::adapt_err_value, timeindex::PyEventTime},
};
use serde::Serialize;
use std::collections::HashMap;

pub mod remote_client;
pub mod remote_collection_metadata;
pub mod remote_edge;
pub mod remote_edges;
pub mod remote_graph;
pub mod remote_history;
pub mod remote_metadata;
pub mod remote_nested_edges;
pub mod remote_node;
pub mod remote_nodes;
pub mod remote_path_from_graph;
pub mod remote_path_from_node;
pub mod remote_schema;
pub(crate) mod view_ops;

/// Convert a node-collection `select` / `nodes[expr]` subscript into the
/// filter tree that narrows the collection's membership. Node predicates,
/// graph views, and their combinations all pass through; the server applies
/// them with select (membership-narrowing) semantics.
///
/// An expression that tests edges is refused eagerly with the same error the
/// local `Nodes.__getitem__` raises, so one `except` clause catches it on
/// either backend — and at the same moment: locally the rejection happens at
/// subscript time, not at first read.
pub(crate) fn node_subscript(filter: &PyFilterExpr) -> PyResult<FilterTree> {
    let tree = filter
        .try_as_filter_tree()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    if tree.tests_edges() {
        return Err(adapt_err_value(&GraphError::NotNodeFilter));
    }
    Ok(tree)
}

/// A temporal update
///
/// Arguments:
///     time (TimeInput): the timestamp for the update
///     properties (PropInput, optional): the properties for the update
#[derive(Clone)]
#[pyclass(name = "RemoteUpdate", module = "raphtory.graphql", from_py_object)]
pub struct PyUpdate {
    time: PyEventTime,
    properties: Option<HashMap<String, Prop>>,
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
#[pyclass(
    name = "RemoteNodeAddition",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyNodeAddition {
    name: GID,
    node_type: Option<String>,
    metadata: Option<HashMap<String, Prop>>,
    updates: Option<Vec<PyUpdate>>,
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
#[pyclass(
    name = "RemoteEdgeAddition",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyEdgeAddition {
    src: GID,
    dst: GID,
    layer: Option<String>,
    metadata: Option<HashMap<String, Prop>>,
    updates: Option<Vec<PyUpdate>>,
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
/// Use one of the predefined variants: `All`, `AllMetadata`, or `AllProperties`.
#[derive(Clone, Serialize, PartialEq)]
#[pyclass(
    name = "AllPropertySpec",
    module = "raphtory.graphql",
    eq,
    eq_int,
    from_py_object
)]
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
#[pyclass(name = "SomePropertySpec", module = "raphtory.graphql", from_py_object)]
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
#[pyclass(name = "PropsInput", module = "raphtory.graphql", from_py_object)]
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
#[pyclass(name = "RemoteIndexSpec", module = "raphtory.graphql", from_py_object)]
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

// Takes care of the ClientError -> PyException conversion.
// A permission denial maps to the distinct `RemotePermissionError` type so
// callers can catch it specifically; everything else (including a missing graph)
// stays a generic exception.
impl From<ClientError> for PyErr {
    fn from(err: ClientError) -> Self {
        match &err {
            ClientError::PermissionDenied(msg) => RemotePermissionError::new_err(msg.clone()),
            _ => adapt_err_value(&err),
        }
    }
}

// ============ Py* → transport-layer op-arg conversions ============
// Used by the batch `add_nodes` / `add_edges` mutations to hand off the
// Python-supplied input to `WriteOp::AddNodes` / `WriteOp::AddEdges`.

impl From<PyUpdate> for TemporalUpdate {
    fn from(u: PyUpdate) -> Self {
        Self {
            time: u.time.into_time().t(),
            properties: u.properties,
        }
    }
}

impl From<PyNodeAddition> for NodeAddition {
    fn from(n: PyNodeAddition) -> Self {
        Self {
            name: n.name,
            node_type: n.node_type,
            metadata: n.metadata,
            updates: n.updates.map(|us| us.into_iter().map(Into::into).collect()),
        }
    }
}

impl From<PyEdgeAddition> for EdgeAddition {
    fn from(e: PyEdgeAddition) -> Self {
        Self {
            src: e.src,
            dst: e.dst,
            layer: e.layer,
            metadata: e.metadata,
            updates: e.updates.map(|us| us.into_iter().map(Into::into).collect()),
        }
    }
}
