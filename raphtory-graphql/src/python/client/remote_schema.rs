use crate::client::remote_schema::{
    RemoteEdgeSchema, RemoteGraphSchema, RemoteLayerSchema, RemoteNodeSchema, RemotePropertySchema,
};
use pyo3::{pyclass, pymethods};

/// One property key on a node/edge type, with its observed property type
/// and (for string-valued properties) the set of distinct values seen.
#[derive(Clone)]
#[pyclass(
    name = "RemotePropertySchema",
    module = "raphtory.graphql",
    get_all,
    skip_from_py_object
)]
pub struct PyRemotePropertySchema {
    /// The property name.
    ///
    /// Returns:
    ///     str: the property name.
    pub key: String,
    /// The observed property type, as reported by the server.
    ///
    /// Returns:
    ///     str: the property type name.
    pub property_type: String,
    /// The distinct values seen for a string-valued property; empty otherwise.
    ///
    /// Returns:
    ///     list[str]: the distinct values seen.
    pub variants: Vec<String>,
}

impl From<RemotePropertySchema> for PyRemotePropertySchema {
    fn from(v: RemotePropertySchema) -> Self {
        Self {
            key: v.key,
            property_type: v.property_type,
            variants: v.variants,
        }
    }
}

#[pymethods]
impl PyRemotePropertySchema {
    fn __repr__(&self) -> String {
        format!(
            "RemotePropertySchema(key={:?}, property_type={:?}, variants={:?})",
            self.key, self.property_type, self.variants
        )
    }
}

/// Schema for edges between a specific `(src_type, dst_type)` pair.
#[derive(Clone)]
#[pyclass(
    name = "RemoteEdgeSchema",
    module = "raphtory.graphql",
    get_all,
    skip_from_py_object
)]
pub struct PyRemoteEdgeSchema {
    /// The node type of the edges' source endpoint.
    ///
    /// Returns:
    ///     str: the source node type.
    pub src_type: String,
    /// The node type of the edges' destination endpoint.
    ///
    /// Returns:
    ///     str: the destination node type.
    pub dst_type: String,
    /// The temporal property schemas observed on these edges.
    ///
    /// Returns:
    ///     list[RemotePropertySchema]: one entry per property key.
    pub properties: Vec<PyRemotePropertySchema>,
    /// The metadata schemas observed on these edges.
    ///
    /// Returns:
    ///     list[RemotePropertySchema]: one entry per metadata key.
    pub metadata: Vec<PyRemotePropertySchema>,
}

impl From<RemoteEdgeSchema> for PyRemoteEdgeSchema {
    fn from(v: RemoteEdgeSchema) -> Self {
        Self {
            src_type: v.src_type,
            dst_type: v.dst_type,
            properties: v.properties.into_iter().map(Into::into).collect(),
            metadata: v.metadata.into_iter().map(Into::into).collect(),
        }
    }
}

#[pymethods]
impl PyRemoteEdgeSchema {
    fn __repr__(&self) -> String {
        format!(
            "RemoteEdgeSchema(src_type={:?}, dst_type={:?}, properties=[...], metadata=[...])",
            self.src_type, self.dst_type
        )
    }
}

/// Schema for a single edge layer.
#[derive(Clone)]
#[pyclass(
    name = "RemoteLayerSchema",
    module = "raphtory.graphql",
    get_all,
    skip_from_py_object
)]
pub struct PyRemoteLayerSchema {
    /// The layer name.
    ///
    /// Returns:
    ///     str: the layer name.
    pub name: String,
    /// The edge schemas in this layer, one per `(src_type, dst_type)` pair.
    ///
    /// Returns:
    ///     list[RemoteEdgeSchema]: one entry per endpoint-type pair.
    pub edges: Vec<PyRemoteEdgeSchema>,
}

impl From<RemoteLayerSchema> for PyRemoteLayerSchema {
    fn from(v: RemoteLayerSchema) -> Self {
        Self {
            name: v.name,
            edges: v.edges.into_iter().map(Into::into).collect(),
        }
    }
}

#[pymethods]
impl PyRemoteLayerSchema {
    fn __repr__(&self) -> String {
        format!("RemoteLayerSchema(name={:?}, edges=[...])", self.name)
    }
}

/// Schema for nodes of a specific type.
#[derive(Clone)]
#[pyclass(
    name = "RemoteNodeSchema",
    module = "raphtory.graphql",
    get_all,
    skip_from_py_object
)]
pub struct PyRemoteNodeSchema {
    /// The node type these nodes share.
    ///
    /// Returns:
    ///     str: the node type name.
    pub type_name: String,
    /// The temporal property schemas observed on these nodes.
    ///
    /// Returns:
    ///     list[RemotePropertySchema]: one entry per property key.
    pub properties: Vec<PyRemotePropertySchema>,
    /// The metadata schemas observed on these nodes.
    ///
    /// Returns:
    ///     list[RemotePropertySchema]: one entry per metadata key.
    pub metadata: Vec<PyRemotePropertySchema>,
}

impl From<RemoteNodeSchema> for PyRemoteNodeSchema {
    fn from(v: RemoteNodeSchema) -> Self {
        Self {
            type_name: v.type_name,
            properties: v.properties.into_iter().map(Into::into).collect(),
            metadata: v.metadata.into_iter().map(Into::into).collect(),
        }
    }
}

#[pymethods]
impl PyRemoteNodeSchema {
    fn __repr__(&self) -> String {
        format!(
            "RemoteNodeSchema(type_name={:?}, properties=[...], metadata=[...])",
            self.type_name
        )
    }
}

/// The full schema of a remote graph — the tree of node types, edge
/// layers, and their observed property/metadata fields.
///
/// Returned by [RemoteGraph.schema][raphtory.graphql.RemoteGraph.schema].
#[derive(Clone)]
#[pyclass(
    name = "RemoteGraphSchema",
    module = "raphtory.graphql",
    get_all,
    skip_from_py_object
)]
pub struct PyRemoteGraphSchema {
    /// The per-node-type schemas in this graph.
    ///
    /// Returns:
    ///     list[RemoteNodeSchema]: one entry per node type.
    pub nodes: Vec<PyRemoteNodeSchema>,
    /// The per-layer edge schemas in this graph.
    ///
    /// Returns:
    ///     list[RemoteLayerSchema]: one entry per edge layer.
    pub layers: Vec<PyRemoteLayerSchema>,
}

impl From<RemoteGraphSchema> for PyRemoteGraphSchema {
    fn from(v: RemoteGraphSchema) -> Self {
        Self {
            nodes: v.nodes.into_iter().map(Into::into).collect(),
            layers: v.layers.into_iter().map(Into::into).collect(),
        }
    }
}

#[pymethods]
impl PyRemoteGraphSchema {
    fn __repr__(&self) -> String {
        format!(
            "RemoteGraphSchema(nodes=[{} types], layers=[{}])",
            self.nodes.len(),
            self.layers.len()
        )
    }
}
