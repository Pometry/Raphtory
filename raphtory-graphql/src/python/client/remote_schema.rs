use crate::client::remote_schema::{
    RemoteEdgeSchema, RemoteGraphSchema, RemoteLayerSchema, RemoteNodeSchema, RemotePropertySchema,
};
use pyo3::{pyclass, pymethods};

/// One property key on a node/edge type, with its observed property type
/// and (for string-valued properties) the set of distinct values seen.
#[derive(Clone)]
#[pyclass(name = "RemotePropertySchema", module = "raphtory.graphql", get_all)]
pub struct PyRemotePropertySchema {
    pub key: String,
    pub property_type: String,
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
#[pyclass(name = "RemoteEdgeSchema", module = "raphtory.graphql", get_all)]
pub struct PyRemoteEdgeSchema {
    pub src_type: String,
    pub dst_type: String,
    pub properties: Vec<PyRemotePropertySchema>,
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
#[pyclass(name = "RemoteLayerSchema", module = "raphtory.graphql", get_all)]
pub struct PyRemoteLayerSchema {
    pub name: String,
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
#[pyclass(name = "RemoteNodeSchema", module = "raphtory.graphql", get_all)]
pub struct PyRemoteNodeSchema {
    pub type_name: String,
    pub properties: Vec<PyRemotePropertySchema>,
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
#[pyclass(name = "RemoteGraphSchema", module = "raphtory.graphql", get_all)]
pub struct PyRemoteGraphSchema {
    pub nodes: Vec<PyRemoteNodeSchema>,
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
