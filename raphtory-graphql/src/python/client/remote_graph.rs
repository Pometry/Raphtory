use crate::python::client::{
    raphtory_client::PyRaphtoryClient, remote_edge::PyRemoteEdge, remote_node::PyRemoteNode,
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyResult};
use raphtory::{
    core::{utils::errors::GraphError, Prop},
    db::graph::{edge::EdgeView, views::deletion_graph::PersistentGraph},
    python::utils::PyTime,
};
use raphtory_api::core::entities::GID;
use std::collections::HashMap;

#[derive(Clone)]
#[pyclass(name = "RemoteGraph")]
pub struct PyRemoteGraph {
    pub(crate) path: String,
}

#[pymethods]
impl PyRemoteGraph {
    #[new]
    pub(crate) fn new(path: String) -> Self {
        Self { path }
    }

    /// Adds a new node with the given id and properties to the remote graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the node.
    ///    id (str or int): The id of the node.
    ///    properties (dict): The properties of the node (optional).
    ///    node_type (str): The optional string which will be used as a node type
    /// Returns:
    ///   the added node (RemoteNode)
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None))]
    pub fn add_node(
        &self,
        timestamp: PyTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
    ) -> Result<PyRemoteNode, GraphError> {
        Ok(PyRemoteNode::new(self.path.clone(), id.to_string()))
    }

    /// Adds properties to the remote graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the temporal property.
    ///    properties (dict): The temporal properties of the graph.
    ///
    /// Returns:
    ///    None
    pub fn add_property(
        &self,
        timestamp: PyTime,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        Ok(())
    }

    /// Adds static properties to the remote graph.
    ///
    /// Arguments:
    ///     properties (dict): The static properties of the graph.
    ///
    /// Returns:
    ///    None
    pub fn add_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        Ok(())
    }

    /// Updates static properties on the remote graph.
    ///
    /// Arguments:
    ///     properties (dict): The static properties of the graph.
    ///
    /// Returns:
    ///    None
    pub fn update_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        Ok(())
    }

    /// Adds a new edge with the given source and destination nodes and properties to the remote graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the edge.
    ///    src (str or int): The id of the source node.
    ///    dst (str or int): The id of the destination node.
    ///    properties (dict): The properties of the edge, as a dict of string and properties (optional).
    ///    layer (str): The layer of the edge (optional).
    ///
    /// Returns:
    ///   The added edge (RemoteEdge)
    #[pyo3(signature = (timestamp, src, dst, properties = None, layer = None))]
    pub fn add_edge(
        &self,
        timestamp: PyTime,
        src: GID,
        dst: GID,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<PyRemoteEdge, GraphError> {
        Ok(PyRemoteEdge::new(
            self.path.clone(),
            src.to_string(),
            dst.to_string(),
        ))
    }

    /// Deletes an edge in the remote graph, given the timestamp, src and dst nodes and layer (optional)
    ///
    /// Arguments:
    ///   timestamp (int): The timestamp of the edge.
    ///   src (str or int): The id of the source node.
    ///   dst (str or int): The id of the destination node.
    ///   layer (str): The layer of the edge. (optional)
    ///
    /// Returns:
    ///  The deleted edge (RemoteEdge)
    pub fn delete_edge(
        &self,
        timestamp: PyTime,
        src: GID,
        dst: GID,
        layer: Option<&str>,
    ) -> Result<PyRemoteEdge, GraphError> {
        Ok(PyRemoteEdge::new(
            self.path.clone(),
            src.to_string(),
            dst.to_string(),
        ))
    }
}
