//! Defines the `GraphWithDeletions` class, which represents a raphtory graph in memory.
//! Unlike in the `Graph` which has event semantics, `GraphWithDeletions` has edges that persist until explicitly deleted.
//!
//! This is the base class used to create a temporal graph, add nodes and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//! In Python, this class wraps around the rust graph.
use crate::{
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError, Prop},
    db::{
        api::{
            mutation::{AdditionOps, PropertyAdditionOps},
            view::internal::MaterializedGraph,
        },
        graph::{edge::EdgeView, node::NodeView, views::deletion_graph::GraphWithDeletions},
    },
    prelude::{DeletionOps, Graph, GraphViewOps, ImportOps},
    python::{
        graph::{edge::PyEdge, node::PyNode, views::graph_view::PyGraphView},
        utils::{PyInputNode, PyTime},
    },
};
use pyo3::{prelude::*, types::PyBytes};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
};

/// A temporal graph that allows edges and nodes to be deleted.
#[derive(Clone)]
#[pyclass(name="GraphWithDeletions", extends=PyGraphView)]
pub struct PyGraphWithDeletions {
    pub(crate) graph: GraphWithDeletions,
}

impl Debug for PyGraphWithDeletions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.graph)
    }
}

impl From<GraphWithDeletions> for PyGraphWithDeletions {
    fn from(value: GraphWithDeletions) -> Self {
        Self { graph: value }
    }
}

impl IntoPy<PyObject> for GraphWithDeletions {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(
            py,
            (
                PyGraphWithDeletions::from(self.clone()),
                PyGraphView::from(self),
            ),
        )
        .unwrap() // I think this only fails if we are out of memory? Seems to be unavoidable if we want to create an actual graph.
        .into_py(py)
    }
}

impl PyGraphWithDeletions {
    pub fn py_from_db_graph(db_graph: GraphWithDeletions) -> PyResult<Py<PyGraphWithDeletions>> {
        Python::with_gil(|py| {
            Py::new(
                py,
                (
                    PyGraphWithDeletions::from(db_graph.clone()),
                    PyGraphView::from(db_graph),
                ),
            )
        })
    }
}

/// A temporal graph that allows edges and nodes to be deleted.
#[pymethods]
impl PyGraphWithDeletions {
    #[new]
    pub fn py_new() -> (Self, PyGraphView) {
        let graph = GraphWithDeletions::new();
        (
            Self {
                graph: graph.clone(),
            },
            PyGraphView::from(graph),
        )
    }

    /// Adds a new node with the given id and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the node.
    ///    id (str or int): The id of the node.
    ///    properties (dict): The properties of the node.
    ///
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, id, properties=None))]
    pub fn add_node(
        &self,
        timestamp: PyTime,
        id: PyInputNode,
        properties: Option<HashMap<String, Prop>>,
    ) -> Result<NodeView<GraphWithDeletions>, GraphError> {
        self.graph
            .add_node(timestamp, id, properties.unwrap_or_default())
    }

    /// Adds properties to the graph.
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
        self.graph.add_properties(timestamp, properties)
    }

    /// Adds static properties to the graph.
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
        self.graph.add_constant_properties(properties)
    }

    /// Updates static properties to the graph.
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
        self.graph.update_constant_properties(properties)
    }

    /// Adds a new edge with the given source and destination nodes and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int): The timestamp of the edge.
    ///    src (str or int): The id of the source node.
    ///    dst (str or int): The id of the destination node.
    ///    properties (dict): The properties of the edge, as a dict of string and properties
    ///    layer (str): The layer of the edge.
    ///
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, src, dst, properties=None, layer=None))]
    pub fn add_edge(
        &self,
        timestamp: PyTime,
        src: PyInputNode,
        dst: PyInputNode,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<EdgeView<GraphWithDeletions, GraphWithDeletions>, GraphError> {
        self.graph
            .add_edge(timestamp, src, dst, properties.unwrap_or_default(), layer)
    }

    /// Deletes an edge given the timestamp, src and dst nodes and layer (optional)
    ///
    /// Arguments:
    ///   timestamp (int): The timestamp of the edge.
    ///   src (str or int): The id of the source node.
    ///   dst (str or int): The id of the destination node.
    ///   layer (str): The layer of the edge. (optional)
    ///
    /// Returns:
    ///  None or a GraphError if the edge could not be deleted
    pub fn delete_edge(
        &self,
        timestamp: PyTime,
        src: PyInputNode,
        dst: PyInputNode,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.graph.delete_edge(timestamp, src, dst, layer)
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the node with the specified id
    ///
    /// Arguments:
    ///   id (str or int): the node id
    ///
    /// Returns:
    ///   the node with the specified id, or None if the node does not exist
    pub fn node(&self, id: NodeRef) -> Option<NodeView<GraphWithDeletions>> {
        self.graph.node(id)
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the edge with the specified source and destination nodes
    ///
    /// Arguments:
    ///     src (str or int): the source node id
    ///     dst (str or int): the destination node id
    ///
    /// Returns:
    ///     the edge with the specified source and destination nodes, or None if the edge does not exist
    #[pyo3(signature = (src, dst))]
    pub fn edge(
        &self,
        src: NodeRef,
        dst: NodeRef,
    ) -> Option<EdgeView<GraphWithDeletions, GraphWithDeletions>> {
        self.graph.edge(src, dst)
    }

    /// Import a single node into the graph.
    ///
    /// This function takes a PyNode object and an optional boolean flag. If the flag is set to true,
    /// the function will force the import of the node even if it already exists in the graph.
    ///
    /// Arguments:
    ///     node (Node) - A PyNode object representing the node to be imported.
    ///     force (boolean) - An optional boolean flag indicating whether to force the import of the node.
    ///
    /// Returns:
    ///     Result<NodeView<Graph, Graph>, GraphError> - A Result object which is Ok if the node was successfully imported, and Err otherwise.
    #[pyo3(signature = (node, force=false))]
    pub fn import_node(
        &self,
        node: PyNode,
        force: Option<bool>,
    ) -> Result<NodeView<GraphWithDeletions, GraphWithDeletions>, GraphError> {
        self.graph.import_node(&node.node, force)
    }

    /// Import multiple nodes into the graph.
    ///
    /// This function takes a vector of PyNode objects and an optional boolean flag. If the flag is set to true,
    /// the function will force the import of the nodes even if they already exist in the graph.
    ///
    /// Arguments:
    ///
    ///     nodes (List(Node))- A vector of PyNode objects representing the nodes to be imported.
    ///     force (boolean) - An optional boolean flag indicating whether to force the import of the nodes.
    ///
    /// Returns:
    ///     Result<List(NodeView<Graph, Graph>), GraphError> - A Result object which is Ok if the nodes were successfully imported, and Err otherwise.
    #[pyo3(signature = (nodes, force=false))]
    pub fn import_nodes(
        &self,
        nodes: Vec<PyNode>,
        force: Option<bool>,
    ) -> Result<Vec<NodeView<GraphWithDeletions, GraphWithDeletions>>, GraphError> {
        let nodeviews = nodes.iter().map(|node| &node.node).collect();
        self.graph.import_nodes(nodeviews, force)
    }

    /// Import a single edge into the graph.
    ///
    /// This function takes a PyEdge object and an optional boolean flag. If the flag is set to true,
    /// the function will force the import of the edge even if it already exists in the graph.
    ///
    /// Arguments:
    ///
    ///     edge (Edge) - A PyEdge object representing the edge to be imported.
    ///     force (boolean) - An optional boolean flag indicating whether to force the import of the edge.
    ///
    /// Returns:
    ///     Result<EdgeView<Graph, Graph>, GraphError> - A Result object which is Ok if the edge was successfully imported, and Err otherwise.
    #[pyo3(signature = (edge, force=false))]
    pub fn import_edge(
        &self,
        edge: PyEdge,
        force: Option<bool>,
    ) -> Result<EdgeView<GraphWithDeletions, GraphWithDeletions>, GraphError> {
        self.graph.import_edge(&edge.edge, force)
    }

    /// Import multiple edges into the graph.
    ///
    /// This function takes a vector of PyEdge objects and an optional boolean flag. If the flag is set to true,
    /// the function will force the import of the edges even if they already exist in the graph.
    ///
    /// Arguments:
    ///
    ///     edges (List(edges)) - A vector of PyEdge objects representing the edges to be imported.
    ///     force (boolean) - An optional boolean flag indicating whether to force the import of the edges.
    ///
    /// Returns:
    ///     Result<List(EdgeView<Graph, Graph>), GraphError> - A Result object which is Ok if the edges were successfully imported, and Err otherwise.
    #[pyo3(signature = (edges, force=false))]
    pub fn import_edges(
        &self,
        edges: Vec<PyEdge>,
        force: Option<bool>,
    ) -> Result<Vec<EdgeView<GraphWithDeletions, GraphWithDeletions>>, GraphError> {
        let edgeviews = edges.iter().map(|edge| &edge.edge).collect();
        self.graph.import_edges(edgeviews, force)
    }

    //******  Saving And Loading  ******//

    // Alternative constructors are tricky, see: https://gist.github.com/redshiftzero/648e4feeff3843ffd9924f13625f839c

    /// Loads a graph from the given path.
    ///
    /// Arguments:
    ///   path (str): The path to the graph.
    ///
    /// Returns:
    ///  Graph: The loaded graph.
    #[staticmethod]
    pub fn load_from_file(path: &str) -> Result<GraphWithDeletions, GraphError> {
        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), path].iter().collect();
        GraphWithDeletions::load_from_file(file_path)
    }

    /// Saves the graph to the given path.
    ///
    /// Arguments:
    ///  path (str): The path to the graph.
    ///
    /// Returns:
    /// None
    pub fn save_to_file(&self, path: &str) -> Result<(), GraphError> {
        self.graph.save_to_file(Path::new(path))
    }

    /// Get bincode encoded graph
    pub fn bincode<'py>(&'py self, py: Python<'py>) -> Result<&'py PyBytes, GraphError> {
        let bytes = MaterializedGraph::from(self.graph.clone()).bincode()?;
        Ok(PyBytes::new(py, &bytes))
    }
}
