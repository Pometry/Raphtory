//! Defines the `PersistentGraph` class, which represents a raphtory graph in memory.
//! Unlike in the `Graph` which has event semantics, `PersistentGraph` has edges that persist until explicitly deleted.
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
            view::{
                internal::{CoreGraphOps, MaterializedGraph},
                serialise::StableEncoder,
            },
        },
        graph::{edge::EdgeView, node::NodeView, views::deletion_graph::PersistentGraph},
    },
    prelude::{DeletionOps, GraphViewOps, ImportOps},
    python::{
        graph::{edge::PyEdge, node::PyNode, views::graph_view::PyGraphView},
        utils::PyTime,
    },
};
use pyo3::{prelude::*, types::PyBytes};
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
};

use super::{
    graph::{PyGraph, PyGraphEncoder},
    io::pandas_loaders::*,
};
use crate::io::parquet_loaders::*;

/// A temporal graph that allows edges and nodes to be deleted.
#[derive(Clone)]
#[pyclass(name = "PersistentGraph", extends = PyGraphView)]
pub struct PyPersistentGraph {
    pub(crate) graph: PersistentGraph,
}

impl Debug for PyPersistentGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.graph)
    }
}

impl From<PersistentGraph> for PyPersistentGraph {
    fn from(value: PersistentGraph) -> Self {
        Self { graph: value }
    }
}

impl IntoPy<PyObject> for PersistentGraph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(
            py,
            (
                PyPersistentGraph::from(self.clone()),
                PyGraphView::from(self),
            ),
        )
        .unwrap() // I think this only fails if we are out of memory? Seems to be unavoidable if we want to create an actual graph.
        .into_py(py)
    }
}

impl<'source> FromPyObject<'source> for PersistentGraph {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let g: PyRef<PyPersistentGraph> = ob.extract()?;
        Ok(g.graph.clone())
    }
}

impl PyPersistentGraph {
    pub fn py_from_db_graph(db_graph: PersistentGraph) -> PyResult<Py<PyPersistentGraph>> {
        Python::with_gil(|py| {
            Py::new(
                py,
                (
                    PyPersistentGraph::from(db_graph.clone()),
                    PyGraphView::from(db_graph),
                ),
            )
        })
    }
}

/// A temporal graph that allows edges and nodes to be deleted.
#[pymethods]
impl PyPersistentGraph {
    #[new]
    pub fn py_new() -> (Self, PyGraphView) {
        let graph = PersistentGraph::new();
        (
            Self {
                graph: graph.clone(),
            },
            PyGraphView::from(graph),
        )
    }

    fn __reduce__(&self) -> (PyGraphEncoder, (Vec<u8>,)) {
        let state = self.graph.encode_to_vec();
        (PyGraphEncoder, (state,))
    }

    /// Adds a new node with the given id and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the node.
    ///    id (str or int): The id of the node.
    ///    properties (dict): The properties of the node.
    ///    node_type (str) : The optional string which will be used as a node type
    ///
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None))]
    pub fn add_node(
        &self,
        timestamp: PyTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
    ) -> Result<NodeView<PersistentGraph>, GraphError> {
        self.graph
            .add_node(timestamp, id, properties.unwrap_or_default(), node_type)
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
    #[pyo3(signature = (timestamp, src, dst, properties = None, layer = None))]
    pub fn add_edge(
        &self,
        timestamp: PyTime,
        src: GID,
        dst: GID,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<EdgeView<PersistentGraph, PersistentGraph>, GraphError> {
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
        src: GID,
        dst: GID,
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
    pub fn node(&self, id: NodeRef) -> Option<NodeView<PersistentGraph>> {
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
    ) -> Option<EdgeView<PersistentGraph, PersistentGraph>> {
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
    #[pyo3(signature = (node, force = false))]
    pub fn import_node(
        &self,
        node: PyNode,
        force: bool,
    ) -> Result<NodeView<PersistentGraph, PersistentGraph>, GraphError> {
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
    #[pyo3(signature = (nodes, force = false))]
    pub fn import_nodes(&self, nodes: Vec<PyNode>, force: bool) -> Result<(), GraphError> {
        let node_views = nodes.iter().map(|node| &node.node);
        self.graph.import_nodes(node_views, force)
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
    #[pyo3(signature = (edge, force = false))]
    pub fn import_edge(
        &self,
        edge: PyEdge,
        force: bool,
    ) -> Result<EdgeView<PersistentGraph, PersistentGraph>, GraphError> {
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
    #[pyo3(signature = (edges, force = false))]
    pub fn import_edges(&self, edges: Vec<PyEdge>, force: bool) -> Result<(), GraphError> {
        let edge_views = edges.iter().map(|edge| &edge.edge);
        self.graph.import_edges(edge_views, force)
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
    #[pyo3(signature = (path, force = false))]
    pub fn load_from_file(path: &str, force: bool) -> Result<PersistentGraph, GraphError> {
        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), path].iter().collect();
        PersistentGraph::load_from_file(file_path, force)
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

    /// Returns all the node types in the graph.
    ///
    /// Returns:
    /// A list of node types
    pub fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.graph.get_all_node_types()
    }

    /// Get bincode encoded graph
    pub fn bincode<'py>(&'py self, py: Python<'py>) -> Result<&'py PyBytes, GraphError> {
        let bytes = MaterializedGraph::from(self.graph.clone()).bincode()?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Creates a graph from a bincode encoded graph
    #[staticmethod]
    fn from_bincode(bytes: &[u8]) -> Result<Option<PersistentGraph>, GraphError> {
        let graph = MaterializedGraph::from_bincode(bytes)?;
        Ok(graph.into_persistent())
    }

    /// Get event graph
    pub fn event_graph<'py>(&'py self) -> PyResult<Py<PyGraph>> {
        PyGraph::py_from_db_graph(self.graph.event_graph())
    }

    /// Load nodes from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (pandas.DataFrame): The Pandas DataFrame containing the nodes.
    ///     time (str): The column name for the timestamps.
    ///     id (str): The column name for the node IDs.
    ///     node_type (str): A constant value to use as the node type for all nodes (optional). Defaults to None. (cannot be used in combination with node_type_col)
    ///     node_type_col (str): The node type col name in dataframe (optional) Defaults to None. (cannot be used in combination with node_type)
    ///     properties (List[str]): List of node property column names. Defaults to None. (optional)
    ///     constant_properties (List[str]): List of constant node property column names. Defaults to None.  (optional)
    ///     shared_constant_properties (dict): A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
    /// Returns:
    ///     None: If the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (df,time,id, node_type = None, node_type_col = None, properties = None, constant_properties = None, shared_constant_properties = None))]
    fn load_nodes_from_pandas(
        &self,
        df: &PyAny,
        time: &str,
        id: &str,
        node_type: Option<&str>,
        node_type_col: Option<&str>,
        properties: Option<Vec<&str>>,
        constant_properties: Option<Vec<&str>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        load_nodes_from_pandas(
            &self.graph.0,
            df,
            time,
            id,
            node_type,
            node_type_col,
            properties.as_ref().map(|props| props.as_ref()),
            constant_properties.as_ref().map(|props| props.as_ref()),
            shared_constant_properties.as_ref(),
        )
    }

    /// Load nodes from a Parquet file into the graph.
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files containing the nodes
    ///     time (str): The column name for the timestamps.
    ///     id (str): The column name for the node IDs.
    ///     node_type (str): A constant value to use as the node type for all nodes (optional). Defaults to None. (cannot be used in combination with node_type_col)
    ///     node_type_col (str): The node type col name in dataframe (optional) Defaults to None. (cannot be used in combination with node_type)
    ///     properties (List[str]): List of node property column names. Defaults to None. (optional)
    ///     constant_properties (List[str]): List of constant node property column names. Defaults to None.  (optional)
    ///     shared_constant_properties (dict): A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
    /// Returns:
    ///     None: If the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (parquet_path, time,id, node_type = None, node_type_col = None, properties = None, constant_properties = None, shared_constant_properties = None))]
    fn load_nodes_from_parquet(
        &self,
        parquet_path: PathBuf,
        time: &str,
        id: &str,
        node_type: Option<&str>,
        node_type_col: Option<&str>,
        properties: Option<Vec<&str>>,
        constant_properties: Option<Vec<&str>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        load_nodes_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            time,
            id,
            node_type,
            node_type_col,
            properties.as_ref().map(|props| props.as_ref()),
            constant_properties.as_ref().map(|props| props.as_ref()),
            shared_constant_properties.as_ref(),
        )
    }

    /// Load edges from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing the edges.
    ///     time (str): The column name for the update timestamps.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     properties (List[str]): List of edge property column names. Defaults to None. (optional)
    ///     constant_properties (List[str]): List of constant edge property column names. Defaults to None. (optional)
    ///     shared_constant_properties (dict): A dictionary of constant properties that will be added to every edge. Defaults to None. (optional)
    ///     layer (str): A constant value to use as the layer for all edges (optional) Defaults to None. (cannot be used in combination with layer_col)
    ///     layer_col (str): The edge layer col name in dataframe (optional) Defaults to None. (cannot be used in combination with layer)
    /// Returns:
    ///     None: If the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (df, time, src, dst, properties = None, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None))]
    fn load_edges_from_pandas(
        &self,
        df: &PyAny,
        time: &str,
        src: &str,
        dst: &str,
        properties: Option<Vec<&str>>,
        constant_properties: Option<Vec<&str>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        load_edges_from_pandas(
            &self.graph.0,
            df,
            time,
            src,
            dst,
            properties.as_ref().map(|props| props.as_ref()),
            constant_properties.as_ref().map(|props| props.as_ref()),
            shared_constant_properties.as_ref(),
            layer,
            layer_col,
        )
    }

    /// Load edges from a Parquet file into the graph.
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files path containing edges
    ///     time (str): The column name for the update timestamps.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     properties (List[str]): List of edge property column names. Defaults to None. (optional)
    ///     constant_properties (List[str]): List of constant edge property column names. Defaults to None. (optional)
    ///     shared_constant_properties (dict): A dictionary of constant properties that will be added to every edge. Defaults to None. (optional)
    ///     layer (str): A constant value to use as the layer for all edges (optional) Defaults to None. (cannot be used in combination with layer_col)
    ///     layer_col (str): The edge layer col name in dataframe (optional) Defaults to None. (cannot be used in combination with layer)
    /// Returns:
    ///     None: If the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (parquet_path, time, src, dst, properties = None, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None))]
    fn load_edges_from_parquet(
        &self,
        parquet_path: PathBuf,
        time: &str,
        src: &str,
        dst: &str,
        properties: Option<Vec<&str>>,
        constant_properties: Option<Vec<&str>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        load_edges_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            time,
            src,
            dst,
            properties.as_ref().map(|props| props.as_ref()),
            constant_properties.as_ref().map(|props| props.as_ref()),
            shared_constant_properties.as_ref(),
            layer,
            layer_col,
        )
    }

    /// Load edges deletions from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing the edges.
    ///     time (str): The column name for the update timestamps.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     layer (str): A constant value to use as the layer for all edges (optional) Defaults to None. (cannot be used in combination with layer_col)
    ///     layer_col (str): The edge layer col name in dataframe (optional) Defaults to None. (cannot be used in combination with layer)
    /// Returns:
    ///     None: If the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (df, time, src, dst, layer = None, layer_col = None))]
    fn load_edge_deletions_from_pandas(
        &self,
        df: &PyAny,
        time: &str,
        src: &str,
        dst: &str,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        load_edge_deletions_from_pandas(&self.graph.0, df, time, src, dst, layer, layer_col)
    }

    /// Load edges deletions from a Parquet file into the graph.
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files path containing node information.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     time (str): The column name for the update timestamps.
    ///     layer (str): A constant value to use as the layer for all edges (optional) Defaults to None. (cannot be used in combination with layer_col)
    ///     layer_col (str): The edge layer col name in dataframe (optional) Defaults to None. (cannot be used in combination with layer)
    /// Returns:
    ///     None: If the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (parquet_path, time, src, dst, layer = None, layer_col = None))]
    fn load_edge_deletions_from_parquet(
        &self,
        parquet_path: PathBuf,
        time: &str,
        src: &str,
        dst: &str,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        load_edge_deletions_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            time,
            src,
            dst,
            layer,
            layer_col,
        )
    }

    /// Load node properties from a Pandas DataFrame.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing node information.
    ///     id(str): The column name for the node IDs.
    ///     node_type (str): A constant value to use as the node type for all nodes (optional). Defaults to None. (cannot be used in combination with node_type_col)
    ///     node_type_col (str): The node type col name in dataframe (optional) Defaults to None. (cannot be used in combination with node_type)
    ///     constant_properties (List[str]): List of constant node property column names. Defaults to None. (optional)
    ///     shared_constant_properties (dict): A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
    ///
    /// Returns:
    ///     None: If the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (df, id, node_type=None, node_type_col=None, constant_properties = None, shared_constant_properties = None))]
    fn load_node_props_from_pandas(
        &self,
        df: &PyAny,
        id: &str,
        node_type: Option<&str>,
        node_type_col: Option<&str>,
        constant_properties: Option<Vec<&str>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        load_node_props_from_pandas(
            &self.graph.0,
            df,
            id,
            node_type,
            node_type_col,
            constant_properties.as_ref().map(|props| props.as_ref()),
            shared_constant_properties.as_ref(),
        )
    }

    /// Load node properties from a parquet file.
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files path containing node information.
    ///     id(str): The column name for the node IDs.
    ///     node_type (str): A constant value to use as the node type for all nodes (optional). Defaults to None. (cannot be used in combination with node_type_col)
    ///     node_type_col (str): The node type col name in dataframe (optional) Defaults to None. (cannot be used in combination with node_type)
    ///     constant_properties (List[str]): List of constant node property column names. Defaults to None. (optional)
    ///     shared_constant_properties (dict): A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
    ///
    /// Returns:
    ///     None: If the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (parquet_path, id, node_type = None, node_type_col=None, constant_properties = None, shared_constant_properties = None))]
    fn load_node_props_from_parquet(
        &self,
        parquet_path: PathBuf,
        id: &str,
        node_type: Option<&str>,
        node_type_col: Option<&str>,
        constant_properties: Option<Vec<&str>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        load_node_props_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            id,
            node_type,
            node_type_col,
            constant_properties.as_ref().map(|props| props.as_ref()),
            shared_constant_properties.as_ref(),
        )
    }

    /// Load edge properties from a Pandas DataFrame.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing edge information.
    ///     src (str): The column name for the source node.
    ///     dst (str): The column name for the destination node.
    ///     constant_properties (List[str]): List of constant edge property column names. Defaults to None. (optional)
    ///     shared_constant_properties (dict): A dictionary of constant properties that will be added to every edge. Defaults to None. (optional)
    ///     layer (str): The edge layer name (optional) Defaults to None.
    ///     layer_col (str): The edge layer col name in dataframe (optional) Defaults to None.
    ///
    /// Returns:
    ///     None: If the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (df, src, dst, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None))]
    fn load_edge_props_from_pandas(
        &self,
        df: &PyAny,
        src: &str,
        dst: &str,
        constant_properties: Option<Vec<&str>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        load_edge_props_from_pandas(
            &self.graph.0,
            df,
            src,
            dst,
            constant_properties.as_ref().map(|props| props.as_ref()),
            shared_constant_properties.as_ref(),
            layer,
            layer_col,
        )
    }

    /// Load edge properties from parquet file
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files path containing edge information.
    ///     src (str): The column name for the source node.
    ///     dst (str): The column name for the destination node.
    ///     constant_properties (List[str]): List of constant edge property column names. Defaults to None. (optional)
    ///     shared_constant_properties (dict): A dictionary of constant properties that will be added to every edge. Defaults to None. (optional)
    ///     layer (str): The edge layer name (optional) Defaults to None.
    ///     layer_col (str): The edge layer col name in dataframe (optional) Defaults to None.
    ///
    /// Returns:
    ///     None: If the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (parquet_path, src, dst, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None))]
    fn load_edge_props_from_parquet(
        &self,
        parquet_path: PathBuf,
        src: &str,
        dst: &str,
        constant_properties: Option<Vec<&str>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        load_edge_props_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            src,
            dst,
            constant_properties.as_ref().map(|props| props.as_ref()),
            shared_constant_properties.as_ref(),
            layer,
            layer_col,
        )
    }
}
