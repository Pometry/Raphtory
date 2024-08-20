//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add nodes and edges,
//! create windows, and query the graph with a variety of algorithms.
//! In Python, this class wraps around the rust graph.
use crate::{
    algorithms::components::LargestConnectedComponent,
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError},
    db::{
        api::view::{
            internal::{CoreGraphOps, DynamicGraph, IntoDynamic, MaterializedGraph},
            serialise::{StableDecode, StableEncoder},
        },
        graph::{edge::EdgeView, node::NodeView, views::node_subgraph::NodeSubgraph},
    },
    io::parquet_loaders::*,
    prelude::*,
    python::{
        graph::{
            edge::PyEdge, graph_with_deletions::PyPersistentGraph, io::pandas_loaders::*,
            node::PyNode, views::graph_view::PyGraphView,
        },
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

/// A temporal graph.
#[derive(Clone)]
#[pyclass(name = "Graph", extends = PyGraphView, module = "raphtory")]
pub struct PyGraph {
    pub graph: Graph,
}

impl Debug for PyGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.graph)
    }
}

impl From<Graph> for PyGraph {
    fn from(value: Graph) -> Self {
        Self { graph: value }
    }
}

impl From<PyGraph> for MaterializedGraph {
    fn from(value: PyGraph) -> Self {
        value.graph.into()
    }
}

impl From<PyPersistentGraph> for MaterializedGraph {
    fn from(value: PyPersistentGraph) -> Self {
        value.graph.into()
    }
}

impl From<PyGraph> for Graph {
    fn from(value: PyGraph) -> Self {
        value.graph
    }
}

impl From<PyGraph> for DynamicGraph {
    fn from(value: PyGraph) -> Self {
        value.graph.into_dynamic()
    }
}

impl<'source> FromPyObject<'source> for MaterializedGraph {
    fn extract(graph: &'source PyAny) -> PyResult<Self> {
        if let Ok(graph) = graph.extract::<PyRef<PyGraph>>() {
            Ok(graph.graph.clone().into())
        } else if let Ok(graph) = graph.extract::<PyRef<PyPersistentGraph>>() {
            Ok(graph.graph.clone().into())
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Incorrect type, object is not a PyGraph or PyPersistentGraph".to_string(),
            ))
        }
    }
}

impl IntoPy<PyObject> for Graph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, (PyGraph::from(self.clone()), PyGraphView::from(self)))
            .unwrap() // I think this only fails if we are out of memory? Seems to be unavoidable if we want to create an actual graph.
            .into_py(py)
    }
}

impl<'source> FromPyObject<'source> for Graph {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let g: PyRef<PyGraph> = ob.extract()?;
        Ok(g.graph.clone())
    }
}

impl PyGraph {
    pub fn py_from_db_graph(db_graph: Graph) -> PyResult<Py<PyGraph>> {
        Python::with_gil(|py| {
            Py::new(
                py,
                (PyGraph::from(db_graph.clone()), PyGraphView::from(db_graph)),
            )
        })
    }
}

#[pyclass(module = "raphtory")]
pub struct PyGraphEncoder;

#[pymethods]
impl PyGraphEncoder {
    #[new]
    fn new() -> Self {
        PyGraphEncoder
    }

    fn __call__(&self, bytes: Vec<u8>) -> Result<MaterializedGraph, GraphError> {
        MaterializedGraph::decode_from_bytes(&bytes)
    }
    fn __setstate__(&mut self) {}
    fn __getstate__(&self) {}
}

/// A temporal graph.
#[pymethods]
impl PyGraph {
    #[new]
    pub fn py_new() -> (Self, PyGraphView) {
        let graph = Graph::new();
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

    #[cfg(feature = "storage")]
    pub fn to_disk_graph(&self, graph_dir: String) -> PyResult<Py<Self>> {
        use std::sync::Arc;

        use crate::db::api::storage::storage_ops::GraphStorage;

        let disk_graph = Graph::persist_as_disk_graph(&self.graph, graph_dir)?;
        let storage = GraphStorage::Disk(Arc::new(disk_graph));
        let graph = Graph::from_internal_graph(storage);

        Python::with_gil(|py| {
            Ok(Py::new(
                py,
                (
                    Self {
                        graph: graph.clone(),
                    },
                    PyGraphView::from(graph.clone()),
                ),
            )?)
        })
    }

    /// Adds a new node with the given id and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the node.
    ///    id (str or int): The id of the node.
    ///    properties (dict): The properties of the node (optional).
    ///    node_type (str): The optional string which will be used as a node type
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None))]
    pub fn add_node(
        &self,
        timestamp: PyTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
    ) -> Result<NodeView<Graph, Graph>, GraphError> {
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
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the edge.
    ///    src (str or int): The id of the source node.
    ///    dst (str or int): The id of the destination node.
    ///    properties (dict): The properties of the edge, as a dict of string and properties (optional).
    ///    layer (str): The layer of the edge (optional).
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
    ) -> Result<EdgeView<Graph, Graph>, GraphError> {
        self.graph
            .add_edge(timestamp, src, dst, properties.unwrap_or_default(), layer)
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
    ) -> Result<NodeView<Graph, Graph>, GraphError> {
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
    ) -> Result<EdgeView<Graph, Graph>, GraphError> {
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

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the node with the specified id
    ///
    /// Arguments:
    ///   id (str or int): the node id
    ///
    /// Returns:
    ///   the node with the specified id, or None if the node does not exist
    pub fn node(&self, id: NodeRef) -> Option<NodeView<Graph>> {
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
    pub fn edge(&self, src: NodeRef, dst: NodeRef) -> Option<EdgeView<Graph, Graph>> {
        self.graph.edge(src, dst)
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
    pub fn load_from_file(path: &str, force: bool) -> Result<Graph, GraphError> {
        Graph::load_from_file(path, force)
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
    fn from_bincode(bytes: &[u8]) -> Result<Option<Graph>, GraphError> {
        let graph = MaterializedGraph::from_bincode(bytes)?;
        Ok(graph.into_events())
    }

    /// Gives the large connected component of a graph.
    ///
    /// # Example Usage:
    /// g.largest_connected_component()
    ///
    /// # Returns:
    /// A raphtory graph, which essentially is a sub-graph of the graph `g`
    ///
    pub fn largest_connected_component(&self) -> NodeSubgraph<Graph> {
        self.graph.largest_connected_component()
    }

    /// Get persistent graph
    pub fn persistent_graph<'py>(&'py self) -> PyResult<Py<PyPersistentGraph>> {
        PyPersistentGraph::py_from_db_graph(self.graph.persistent_graph())
    }

    /// Load a graph from a Pandas DataFrame.
    ///
    /// Args:
    ///     edge_df (pandas.DataFrame): The DataFrame containing the edges.
    ///     edge_src (str): The column name for the source node ids.
    ///     edge_dst (str): The column name for the destination node ids.
    ///     edge_time (str): The column name for the timestamps.
    ///     edge_properties (list): The column names for the temporal properties (optional) Defaults to None.
    ///     edge_const_properties (list): The column names for the constant properties (optional) Defaults to None.
    ///     edge_shared_const_properties (dict): A dictionary of constant properties that will be added to every edge (optional) Defaults to None.
    ///     edge_layer (str): The edge layer name (optional) Defaults to None.
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the edge_df or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///     node_df (pandas.DataFrame): The DataFrame containing the nodes (optional) Defaults to None.
    ///     node_id (str): The column name for the node ids (optional) Defaults to None.
    ///     node_time (str): The column name for the node timestamps (optional) Defaults to None.
    ///     node_properties (list): The column names for the node temporal properties (optional) Defaults to None.
    ///     node_const_properties (list): The column names for the node constant properties (optional) Defaults to None.
    ///     node_shared_const_properties (dict): A dictionary of constant properties that will be added to every node (optional) Defaults to None.
    ///     node_type (str): the column name for the node type
    ///     node_type_in_df (bool): whether the node type should be used to look up the values in a column of the df or if it should be used directly as the node type
    ///
    /// Returns:
    ///      Graph: The loaded Graph object.
    #[staticmethod]
    #[pyo3(
        signature = (edge_df, edge_src, edge_dst, edge_time, edge_properties = None, edge_const_properties = None, edge_shared_const_properties = None,
        edge_layer = None, layer_in_df = true, node_df = None, node_id = None, node_time = None, node_properties = None,
        node_const_properties = None, node_shared_const_properties = None, node_type = None, node_type_in_df = true)
    )]
    fn load_from_pandas(
        edge_df: &PyAny,
        edge_src: &str,
        edge_dst: &str,
        edge_time: &str,
        edge_properties: Option<Vec<&str>>,
        edge_const_properties: Option<Vec<&str>>,
        edge_shared_const_properties: Option<HashMap<String, Prop>>,
        edge_layer: Option<&str>,
        layer_in_df: Option<bool>,
        node_df: Option<&PyAny>,
        node_id: Option<&str>,
        node_time: Option<&str>,
        node_properties: Option<Vec<&str>>,
        node_const_properties: Option<Vec<&str>>,
        node_shared_const_properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
        node_type_in_df: Option<bool>,
    ) -> Result<Graph, GraphError> {
        let graph = Graph::new();
        if let (Some(node_df), Some(node_id), Some(node_time)) = (node_df, node_id, node_time) {
            load_nodes_from_pandas(
                &graph.core_graph(),
                node_df,
                node_id,
                node_time,
                node_type,
                node_type_in_df,
                node_properties.as_ref().map(|props| props.as_ref()),
                node_const_properties.as_ref().map(|props| props.as_ref()),
                node_shared_const_properties.as_ref(),
            )?;
        }
        load_edges_from_pandas(
            &graph.core_graph(),
            edge_df,
            edge_src,
            edge_dst,
            edge_time,
            edge_properties.as_ref().map(|props| props.as_ref()),
            edge_const_properties.as_ref().map(|props| props.as_ref()),
            edge_shared_const_properties.as_ref(),
            edge_layer,
            layer_in_df,
        )?;
        Ok(graph)
    }

    /// Load a graph from Parquet file.
    ///
    /// Args:
    ///     edge_parquet_path (str): Parquet file or directory of Parquet files containing the edges.
    ///     edge_src (str): The column name for the source node ids.
    ///     edge_dst (str): The column name for the destination node ids.
    ///     edge_time (str): The column name for the timestamps.
    ///     edge_properties (list): The column names for the temporal properties (optional) Defaults to None.
    ///     edge_const_properties (list): The column names for the constant properties (optional) Defaults to None.
    ///     edge_shared_const_properties (dict): A dictionary of constant properties that will be added to every edge (optional) Defaults to None.
    ///     edge_layer (str): The edge layer name (optional) Defaults to None.
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the edge_df or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///     node_parquet_path (str): Parquet file or directory of Parquet files containing the nodes (optional) Defaults to None.
    ///     node_id (str): The column name for the node ids (optional) Defaults to None.
    ///     node_time (str): The column name for the node timestamps (optional) Defaults to None.
    ///     node_properties (list): The column names for the node temporal properties (optional) Defaults to None.
    ///     node_const_properties (list): The column names for the node constant properties (optional) Defaults to None.
    ///     node_shared_const_properties (dict): A dictionary of constant properties that will be added to every node (optional) Defaults to None.
    ///     node_type (str): the column name for the node type
    ///     node_type_in_df (bool): whether the node type should be used to look up the values in a column of the df or if it should be used directly as the node type
    ///
    /// Returns:
    ///      Graph: The loaded Graph object.
    #[staticmethod]
    #[pyo3(
        signature = (edge_parquet_path, edge_src, edge_dst, edge_time, edge_properties = None, edge_const_properties = None, edge_shared_const_properties = None,
        edge_layer = None, layer_in_df = true, node_parquet_path = None, node_id = None, node_time = None, node_properties = None,
        node_const_properties = None, node_shared_const_properties = None, node_type = None, node_type_in_df = true)
    )]
    fn load_from_parquet(
        edge_parquet_path: PathBuf,
        edge_src: &str,
        edge_dst: &str,
        edge_time: &str,
        edge_properties: Option<Vec<&str>>,
        edge_const_properties: Option<Vec<&str>>,
        edge_shared_const_properties: Option<HashMap<String, Prop>>,
        edge_layer: Option<&str>,
        layer_in_df: Option<bool>,
        node_parquet_path: Option<PathBuf>,
        node_id: Option<&str>,
        node_time: Option<&str>,
        node_properties: Option<Vec<&str>>,
        node_const_properties: Option<Vec<&str>>,
        node_shared_const_properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
        node_type_in_df: Option<bool>,
    ) -> Result<Graph, GraphError> {
        let graph = Graph::new();

        if let (Some(node_parquet_path), Some(node_id), Some(node_time)) =
            (node_parquet_path, node_id, node_time)
        {
            load_nodes_from_parquet(
                &graph,
                &node_parquet_path,
                node_id,
                node_time,
                node_type,
                node_type_in_df,
                node_properties.as_ref().map(|props| props.as_ref()),
                node_const_properties.as_ref().map(|props| props.as_ref()),
                node_shared_const_properties.as_ref(),
            )?;
        }
        load_edges_from_parquet(
            &graph,
            edge_parquet_path,
            edge_src,
            edge_dst,
            edge_time,
            edge_properties.as_ref().map(|props| props.as_ref()),
            edge_const_properties.as_ref().map(|props| props.as_ref()),
            edge_shared_const_properties.as_ref(),
            edge_layer,
            layer_in_df,
        )?;

        Ok(graph)
    }

    /// Load nodes from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (pandas.DataFrame): The Pandas DataFrame containing the nodes.
    ///     id (str): The column name for the node IDs.
    ///     time (str): The column name for the timestamps.
    ///     node_type (str): the column name for the node type
    ///     node_type_in_df (bool): whether the node type should be used to look up the values in a column of the df or if it should be used directly as the node type
    ///     properties (List<str>): List of node property column names. Defaults to None. (optional)
    ///     const_properties (List<str>): List of constant node property column names. Defaults to None.  (optional)
    ///     shared_const_properties (Dictionary/Hashmap of properties): A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(
        signature = (df, id, time, node_type = None, node_type_in_df = true, properties = None, const_properties = None, shared_const_properties = None)
    )]
    fn load_nodes_from_pandas(
        &self,
        df: &PyAny,
        id: &str,
        time: &str,
        node_type: Option<&str>,
        node_type_in_df: Option<bool>,
        properties: Option<Vec<&str>>,
        const_properties: Option<Vec<&str>>,
        shared_const_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        load_nodes_from_pandas(
            self.graph.core_graph(),
            df,
            id,
            time,
            node_type,
            node_type_in_df,
            properties.as_ref().map(|props| props.as_ref()),
            const_properties.as_ref().map(|props| props.as_ref()),
            shared_const_properties.as_ref(),
        )
    }

    /// Load nodes from a Parquet file into the graph.
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files containing the nodes
    ///     id (str): The column name for the node IDs.
    ///     time (str): The column name for the timestamps.
    ///     node_type (str): the column name for the node type
    ///     node_type_in_df (bool): whether the node type should be used to look up the values in a column of the df or if it should be used directly as the node type
    ///     properties (List<str>): List of node property column names. Defaults to None. (optional)
    ///     const_properties (List<str>): List of constant node property column names. Defaults to None.  (optional)
    ///     shared_const_properties (Dictionary/Hashmap of properties): A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(
        signature = (parquet_path, id, time, node_type = None, node_type_in_df = true, properties = None, const_properties = None, shared_const_properties = None)
    )]
    fn load_nodes_from_parquet(
        &self,
        parquet_path: PathBuf,
        id: &str,
        time: &str,
        node_type: Option<&str>,
        node_type_in_df: Option<bool>,
        properties: Option<Vec<&str>>,
        const_properties: Option<Vec<&str>>,
        shared_const_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        load_nodes_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            id,
            time,
            node_type,
            node_type_in_df,
            properties.as_ref().map(|props| props.as_ref()),
            const_properties.as_ref().map(|props| props.as_ref()),
            shared_const_properties.as_ref(),
        )
    }

    /// Load edges from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing the edges.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     time (str): The column name for the update timestamps.
    ///     properties (List<str>): List of edge property column names. Defaults to None. (optional)
    ///     const_properties (List<str>): List of constant edge property column names. Defaults to None. (optional)
    ///     shared_const_properties (dict): A dictionary of constant properties that will be added to every edge. Defaults to None. (optional)
    ///     layer (str): The edge layer name (optional) Defaults to None.
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the dateframe or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(
        signature = (df, src, dst, time, properties = None, const_properties = None, shared_const_properties = None, layer = None, layer_in_df = true)
    )]
    fn load_edges_from_pandas(
        &self,
        df: &PyAny,
        src: &str,
        dst: &str,
        time: &str,
        properties: Option<Vec<&str>>,
        const_properties: Option<Vec<&str>>,
        shared_const_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_in_df: Option<bool>,
    ) -> Result<(), GraphError> {
        load_edges_from_pandas(
            self.graph.core_graph(),
            df,
            src,
            dst,
            time,
            properties.as_ref().map(|props| props.as_ref()),
            const_properties.as_ref().map(|props| props.as_ref()),
            shared_const_properties.as_ref(),
            layer,
            layer_in_df,
        )
    }

    /// Load edges from a Parquet file into the graph.
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files path containing edges
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     time (str): The column name for the update timestamps.
    ///     properties (List<str>): List of edge property column names. Defaults to None. (optional)
    ///     const_properties (List<str>): List of constant edge property column names. Defaults to None. (optional)
    ///     shared_const_properties (dict): A dictionary of constant properties that will be added to every edge. Defaults to None. (optional)
    ///     layer (str): The edge layer name (optional) Defaults to None.
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the dataframe or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(
        signature = (parquet_path, src, dst, time, properties = None, const_properties = None, shared_const_properties = None, layer = None, layer_in_df = true)
    )]
    fn load_edges_from_parquet(
        &self,
        parquet_path: PathBuf,
        src: &str,
        dst: &str,
        time: &str,
        properties: Option<Vec<&str>>,
        const_properties: Option<Vec<&str>>,
        shared_const_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_in_df: Option<bool>,
    ) -> Result<(), GraphError> {
        load_edges_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            src,
            dst,
            time,
            properties.as_ref().map(|props| props.as_ref()),
            const_properties.as_ref().map(|props| props.as_ref()),
            shared_const_properties.as_ref(),
            layer,
            layer_in_df,
        )
    }

    /// Load node properties from a Pandas DataFrame.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing node information.
    ///     id(str): The column name for the node IDs.
    ///     const_properties (List<str>): List of constant node property column names. Defaults to None. (optional)
    ///     shared_const_properties (<HashMap<String, Prop>>):  A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(signature = (df, id, const_properties = None, shared_const_properties = None))]
    fn load_node_props_from_pandas(
        &self,
        df: &PyAny,
        id: &str,
        const_properties: Option<Vec<&str>>,
        shared_const_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        load_node_props_from_pandas(
            self.graph.core_graph(),
            df,
            id,
            const_properties.as_ref().map(|props| props.as_ref()),
            shared_const_properties.as_ref(),
        )
    }

    /// Load node properties from a parquet file.
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files path containing node information.
    ///     id(str): The column name for the node IDs.
    ///     const_properties (List<str>): List of constant node property column names. Defaults to None. (optional)
    ///     shared_const_properties (<HashMap<String, Prop>>):  A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(signature = (parquet_path, id, const_properties = None, shared_const_properties = None))]
    fn load_node_props_from_parquet(
        &self,
        parquet_path: PathBuf,
        id: &str,
        const_properties: Option<Vec<&str>>,
        shared_const_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        load_node_props_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            id,
            const_properties.as_ref().map(|props| props.as_ref()),
            shared_const_properties.as_ref(),
        )
    }

    /// Load edge properties from a Pandas DataFrame.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing edge information.
    ///     src (str): The column name for the source node.
    ///     dst (str): The column name for the destination node.
    ///     const_properties (List<str>): List of constant edge property column names. Defaults to None. (optional)
    ///     shared_const_properties (dict): A dictionary of constant properties that will be added to every edge. Defaults to None. (optional)
    ///     layer (str): Layer name. Defaults to None.  (optional)
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the data frame or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(
        signature = (df, src, dst, const_properties = None, shared_const_properties = None, layer = None, layer_in_df = true)
    )]
    fn load_edge_props_from_pandas(
        &self,
        df: &PyAny,
        src: &str,
        dst: &str,
        const_properties: Option<Vec<&str>>,
        shared_const_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_in_df: Option<bool>,
    ) -> Result<(), GraphError> {
        load_edge_props_from_pandas(
            self.graph.core_graph(),
            df,
            src,
            dst,
            const_properties.as_ref().map(|props| props.as_ref()),
            shared_const_properties.as_ref(),
            layer,
            layer_in_df,
        )
    }

    /// Load edge properties from parquet file
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files path containing edge information.
    ///     src (str): The column name for the source node.
    ///     dst (str): The column name for the destination node.
    ///     const_properties (List<str>): List of constant edge property column names. Defaults to None. (optional)
    ///     shared_const_properties (dict): A dictionary of constant properties that will be added to every edge. Defaults to None. (optional)
    ///     layer (str): Layer name. Defaults to None.  (optional)
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the data frame or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(
        signature = (parquet_path, src, dst, const_properties = None, shared_const_properties = None, layer = None, layer_in_df = true)
    )]
    fn load_edge_props_from_parquet(
        &self,
        parquet_path: PathBuf,
        src: &str,
        dst: &str,
        const_properties: Option<Vec<&str>>,
        shared_const_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_in_df: Option<bool>,
    ) -> Result<(), GraphError> {
        load_edge_props_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            src,
            dst,
            const_properties.as_ref().map(|props| props.as_ref()),
            shared_const_properties.as_ref(),
            layer,
            layer_in_df,
        )
    }
}
