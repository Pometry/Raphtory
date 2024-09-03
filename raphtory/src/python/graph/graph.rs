//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add nodes and edges,
//! create windows, and query the graph with a variety of algorithms.
//! In Python, this class wraps around the rust graph.
use crate::{
    algorithms::components::LargestConnectedComponent,
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError},
    db::{
        api::view::internal::{CoreGraphOps, DynamicGraph, IntoDynamic, MaterializedGraph},
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
    serialise::{StableDecode, StableEncode},
};
use pyo3::prelude::*;
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::PathBuf,
};

/// A temporal graph.
#[derive(Clone)]
#[pyclass(name = "Graph", extends = PyGraphView, module = "raphtory")]
pub struct PyGraph {
    pub graph: Graph,
}

impl_serialise!(PyGraph, graph: Graph, "Graph");

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
    #[pyo3(signature=(num_shards=None))]
    pub fn py_new(num_shards: Option<usize>) -> (Self, PyGraphView) {
        let graph = match num_shards {
            None => Graph::new(),
            Some(num_shards) => Graph::new_with_shards(num_shards),
        };
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
    pub fn to_disk_graph(&self, graph_dir: String) -> Result<Graph, GraphError> {
        use crate::db::api::storage::graph::storage_ops::GraphStorage;
        use std::sync::Arc;

        let disk_graph = Graph::persist_as_disk_graph(&self.graph, graph_dir)?;
        let storage = GraphStorage::Disk(Arc::new(disk_graph));
        let graph = Graph::from_internal_graph(storage);
        Ok(graph)
    }

    /// Adds a new node with the given id and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int|str|Datetime): The timestamp of the node.
    ///    id (str|int): The id of the node.
    ///    properties (dict): The properties of the node (optional).
    ///    node_type (str): The optional string which will be used as a node type
    /// Returns:
    ///   Node: The added node
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
    ///    timestamp (int|str|Datetime): The timestamp of the temporal property.
    ///    properties (dict): The temporal properties of the graph.
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
    pub fn update_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        self.graph.update_constant_properties(properties)
    }

    /// Adds a new edge with the given source and destination nodes and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int|str|Datetime): The timestamp of the edge.
    ///    src (str|int): The id of the source node.
    ///    dst (str|int): The id of the destination node.
    ///    properties (dict): The properties of the edge, as a dict of string and properties (optional).
    ///    layer (str): The layer of the edge (optional).
    ///
    /// Returns:
    ///   Edge: The added edge
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
    ///     node (Node): A Node object representing the node to be imported.
    ///     force (boolean): An optional boolean flag indicating whether to force the import of the node.
    ///
    /// Returns:
    ///     Node: A Result object which is Ok if the node was successfully imported, and Err otherwise.
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
    ///     nodes (List[Node]): A vector of PyNode objects representing the nodes to be imported.
    ///     force (boolean): An optional boolean flag indicating whether to force the import of the nodes.
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
    ///     edge (Edge): A PyEdge object representing the edge to be imported.
    ///     force (boolean): An optional boolean flag indicating whether to force the import of the edge.
    ///
    /// Returns:
    ///     Edge: A Result object which is Ok if the edge was successfully imported, and Err otherwise.
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
    ///     edges (List[Edge]): A list of Edge objects representing the edges to be imported.
    ///     force (boolean): An optional boolean flag indicating whether to force the import of the edges.
    #[pyo3(signature = (edges, force = false))]
    pub fn import_edges(&self, edges: Vec<PyEdge>, force: bool) -> Result<(), GraphError> {
        let edge_views = edges.iter().map(|edge| &edge.edge);
        self.graph.import_edges(edge_views, force)
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the node with the specified id
    ///
    /// Arguments:
    ///   id (str|int): the node id
    ///
    /// Returns:
    ///   Node: the node with the specified id, or None if the node does not exist
    pub fn node(&self, id: NodeRef) -> Option<NodeView<Graph>> {
        self.graph.node(id)
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the edge with the specified source and destination nodes
    ///
    /// Arguments:
    ///     src (str|int): the source node id
    ///     dst (str|int): the destination node id
    ///
    /// Returns:
    ///     Edge: the edge with the specified source and destination nodes, or None if the edge does not exist
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: NodeRef, dst: NodeRef) -> Option<EdgeView<Graph, Graph>> {
        self.graph.edge(src, dst)
    }

    //******  Saving And Loading  ******//

    // Alternative constructors are tricky, see: https://gist.github.com/redshiftzero/648e4feeff3843ffd9924f13625f839c

    /// Returns all the node types in the graph.
    ///
    /// Returns:
    /// List[str]
    pub fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.graph.get_all_node_types()
    }

    /// Gives the large connected component of a graph.
    ///
    /// # Example Usage:
    /// g.largest_connected_component()
    ///
    /// # Returns:
    /// Graph: sub-graph of the graph `g` containing the largest connected component
    ///
    pub fn largest_connected_component(&self) -> NodeSubgraph<Graph> {
        self.graph.largest_connected_component()
    }

    /// Get persistent graph
    pub fn persistent_graph<'py>(&'py self) -> PyResult<Py<PyPersistentGraph>> {
        PyPersistentGraph::py_from_db_graph(self.graph.persistent_graph())
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
    #[pyo3(
        signature = (df,time, id, node_type = None, node_type_col = None, properties = None, constant_properties = None, shared_constant_properties = None)
    )]
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
            self.graph.core_graph(),
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
    #[pyo3(
        signature = (parquet_path, time, id, node_type = None, node_type_col = None, properties = None, constant_properties = None, shared_constant_properties = None)
    )]
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
    #[pyo3(
        signature = (df, time, src, dst, properties = None, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None)
    )]
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
            self.graph.core_graph(),
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
    #[pyo3(
        signature = (parquet_path, time, src, dst, properties = None, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None)
    )]
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

    /// Load node properties from a Pandas DataFrame.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing node information.
    ///     id(str): The column name for the node IDs.
    ///     node_type (str): A constant value to use as the node type for all nodes (optional). Defaults to None. (cannot be used in combination with node_type_col)
    ///     node_type_col (str): The node type col name in dataframe (optional) Defaults to None. (cannot be used in combination with node_type)
    ///     constant_properties (List[str]): List of constant node property column names. Defaults to None. (optional)
    ///     shared_constant_properties (dict): A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
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
            self.graph.core_graph(),
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
    #[pyo3(signature = (parquet_path, id, node_type=None,node_type_col=None, constant_properties = None, shared_constant_properties = None))]
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
    #[pyo3(
        signature = (df, src, dst, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None)
    )]
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
            self.graph.core_graph(),
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
    #[pyo3(
        signature = (parquet_path, src, dst, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None)
    )]
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
