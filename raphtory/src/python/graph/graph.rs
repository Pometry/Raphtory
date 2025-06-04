//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add nodes and edges,
//! create windows, and query the graph with a variety of algorithms.
//! In Python, this class wraps around the rust graph.
use crate::{
    algorithms::components::LargestConnectedComponent,
    core::utils::errors::GraphError,
    db::{
        api::view::internal::{CoreGraphOps, DynamicGraph, IntoDynamic, MaterializedGraph},
        graph::{edge::EdgeView, node::NodeView, views::node_subgraph::NodeSubgraph},
    },
    io::parquet_loaders::*,
    prelude::*,
    python::{
        graph::{
            edge::PyEdge, graph_with_deletions::PyPersistentGraph, index::PyIndexSpec,
            io::pandas_loaders::*, node::PyNode, views::graph_view::PyGraphView,
        },
        types::iterable::FromIterable,
        utils::{PyNodeRef, PyTime},
    },
    serialise::{
        parquet::{ParquetDecoder, ParquetEncoder},
        InternalStableDecode, StableEncode,
    },
};
use pyo3::{prelude::*, pybacked::PyBackedStr, types::PyDict};
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::PathBuf,
};

/// A temporal graph with event semantics.
///
/// Arguments:
///     num_shards (int, optional): The number of locks to use in the storage to allow for multithreaded updates.
#[derive(Clone)]
#[pyclass(name = "Graph", extends = PyGraphView, module = "raphtory", frozen)]
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
    fn extract_bound(graph: &Bound<'source, PyAny>) -> PyResult<Self> {
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

impl<'py> IntoPyObject<'py> for Graph {
    type Target = PyGraph;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(py, (PyGraph::from(self.clone()), PyGraphView::from(self)))
    }
}

impl<'source> FromPyObject<'source> for Graph {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        let g = ob.downcast::<PyGraph>()?.borrow();

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

#[pyclass(module = "raphtory", name = "_GraphEncoder", frozen)]
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
    fn __setstate__(&self) {}
    fn __getstate__(&self) {}
}

/// A temporal graph.
#[pymethods]
impl PyGraph {
    #[new]
    #[pyo3(signature = (num_shards = None))]
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

    /// Persist graph on disk
    ///
    /// Arguments:
    ///     graph_dir (str | PathLike): the folder where the graph will be persisted
    ///
    /// Returns:
    ///     Graph: a view of the persisted graph
    #[cfg(feature = "storage")]
    pub fn to_disk_graph(&self, graph_dir: PathBuf) -> Result<Graph, GraphError> {
        use crate::db::api::storage::graph::storage_ops::GraphStorage;
        use std::sync::Arc;

        let disk_graph = Graph::persist_as_disk_graph(&self.graph, graph_dir)?;
        let storage = GraphStorage::Disk(Arc::new(disk_graph));
        let graph = Graph::from_internal_graph(storage);
        Ok(graph)
    }

    /// Persist graph to parquet files
    ///
    /// Arguments:
    ///     graph_dir (str | PathLike): the folder where the graph will be persisted as parquet
    ///
    pub fn to_parquet(&self, graph_dir: PathBuf) -> Result<(), GraphError> {
        self.graph.encode_parquet(graph_dir)
    }

    /// Read graph from parquet files
    ///
    /// Arguments:
    ///    graph_dir (str | PathLike): the folder where the graph is stored as parquet
    ///
    /// Returns:
    ///   Graph: a view of the graph
    ///
    #[staticmethod]
    pub fn from_parquet(graph_dir: PathBuf) -> Result<Graph, GraphError> {
        Graph::decode_parquet(graph_dir)
    }

    /// Adds a new node with the given id and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (TimeInput): The timestamp of the node.
    ///    id (str|int): The id of the node.
    ///    properties (PropInput, optional): The properties of the node.
    ///    node_type (str, optional): The optional string which will be used as a node type
    ///    secondary_index (int, optional): The optional integer which will be used as a secondary index
    ///
    /// Returns:
    ///     MutableNode: The added node.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (timestamp, id, properties = None, node_type = None, secondary_index = None)
    )]
    pub fn add_node(
        &self,
        timestamp: PyTime,
        id: GID,
        properties: Option<Bound<PyDict>>,
        node_type: Option<&str>,
        secondary_index: Option<usize>,
    ) -> Result<NodeView<Graph, Graph>, GraphError> {
        let props = properties
            .into_iter()
            .flat_map(|map| {
                map.into_iter().map(|(k, v)| {
                    k.extract::<String>()
                        .and_then(|k| v.extract::<Prop>().map(move |v| (k, v)))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        match secondary_index {
            None => self.graph.add_node(timestamp, id, props, node_type),
            Some(secondary_index) => {
                self.graph
                    .add_node((timestamp, secondary_index), id, props, node_type)
            }
        }
    }

    /// Creates a new node with the given id and properties to the graph. It fails if the node already exists.
    ///
    /// Arguments:
    ///    timestamp (TimeInput): The timestamp of the node.
    ///    id (str|int): The id of the node.
    ///    properties (PropInput, optional): The properties of the node.
    ///    node_type (str, optional): The optional string which will be used as a node type
    ///    secondary_index (int, optional): The optional integer which will be used as a secondary index
    ///
    /// Returns:
    ///     MutableNode: The created node.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None, secondary_index = None))]
    pub fn create_node(
        &self,
        timestamp: PyTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
        secondary_index: Option<usize>,
    ) -> Result<NodeView<Graph, Graph>, GraphError> {
        match secondary_index {
            None => {
                self.graph
                    .create_node(timestamp, id, properties.unwrap_or_default(), node_type)
            }
            Some(secondary_index) => self.graph.create_node(
                (timestamp, secondary_index),
                id,
                properties.unwrap_or_default(),
                node_type,
            ),
        }
    }

    /// Adds properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (TimeInput): The timestamp of the temporal property.
    ///    properties (PropInput): The temporal properties of the graph.
    ///    secondary_index (int, optional): The optional integer which will be used as a secondary index
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (timestamp, properties, secondary_index = None))]
    pub fn add_properties(
        &self,
        timestamp: PyTime,
        properties: HashMap<String, Prop>,
        secondary_index: Option<usize>,
    ) -> Result<(), GraphError> {
        match secondary_index {
            None => self.graph.add_properties(timestamp, properties),
            Some(secondary_index) => self
                .graph
                .add_properties((timestamp, secondary_index), properties),
        }
    }

    /// Adds static properties to the graph.
    ///
    /// Arguments:
    ///     properties (PropInput): The static properties of the graph.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    pub fn add_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        self.graph.add_constant_properties(properties)
    }

    /// Updates static properties to the graph.
    ///
    /// Arguments:
    ///     properties (PropInput): The static properties of the graph.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    pub fn update_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        self.graph.update_constant_properties(properties)
    }

    /// Adds a new edge with the given source and destination nodes and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (TimeInput): The timestamp of the edge.
    ///    src (str|int): The id of the source node.
    ///    dst (str|int): The id of the destination node.
    ///    properties (PropInput, optional): The properties of the edge, as a dict of string and properties.
    ///    layer (str, optional): The layer of the edge.
    ///    secondary_index (int, optional): The optional integer which will be used as a secondary index
    ///
    /// Returns:
    ///     MutableEdge: The added edge.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (timestamp, src, dst, properties = None, layer = None, secondary_index = None))]
    pub fn add_edge(
        &self,
        timestamp: PyTime,
        src: GID,
        dst: GID,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        secondary_index: Option<usize>,
    ) -> Result<EdgeView<Graph, Graph>, GraphError> {
        match secondary_index {
            None => self
                .graph
                .add_edge(timestamp, src, dst, properties.unwrap_or_default(), layer),
            Some(secondary_index) => self.graph.add_edge(
                (timestamp, secondary_index),
                src,
                dst,
                properties.unwrap_or_default(),
                layer,
            ),
        }
    }

    /// Import a single node into the graph.
    ///
    /// Arguments:
    ///     node (Node): A Node object representing the node to be imported.
    ///     merge (bool): An optional boolean flag. Defaults to False.
    ///                   If merge is False, the function will return an error if the imported node already exists in the graph.
    ///                   If merge is True, the function merges the histories of the imported node and the existing node (in the graph).
    ///
    /// Returns:
    ///     Node: A node object if the node was successfully imported.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (node, merge = false))]
    pub fn import_node(
        &self,
        node: PyNode,
        merge: bool,
    ) -> Result<NodeView<Graph, Graph>, GraphError> {
        self.graph.import_node(&node.node, merge)
    }

    /// Import a single node into the graph with new id.
    ///
    /// Arguments:
    ///     node (Node): A Node object representing the node to be imported.
    ///     new_id (str|int): The new node id.
    ///     merge (bool): An optional boolean flag. Defaults to False.
    ///                   If merge is False, the function will return an error if the imported node already exists in the graph.
    ///                   If merge is True, the function merges the histories of the imported node and the existing node (in the graph).
    ///
    /// Returns:
    ///     MutableNode: A node object if the node was successfully imported.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (node, new_id, merge = false))]
    pub fn import_node_as(
        &self,
        node: PyNode,
        new_id: GID,
        merge: bool,
    ) -> Result<NodeView<Graph, Graph>, GraphError> {
        self.graph.import_node_as(&node.node, new_id, merge)
    }

    /// Import multiple nodes into the graph.
    ///
    /// Arguments:
    ///     nodes (List[Node]): A vector of Node objects representing the nodes to be imported.
    ///     merge (bool): An optional boolean flag. Defaults to False.
    ///                   If merge is False, the function will return an error if any of the imported nodes already exists in the graph.
    ///                   If merge is True, the function merges the histories of the imported nodes and the existing nodes (in the graph).
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (nodes, merge = false))]
    pub fn import_nodes(&self, nodes: FromIterable<PyNode>, merge: bool) -> Result<(), GraphError> {
        let node_views = nodes.iter().map(|node| &node.node);
        self.graph.import_nodes(node_views, merge)
    }

    /// Import multiple nodes into the graph with new ids.
    ///
    /// Arguments:
    ///     nodes (List[Node]): A vector of Node objects representing the nodes to be imported.
    ///     new_ids (List[str|int]): A list of node IDs to use for the imported nodes.
    ///     merge (bool): An optional boolean flag. Defaults to False.
    ///                   If merge is True, the function will return an error if any of the imported nodes already exists in the graph.
    ///                   If merge is False, the function merges the histories of the imported nodes and the existing nodes (in the graph).
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (nodes, new_ids, merge = false))]
    pub fn import_nodes_as(
        &self,
        nodes: Vec<PyNode>,
        new_ids: Vec<GID>,
        merge: bool,
    ) -> Result<(), GraphError> {
        let node_views = nodes.iter().map(|node| &node.node);
        self.graph.import_nodes_as(node_views, new_ids, merge)
    }

    /// Import a single edge into the graph.
    ///
    /// Arguments:
    ///     edge (Edge): A Edge object representing the edge to be imported.
    ///     merge (bool): An optional boolean flag. Defaults to False.
    ///                   If merge is False, the function will return an error if the imported edge already exists in the graph.
    ///                   If merge is True, the function merges the histories of the imported edge and the existing edge (in the graph).
    ///
    /// Returns:
    ///     MutableEdge: An Edge object if the edge was successfully imported.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edge, merge = false))]
    pub fn import_edge(
        &self,
        edge: PyEdge,
        merge: bool,
    ) -> Result<EdgeView<Graph, Graph>, GraphError> {
        self.graph.import_edge(&edge.edge, merge)
    }

    /// Import a single edge into the graph with new id.
    ///
    /// Arguments:
    ///     edge (Edge): A Edge object representing the edge to be imported.
    ///     new_id (tuple) : The ID of the new edge. It's a tuple of the source and destination node ids.
    ///     merge (bool): An optional boolean flag. Defaults to False.
    ///                   If merge is False, the function will return an error if the imported edge already exists in the graph.
    ///                   If merge is True, the function merges the histories of the imported edge and the existing edge (in the graph).
    ///
    /// Returns:
    ///     Edge: An Edge object if the edge was successfully imported.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edge, new_id, merge = false))]
    pub fn import_edge_as(
        &self,
        edge: PyEdge,
        new_id: (GID, GID),
        merge: bool,
    ) -> Result<EdgeView<Graph, Graph>, GraphError> {
        self.graph.import_edge_as(&edge.edge, new_id, merge)
    }

    /// Import multiple edges into the graph.
    ///
    /// Arguments:
    ///     edges (List[Edge]): A list of Edge objects representing the edges to be imported.
    ///     merge (bool): An optional boolean flag. Defaults to False.
    ///                   If merge is False, the function will return an error if any of the imported edges already exists in the graph.
    ///                   If merge is True, the function merges the histories of the imported edges and the existing edges (in the graph).
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edges, merge = false))]
    pub fn import_edges(&self, edges: FromIterable<PyEdge>, merge: bool) -> Result<(), GraphError> {
        let edge_views = edges.iter().map(|edge| &edge.edge);
        self.graph.import_edges(edge_views, merge)
    }

    /// Import multiple edges into the graph with new ids.
    ///
    /// Arguments:
    ///     edges (List[Edge]): A list of Edge objects representing the edges to be imported.
    ///     new_ids (List[Tuple[int, int]]): The IDs of the new edges. It's a vector of tuples of the source and destination node ids.
    ///     merge (bool): An optional boolean flag. Defaults to False.
    ///                   If merge is False, the function will return an error if any of the imported edges already exists in the graph.
    ///                   If merge is True, the function merges the histories of the imported edges and the existing edges (in the graph).
    ///
    /// Returns:
    ///     None: This function does not return a value if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edges, new_ids, merge = false))]
    pub fn import_edges_as(
        &self,
        edges: Vec<PyEdge>,
        new_ids: Vec<(GID, GID)>,
        merge: bool,
    ) -> Result<(), GraphError> {
        let edge_views = edges.iter().map(|edge| &edge.edge);
        self.graph.import_edges_as(edge_views, new_ids, merge)
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the node with the specified id
    ///
    /// Arguments:
    ///   id (str|int): the node id
    ///
    /// Returns:
    ///   MutableNode: The node object with the specified id, or None if the node does not exist
    pub fn node(&self, id: PyNodeRef) -> Option<NodeView<Graph>> {
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
    ///     MutableEdge: the edge with the specified source and destination nodes, or None if the edge does not exist
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: PyNodeRef, dst: PyNodeRef) -> Option<EdgeView<Graph, Graph>> {
        self.graph.edge(src, dst)
    }

    //******  Saving And Loading  ******//

    // Alternative constructors are tricky, see: https://gist.github.com/redshiftzero/648e4feeff3843ffd9924f13625f839c

    /// Returns all the node types in the graph.
    ///
    /// Returns:
    ///     List[str]: the node types
    pub fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.graph.get_all_node_types()
    }

    /// Gives the large connected component of a graph.
    ///
    /// # Example Usage:
    /// g.largest_connected_component()
    ///
    /// Returns:
    ///     GraphView: sub-graph of the graph `g` containing the largest connected component
    ///
    pub fn largest_connected_component(&self) -> NodeSubgraph<Graph> {
        self.graph.largest_connected_component()
    }

    /// View graph with persistent semantics
    ///
    /// Returns:
    ///     PersistentGraph: the graph with persistent semantics applied
    pub fn persistent_graph<'py>(&'py self) -> PyResult<Py<PyPersistentGraph>> {
        PyPersistentGraph::py_from_db_graph(self.graph.persistent_graph())
    }

    /// View graph with event semantics
    ///
    /// Returns:
    ///     Graph: the graph with event semantics applied
    pub fn event_graph<'py>(&'py self) -> PyResult<Py<PyGraph>> {
        PyGraph::py_from_db_graph(self.graph.event_graph())
    }

    /// Load nodes from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (DataFrame): The Pandas DataFrame containing the nodes.
    ///     time (str): The column name for the timestamps.
    ///     id (str): The column name for the node IDs.
    ///     node_type (str, optional): A constant value to use as the node type for all nodes. Defaults to None. (cannot be used in combination with node_type_col)
    ///     node_type_col (str, optional): The node type col name in dataframe. Defaults to None. (cannot be used in combination with node_type)
    ///     properties (List[str], optional): List of node property column names. Defaults to None.
    ///     constant_properties (List[str], optional): List of constant node property column names. Defaults to None.
    ///     shared_constant_properties (PropInput, optional): A dictionary of constant properties that will be added to every node. Defaults to None.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (df, time, id, node_type = None, node_type_col = None, properties = None, constant_properties = None, shared_constant_properties = None)
    )]
    fn load_nodes_from_pandas<'py>(
        &self,
        df: &Bound<'py, PyAny>,
        time: &str,
        id: &str,
        node_type: Option<&str>,
        node_type_col: Option<&str>,
        properties: Option<Vec<PyBackedStr>>,
        constant_properties: Option<Vec<PyBackedStr>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        let properties = convert_py_prop_args(properties.as_deref()).unwrap_or_default();
        let constant_properties =
            convert_py_prop_args(constant_properties.as_deref()).unwrap_or_default();
        load_nodes_from_pandas(
            &self.graph,
            df,
            time,
            id,
            node_type,
            node_type_col,
            &properties,
            &constant_properties,
            shared_constant_properties.as_ref(),
        )
    }

    /// Load nodes from a Parquet file into the graph.
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files containing the nodes
    ///     time (str): The column name for the timestamps.
    ///     id (str): The column name for the node IDs.
    ///     node_type (str, optional): A constant value to use as the node type for all nodes. Defaults to None. (cannot be used in combination with node_type_col)
    ///     node_type_col (str, optional): The node type col name in dataframe. Defaults to None. (cannot be used in combination with node_type)
    ///     properties (List[str], optional): List of node property column names. Defaults to None.
    ///     constant_properties (List[str], optional): List of constant node property column names. Defaults to None.
    ///     shared_constant_properties (PropInput, optional): A dictionary of constant properties that will be added to every node. Defaults to None.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
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
        properties: Option<Vec<PyBackedStr>>,
        constant_properties: Option<Vec<PyBackedStr>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        let properties = convert_py_prop_args(properties.as_deref()).unwrap_or_default();
        let constant_properties =
            convert_py_prop_args(constant_properties.as_deref()).unwrap_or_default();
        load_nodes_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            time,
            id,
            node_type,
            node_type_col,
            &properties,
            &constant_properties,
            shared_constant_properties.as_ref(),
        )
    }

    /// Load edges from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (DataFrame): The Pandas DataFrame containing the edges.
    ///     time (str): The column name for the update timestamps.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     properties (List[str], optional): List of edge property column names. Defaults to None.
    ///     constant_properties (List[str], optional): List of constant edge property column names. Defaults to None.
    ///     shared_constant_properties (PropInput, optional): A dictionary of constant properties that will be added to every edge. Defaults to None.
    ///     layer (str, optional): A constant value to use as the layer for all edges. Defaults to None. (cannot be used in combination with layer_col)
    ///     layer_col (str, optional): The edge layer col name in dataframe. Defaults to None. (cannot be used in combination with layer)
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (df, time, src, dst, properties = None, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None)
    )]
    fn load_edges_from_pandas(
        &self,
        df: &Bound<PyAny>,
        time: &str,
        src: &str,
        dst: &str,
        properties: Option<Vec<PyBackedStr>>,
        constant_properties: Option<Vec<PyBackedStr>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        let properties = convert_py_prop_args(properties.as_deref()).unwrap_or_default();
        let constant_properties =
            convert_py_prop_args(constant_properties.as_deref()).unwrap_or_default();
        load_edges_from_pandas(
            &self.graph,
            df,
            time,
            src,
            dst,
            &properties,
            &constant_properties,
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
    ///     properties (List[str], optional): List of edge property column names. Defaults to None.
    ///     constant_properties (List[str], optional): List of constant edge property column names. Defaults to None.
    ///     shared_constant_properties (PropInput, optional): A dictionary of constant properties that will be added to every edge. Defaults to None.
    ///     layer (str, optional): A constant value to use as the layer for all edges. Defaults to None. (cannot be used in combination with layer_col)
    ///     layer_col (str, optional): The edge layer col name in dataframe. Defaults to None. (cannot be used in combination with layer)
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (parquet_path, time, src, dst, properties = None, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None)
    )]
    fn load_edges_from_parquet(
        &self,
        parquet_path: PathBuf,
        time: &str,
        src: &str,
        dst: &str,
        properties: Option<Vec<PyBackedStr>>,
        constant_properties: Option<Vec<PyBackedStr>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        let properties = convert_py_prop_args(properties.as_deref()).unwrap_or_default();
        let constant_properties =
            convert_py_prop_args(constant_properties.as_deref()).unwrap_or_default();
        load_edges_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            time,
            src,
            dst,
            &properties,
            &constant_properties,
            shared_constant_properties.as_ref(),
            layer,
            layer_col,
        )
    }

    /// Load node properties from a Pandas DataFrame.
    ///
    /// Arguments:
    ///     df (DataFrame): The Pandas DataFrame containing node information.
    ///     id(str): The column name for the node IDs.
    ///     node_type (str, optional): A constant value to use as the node type for all nodes. Defaults to None. (cannot be used in combination with node_type_col)
    ///     node_type_col (str, optional): The node type col name in dataframe. Defaults to None. (cannot be used in combination with node_type)
    ///     constant_properties (List[str], optional): List of constant node property column names. Defaults to None.
    ///     shared_constant_properties (PropInput, optional): A dictionary of constant properties that will be added to every node. Defaults to None.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (df, id, node_type = None, node_type_col = None, constant_properties = None, shared_constant_properties = None)
    )]
    fn load_node_props_from_pandas(
        &self,
        df: &Bound<PyAny>,
        id: &str,
        node_type: Option<&str>,
        node_type_col: Option<&str>,
        constant_properties: Option<Vec<PyBackedStr>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        let constant_properties =
            convert_py_prop_args(constant_properties.as_deref()).unwrap_or_default();
        load_node_props_from_pandas(
            &self.graph,
            df,
            id,
            node_type,
            node_type_col,
            &constant_properties,
            shared_constant_properties.as_ref(),
        )
    }

    /// Load node properties from a parquet file.
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files path containing node information.
    ///     id(str): The column name for the node IDs.
    ///     node_type (str, optional): A constant value to use as the node type for all nodes. Defaults to None. (cannot be used in combination with node_type_col)
    ///     node_type_col (str, optional): The node type col name in dataframe. Defaults to None. (cannot be used in combination with node_type)
    ///     constant_properties (List[str], optional): List of constant node property column names. Defaults to None.
    ///     shared_constant_properties (PropInput, optional): A dictionary of constant properties that will be added to every node. Defaults to None.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (parquet_path, id, node_type = None, node_type_col = None, constant_properties = None, shared_constant_properties = None)
    )]
    fn load_node_props_from_parquet(
        &self,
        parquet_path: PathBuf,
        id: &str,
        node_type: Option<&str>,
        node_type_col: Option<&str>,
        constant_properties: Option<Vec<PyBackedStr>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        let constant_properties =
            convert_py_prop_args(constant_properties.as_deref()).unwrap_or_default();
        load_node_props_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            id,
            node_type,
            node_type_col,
            &constant_properties,
            shared_constant_properties.as_ref(),
        )
    }

    /// Load edge properties from a Pandas DataFrame.
    ///
    /// Arguments:
    ///     df (DataFrame): The Pandas DataFrame containing edge information.
    ///     src (str): The column name for the source node.
    ///     dst (str): The column name for the destination node.
    ///     constant_properties (List[str], optional): List of constant edge property column names. Defaults to None.
    ///     shared_constant_properties (PropInput, optional): A dictionary of constant properties that will be added to every edge. Defaults to None.
    ///     layer (str, optional): The edge layer name. Defaults to None.
    ///     layer_col (str, optional): The edge layer col name in dataframe. Defaults to None.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (df, src, dst, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None)
    )]
    fn load_edge_props_from_pandas(
        &self,
        df: &Bound<PyAny>,
        src: &str,
        dst: &str,
        constant_properties: Option<Vec<PyBackedStr>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        let constant_properties =
            convert_py_prop_args(constant_properties.as_deref()).unwrap_or_default();
        load_edge_props_from_pandas(
            &self.graph,
            df,
            src,
            dst,
            &constant_properties,
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
    ///     constant_properties (List[str], optional): List of constant edge property column names. Defaults to None.
    ///     shared_constant_properties (PropInput, optional): A dictionary of constant properties that will be added to every edge. Defaults to None.
    ///     layer (str, optional): The edge layer name. Defaults to None.
    ///     layer_col (str, optional): The edge layer col name in dataframe. Defaults to None.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (parquet_path, src, dst, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None)
    )]
    fn load_edge_props_from_parquet(
        &self,
        parquet_path: PathBuf,
        src: &str,
        dst: &str,
        constant_properties: Option<Vec<PyBackedStr>>,
        shared_constant_properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        let constant_properties =
            convert_py_prop_args(constant_properties.as_deref()).unwrap_or_default();
        load_edge_props_from_parquet(
            &self.graph,
            parquet_path.as_path(),
            src,
            dst,
            &constant_properties,
            shared_constant_properties.as_ref(),
            layer,
            layer_col,
        )
    }

    /// Create graph index
    fn create_index(&self) -> Result<(), GraphError> {
        self.graph.create_index()
    }

    /// Create graph index with the provided index spec.
    fn create_index_with_spec(&self, py_spec: &PyIndexSpec) -> Result<(), GraphError> {
        self.graph.create_index_with_spec(py_spec.spec.clone())
    }

    /// Creates a graph index in memory (RAM).
    ///
    /// This is primarily intended for use in tests and should not be used in production environments,
    /// as the index will not be persisted to disk.
    fn create_index_in_ram(&self) -> Result<(), GraphError> {
        self.graph.create_index_in_ram()
    }

    /// Creates a graph index in memory (RAM) with the provided index spec.
    ///
    /// This is primarily intended for use in tests and should not be used in production environments,
    /// as the index will not be persisted to disk.
    fn create_index_in_ram_with_spec(&self, py_spec: &PyIndexSpec) -> Result<(), GraphError> {
        self.graph
            .create_index_in_ram_with_spec(py_spec.spec.clone())
    }
}
