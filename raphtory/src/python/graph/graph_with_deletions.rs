//! Defines the `PersistentGraph` class, which represents a raphtory graph in memory.
//! Unlike in the `Graph` which has event semantics, `PersistentGraph` has edges that persist until explicitly deleted.
//!
//! This is the base class used to create a temporal graph, add nodes and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//! In Python, this class wraps around the rust graph.
use super::{
    graph::{PyGraph, PyGraphEncoder},
    io::pandas_loaders::*,
};
#[cfg(feature = "storage")]
use crate::disk_graph::DiskGraphStorage;
use crate::{
    core::{utils::errors::GraphError, Prop},
    db::{
        api::{
            mutation::{AdditionOps, PropertyAdditionOps},
            view::internal::CoreGraphOps,
        },
        graph::{edge::EdgeView, node::NodeView, views::deletion_graph::PersistentGraph},
    },
    io::parquet_loaders::*,
    prelude::{DeletionOps, GraphViewOps, ImportOps, IndexMutationOps},
    python::{
        graph::{edge::PyEdge, index::PyIndexSpec, node::PyNode, views::graph_view::PyGraphView},
        utils::{PyNodeRef, PyTime},
    },
    serialise::StableEncode,
};
use pyo3::{prelude::*, pybacked::PyBackedStr};
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::PathBuf,
};

/// A temporal graph that allows edges and nodes to be deleted.
#[derive(Clone)]
#[pyclass(name = "PersistentGraph", extends = PyGraphView, frozen, module="raphtory")]
pub struct PyPersistentGraph {
    pub(crate) graph: PersistentGraph,
}

impl_serialise!(PyPersistentGraph, graph: PersistentGraph, "PersistentGraph");

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

impl<'py> IntoPyObject<'py> for PersistentGraph {
    type Target = PyPersistentGraph;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(
            py,
            (
                PyPersistentGraph::from(self.clone()),
                PyGraphView::from(self),
            ),
        )
    }
}

impl<'source> FromPyObject<'source> for PersistentGraph {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        let g = ob.downcast::<PyPersistentGraph>()?.get();
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

    #[cfg(feature = "storage")]
    pub fn to_disk_graph(&self, graph_dir: PathBuf) -> Result<PersistentGraph, GraphError> {
        use crate::db::api::storage::graph::storage_ops::GraphStorage;
        use std::sync::Arc;

        let disk_graph = DiskGraphStorage::from_graph(&self.graph.event_graph(), graph_dir)?;
        let storage = GraphStorage::Disk(Arc::new(disk_graph));
        let graph = PersistentGraph::from_internal_graph(storage);
        Ok(graph)
    }

    fn __reduce__(&self) -> (PyGraphEncoder, (Vec<u8>,)) {
        let state = self.graph.encode_to_vec();
        (PyGraphEncoder, (state,))
    }

    /// Adds a new node with the given id and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (TimeInput): The timestamp of the node.
    ///    id (str | int): The id of the node.
    ///    properties (PropInput, optional): The properties of the node.
    ///    node_type (str, optional) : The optional string which will be used as a node type
    ///    secondary_index (int, optional): The optional integer which will be used as a secondary index
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None, secondary_index = None))]
    pub fn add_node(
        &self,
        timestamp: PyTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
        secondary_index: Option<usize>,
    ) -> Result<NodeView<PersistentGraph>, GraphError> {
        match secondary_index {
            None => self
                .graph
                .add_node(timestamp, id, properties.unwrap_or_default(), node_type),
            Some(secondary_index) => self.graph.add_node(
                (timestamp, secondary_index),
                id,
                properties.unwrap_or_default(),
                node_type,
            ),
        }
    }

    /// Creates a new node with the given id and properties to the graph. It fails if the node already exists.
    ///
    /// Arguments:
    ///    timestamp (TimeInput): The timestamp of the node.
    ///    id (str | int): The id of the node.
    ///    properties (PropInput, optional): The properties of the node.
    ///    node_type (str, optional) : The optional string which will be used as a node type
    ///    secondary_index (int, optional): The optional integer which will be used as a secondary index
    ///
    /// Returns:
    ///   MutableNode: the newly created node.
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
    ) -> Result<NodeView<PersistentGraph>, GraphError> {
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
    ///    properties (dict): The temporal properties of the graph.
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
    ///     properties (dict): The static properties of the graph.
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
    ///     properties (dict): The static properties of the graph.
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
    ///    timestamp (int): The timestamp of the edge.
    ///    src (str | int): The id of the source node.
    ///    dst (str | int): The id of the destination node.
    ///    properties (PropInput, optional): The properties of the edge, as a dict of string and properties
    ///    layer (str, optional): The layer of the edge.
    ///    secondary_index (int, optional): The optional integer which will be used as a secondary index
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
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
    ) -> Result<EdgeView<PersistentGraph, PersistentGraph>, GraphError> {
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

    /// Deletes an edge given the timestamp, src and dst nodes and layer (optional)
    ///
    /// Arguments:
    ///   timestamp (int): The timestamp of the edge.
    ///   src (str | int): The id of the source node.
    ///   dst (str | int): The id of the destination node.
    ///   layer (str, optional): The layer of the edge.
    ///   secondary_index (int, optional): The optional integer which will be used as a secondary index.
    ///
    /// Returns:
    ///   MutableEdge: The deleted edge
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (timestamp, src, dst, layer=None, secondary_index = None))]
    pub fn delete_edge(
        &self,
        timestamp: PyTime,
        src: GID,
        dst: GID,
        layer: Option<&str>,
        secondary_index: Option<usize>,
    ) -> Result<EdgeView<PersistentGraph>, GraphError> {
        match secondary_index {
            None => self.graph.delete_edge(timestamp, src, dst, layer),
            Some(secondary_index) => {
                self.graph
                    .delete_edge((timestamp, secondary_index), src, dst, layer)
            }
        }
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the node with the specified id
    ///
    /// Arguments:
    ///   id (str | int): the node id
    ///
    /// Returns:
    ///   Optional[MutableNode]: The node with the specified id, or None if the node does not exist
    pub fn node(&self, id: PyNodeRef) -> Option<NodeView<PersistentGraph>> {
        self.graph.node(id)
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the edge with the specified source and destination nodes
    ///
    /// Arguments:
    ///     src (str | int): the source node id
    ///     dst (str | int): the destination node id
    ///
    /// Returns:
    ///     Optional[MutableEdge]: The edge with the specified source and destination nodes, or None if the edge does not exist
    #[pyo3(signature = (src, dst))]
    pub fn edge(
        &self,
        src: PyNodeRef,
        dst: PyNodeRef,
    ) -> Option<EdgeView<PersistentGraph, PersistentGraph>> {
        self.graph.edge(src, dst)
    }

    /// Import a single node into the graph.
    ///
    /// This function takes a node object and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the node even if it already exists in the graph.
    ///
    /// Arguments:
    ///     node (Node): A node object representing the node to be imported.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the node. Defaults to False.
    ///
    /// Returns:
    ///     Node: A Node object if the node was successfully imported, and an error otherwise.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (node, merge = false))]
    pub fn import_node(
        &self,
        node: PyNode,
        merge: bool,
    ) -> Result<NodeView<PersistentGraph, PersistentGraph>, GraphError> {
        self.graph.import_node(&node.node, merge)
    }

    /// Import a single node into the graph with new id.
    ///
    /// This function takes a node object, a new node id and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the node even if it already exists in the graph.
    ///
    /// Arguments:
    ///     node (Node): A node object representing the node to be imported.
    ///     new_id (str|int): The new node id.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the node. Defaults to False.
    ///
    /// Returns:
    ///     Node: A Node object if the node was successfully imported, and an error otherwise.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (node, new_id, merge = false))]
    pub fn import_node_as(
        &self,
        node: PyNode,
        new_id: GID,
        merge: bool,
    ) -> Result<NodeView<PersistentGraph, PersistentGraph>, GraphError> {
        self.graph.import_node_as(&node.node, new_id, merge)
    }

    /// Import multiple nodes into the graph.
    ///
    /// This function takes a vector of node objects and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the nodes even if they already exist in the graph.
    ///
    /// Arguments:
    ///     nodes (List[Node]):  A vector of node objects representing the nodes to be imported.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the nodes. Defaults to False.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (nodes, merge = false))]
    pub fn import_nodes(&self, nodes: Vec<PyNode>, merge: bool) -> Result<(), GraphError> {
        let node_views = nodes.iter().map(|node| &node.node);
        self.graph.import_nodes(node_views, merge)
    }

    /// Import multiple nodes into the graph with new ids.
    ///
    /// This function takes a vector of node objects, a list of new node ids and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the nodes even if they already exist in the graph.
    ///
    /// Arguments:
    ///     nodes (List[Node]):  A vector of node objects representing the nodes to be imported.
    ///     new_ids (List[str|int]): A list of node IDs to use for the imported nodes.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the nodes. Defaults to False.
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
    /// This function takes an edge object and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the edge even if it already exists in the graph.
    ///
    /// Arguments:
    ///     edge (Edge): An edge object representing the edge to be imported.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the edge. Defaults to False.
    ///
    /// Returns:
    ///     Edge: The imported edge.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edge, merge = false))]
    pub fn import_edge(
        &self,
        edge: PyEdge,
        merge: bool,
    ) -> Result<EdgeView<PersistentGraph, PersistentGraph>, GraphError> {
        self.graph.import_edge(&edge.edge, merge)
    }

    /// Import a single edge into the graph with new id.
    ///
    /// This function takes a edge object, a new edge id and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the edge even if it already exists in the graph.
    ///
    /// Arguments:
    ///     edge (Edge): A edge object representing the edge to be imported.
    ///     new_id (tuple) : The ID of the new edge. It's a tuple of the source and destination node ids.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the edge. Defaults to False.
    ///
    /// Returns:
    ///     Edge: The imported edge.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edge, new_id, merge = false))]
    pub fn import_edge_as(
        &self,
        edge: PyEdge,
        new_id: (GID, GID),
        merge: bool,
    ) -> Result<EdgeView<PersistentGraph, PersistentGraph>, GraphError> {
        self.graph.import_edge_as(&edge.edge, new_id, merge)
    }

    /// Import multiple edges into the graph.
    ///
    /// This function takes a vector of edge objects and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the edges even if they already exist in the graph.
    ///
    /// Arguments:
    ///     edges (List[Edge]): A vector of edge objects representing the edges to be imported.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the edges. Defaults to False.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edges, merge = false))]
    pub fn import_edges(&self, edges: Vec<PyEdge>, merge: bool) -> Result<(), GraphError> {
        let edge_views = edges.iter().map(|edge| &edge.edge);
        self.graph.import_edges(edge_views, merge)
    }

    /// Import multiple edges into the graph with new ids.
    ///
    /// This function takes a vector of edge objects, a list of new edge ids and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the edges even if they already exist in the graph.
    ///
    /// Arguments:
    ///     edges (List[Edge]): A vector of edge objects representing the edges to be imported.
    ///     new_ids (list[Tuple[GID, GID]]): The new edge ids
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the edges. Defaults to False.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
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

    //******  Saving And Loading  ******//

    // Alternative constructors are tricky, see: https://gist.github.com/redshiftzero/648e4feeff3843ffd9924f13625f839c

    /// Returns all the node types in the graph.
    ///
    /// Returns:
    ///     list[str]: A list of node types
    pub fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.graph.get_all_node_types()
    }

    /// Get event graph
    ///
    /// Returns:
    ///     Graph: the graph with event semantics applied
    pub fn event_graph<'py>(&'py self) -> PyResult<Py<PyGraph>> {
        PyGraph::py_from_db_graph(self.graph.event_graph())
    }

    /// Get persistent graph
    ///
    /// Returns:
    ///     PersistentGraph: the graph with persistent semantics applied
    pub fn persistent_graph<'py>(&'py self) -> PyResult<Py<PyPersistentGraph>> {
        PyPersistentGraph::py_from_db_graph(self.graph.persistent_graph())
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
    #[pyo3(signature = (df,time,id, node_type = None, node_type_col = None, properties = None, constant_properties = None, shared_constant_properties = None))]
    fn load_nodes_from_pandas(
        &self,
        df: &Bound<PyAny>,
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
    #[pyo3(signature = (parquet_path, time,id, node_type = None, node_type_col = None, properties = None, constant_properties = None, shared_constant_properties = None))]
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
    #[pyo3(signature = (df, time, src, dst, properties = None, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None))]
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
    #[pyo3(signature = (parquet_path, time, src, dst, properties = None, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None))]
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

    /// Load edges deletions from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (DataFrame): The Pandas DataFrame containing the edges.
    ///     time (str): The column name for the update timestamps.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     layer (str, optional): A constant value to use as the layer for all edges. Defaults to None. (cannot be used in combination with layer_col)
    ///     layer_col (str, optional): The edge layer col name in dataframe. Defaults to None. (cannot be used in combination with layer)
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (df, time, src, dst, layer = None, layer_col = None))]
    fn load_edge_deletions_from_pandas(
        &self,
        df: &Bound<PyAny>,
        time: &str,
        src: &str,
        dst: &str,
        layer: Option<&str>,
        layer_col: Option<&str>,
    ) -> Result<(), GraphError> {
        load_edge_deletions_from_pandas(&self.graph, df, time, src, dst, layer, layer_col)
    }

    /// Load edges deletions from a Parquet file into the graph.
    ///
    /// Arguments:
    ///     parquet_path (str): Parquet file or directory of Parquet files path containing node information.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     time (str): The column name for the update timestamps.
    ///     layer (str, optional): A constant value to use as the layer for all edges. Defaults to None. (cannot be used in combination with layer_col)
    ///     layer_col (str, optional): The edge layer col name in dataframe. Defaults to None. (cannot be used in combination with layer)
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
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
    #[pyo3(signature = (df, id, node_type=None, node_type_col=None, constant_properties = None, shared_constant_properties = None))]
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
    #[pyo3(signature = (parquet_path, id, node_type = None, node_type_col=None, constant_properties = None, shared_constant_properties = None))]
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
    #[pyo3(signature = (df, src, dst, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None))]
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
    #[pyo3(signature = (parquet_path, src, dst, constant_properties = None, shared_constant_properties = None, layer = None, layer_col = None))]
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
