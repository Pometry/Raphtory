//! Defines the `GraphWithDeletions` class, which represents a raphtory graph in memory.
//! Unlike in the `Graph` which has event semantics, `GraphWithDeletions` has edges that persist until explicitly deleted.
//!
//! This is the base class used to create a temporal graph, add nodes and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//! In Python, this class wraps around the rust graph.
use crate::{
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError, ArcStr, Prop},
    db::{
        api::{
            mutation::{AdditionOps, PropertyAdditionOps},
            view::internal::{CoreGraphOps, MaterializedGraph},
        },
        graph::{edge::EdgeView, node::NodeView, views::deletion_graph::GraphWithDeletions},
    },
    prelude::{DeletionOps, GraphViewOps, ImportOps},
    python::{
        graph::{edge::PyEdge, node::PyNode, views::graph_view::PyGraphView},
        utils::{PyInputNode, PyTime},
    },
};
use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyBytes},
};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
};

use super::{
    pandas::{
        dataframe::{process_pandas_py_df, GraphLoadException},
        loaders::load_edges_deletions_from_df,
    },
    utils,
};

/// A temporal graph that allows edges and nodes to be deleted.
#[derive(Clone)]
#[pyclass(name = "GraphWithDeletions", extends = PyGraphView)]
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
    ///    node_type (str) : The optional string which will be used as a node type
    ///
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None))]
    pub fn add_node(
        &self,
        timestamp: PyTime,
        id: PyInputNode,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
    ) -> Result<NodeView<GraphWithDeletions>, GraphError> {
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
    #[pyo3(signature = (node, force = false))]
    pub fn import_node(
        &self,
        node: PyNode,
        force: bool,
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
    #[pyo3(signature = (nodes, force = false))]
    pub fn import_nodes(
        &self,
        nodes: Vec<PyNode>,
        force: bool,
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
    #[pyo3(signature = (edge, force = false))]
    pub fn import_edge(
        &self,
        edge: PyEdge,
        force: bool,
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
    #[pyo3(signature = (edges, force = false))]
    pub fn import_edges(
        &self,
        edges: Vec<PyEdge>,
        force: bool,
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
    #[pyo3(signature = (path, force = false))]
    pub fn load_from_file(path: &str, force: bool) -> Result<GraphWithDeletions, GraphError> {
        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), path].iter().collect();
        GraphWithDeletions::load_from_file(file_path, force)
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
    #[pyo3(signature = (edge_df, edge_src, edge_dst, edge_time, edge_properties = None, edge_const_properties = None, edge_shared_const_properties = None,
    edge_layer = None, layer_in_df = true, node_df = None, node_id = None, node_time = None, node_properties = None,
    node_const_properties = None, node_shared_const_properties = None, node_type = None, node_type_in_df = true))]
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
    ) -> Result<GraphWithDeletions, GraphError> {
        let graph = PyGraphWithDeletions {
            graph: GraphWithDeletions::new(),
        };
        graph.load_edges_from_pandas(
            edge_df,
            edge_src,
            edge_dst,
            edge_time,
            edge_properties,
            edge_const_properties,
            edge_shared_const_properties,
            edge_layer,
            layer_in_df,
        )?;
        if let (Some(node_df), Some(node_id), Some(node_time)) = (node_df, node_id, node_time) {
            graph.load_nodes_from_pandas(
                node_df,
                node_id,
                node_time,
                node_type,
                node_type_in_df,
                node_properties,
                node_const_properties,
                node_shared_const_properties,
            )?;
        }
        Ok(graph.graph)
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
    #[pyo3(signature = (df, id, time, node_type = None, node_type_in_df = true, properties = None, const_properties = None, shared_const_properties = None))]
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
        utils::load_nodes_from_pandas(
            &self.graph.0,
            df,
            id,
            time,
            node_type,
            node_type_in_df,
            properties,
            const_properties,
            shared_const_properties,
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
    #[pyo3(signature = (df, src, dst, time, properties = None, const_properties = None, shared_const_properties = None, layer = None, layer_in_df = true))]
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
        utils::load_edges_from_pandas(
            &self.graph.0,
            df,
            src,
            dst,
            time,
            properties,
            const_properties,
            shared_const_properties,
            layer,
            layer_in_df,
        )
    }

    /// Load edges deletions from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing the edges.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     time (str): The column name for the update timestamps.
    ///     layer (str): The edge layer name (optional) Defaults to None.
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the dateframe or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(signature = (df, src, dst, time, layer = None, layer_in_df = true))]
    fn load_edges_deletions_from_pandas(
        &self,
        df: &PyAny,
        src: &str,
        dst: &str,
        time: &str,
        layer: Option<&str>,
        layer_in_df: Option<bool>,
    ) -> Result<(), GraphError> {
        let graph = &self.graph.0;
        Python::with_gil(|py| {
            let size: usize = py
                .eval(
                    "index.__len__()",
                    Some([("index", df.getattr("index")?)].into_py_dict(py)),
                    None,
                )?
                .extract()?;

            let mut cols_to_check = vec![src, dst, time];
            if layer_in_df.unwrap_or(true) {
                if let Some(ref layer) = layer {
                    cols_to_check.push(layer.as_ref());
                }
            }

            let df = process_pandas_py_df(df, py, size, cols_to_check.clone())?;

            df.check_cols_exist(&cols_to_check)?;
            load_edges_deletions_from_df(
                &df,
                size,
                src,
                dst,
                time,
                layer,
                layer_in_df.unwrap_or(true),
                graph,
            )
            .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;

            Ok::<(), PyErr>(())
        })
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
        Ok(())
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
        utils::load_node_props_from_pandas(
            &self.graph.0,
            df,
            id,
            const_properties,
            shared_const_properties,
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
    #[pyo3(signature = (df, src, dst, const_properties = None, shared_const_properties = None, layer = None, layer_in_df = true))]
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
        utils::load_edge_props_from_pandas(
            &self.graph.0,
            df,
            src,
            dst,
            const_properties,
            shared_const_properties,
            layer,
            layer_in_df,
        )
    }
}
