//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add nodes and edges,
//! create windows, and query the graph with a variety of algorithms.
//! In Python, this class wraps around the rust graph.
use crate::{
    core::{utils::errors::GraphError, ArcStr},
    db::api::view::{internal::MaterializedGraph, BaseNodeViewOps},
    prelude::*,
    python::{
        graph::{graph_with_deletions::PyGraphWithDeletions, views::graph_view::PyGraphView},
        utils::{PyInputNode, PyTime},
    },
};
use pyo3::{prelude::*, types::PyDict};

use crate::{
    core::entities::nodes::node_ref::NodeRef,
    db::{
        api::view::internal::{DynamicGraph, IntoDynamic},
        graph::{edge::EdgeView, edges::Edges, node::NodeView},
    },
    python::graph::pandas::{
        dataframe::{process_pandas_py_df, GraphLoadException},
        loaders::{load_edges_props_from_df, load_node_props_from_df},
    },
};
use pyo3::types::{IntoPyDict, PyBytes};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
};

use super::pandas::loaders::{load_edges_from_df, load_nodes_from_df};

/// A temporal graph.
#[derive(Clone)]
#[pyclass(name="Graph", extends=PyGraphView)]
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

impl From<PyGraphWithDeletions> for MaterializedGraph {
    fn from(value: PyGraphWithDeletions) -> Self {
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
        } else if let Ok(graph) = graph.extract::<PyRef<PyGraphWithDeletions>>() {
            Ok(graph.graph.clone().into())
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Incorrect type, object is not a PyGraph or PyGraphWithDeletions"
            )))
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

    /// Adds a new node with the given id and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the node.
    ///    id (str or int): The id of the node.
    ///    properties (dict): The properties of the node (optional).
    ///
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, id, properties=None))]
    pub fn add_node(
        &self,
        timestamp: PyTime,
        id: PyInputNode,
        properties: Option<HashMap<String, Prop>>,
    ) -> Result<NodeView<Graph, Graph>, GraphError> {
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
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the edge.
    ///    src (str or int): The id of the source node.
    ///    dst (str or int): The id of the destination node.
    ///    properties (dict): The properties of the edge, as a dict of string and properties (optional).
    ///    layer (str): The layer of the edge (optional).
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
    ) -> Result<EdgeView<Graph, Graph>, GraphError> {
        self.graph
            .add_edge(timestamp, src, dst, properties.unwrap_or_default(), layer)
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
    pub fn load_from_file(path: &str) -> Result<Graph, GraphError> {
        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), path].iter().collect();
        Graph::load_from_file(file_path)
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

    /// Load a graph from a Pandas DataFrame.
    ///
    /// Args:
    ///     edge_df (pandas.DataFrame): The DataFrame containing the edges.
    ///     edge_src (str): The column name for the source node ids.
    ///     edge_dst (str): The column name for the destination node ids.
    ///     edge_time (str): The column name for the timestamps.
    ///     edge_props (list): The column names for the temporal properties (optional) Defaults to None.
    ///     edge_const_props (list): The column names for the constant properties (optional) Defaults to None.
    ///     edge_shared_const_props (dict): A dictionary of constant properties that will be added to every edge (optional) Defaults to None.
    ///     edge_layer (str): The edge layer name (optional) Defaults to None.
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the edge_df or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///     node_df (pandas.DataFrame): The DataFrame containing the nodes (optional) Defaults to None.
    ///     node_id (str): The column name for the node ids (optional) Defaults to None.
    ///     node_time (str): The column name for the node timestamps (optional) Defaults to None.
    ///     node_props (list): The column names for the node temporal properties (optional) Defaults to None.
    ///     node_const_props (list): The column names for the node constant properties (optional) Defaults to None.
    ///     node_shared_const_props (dict): A dictionary of constant properties that will be added to every node (optional) Defaults to None.
    ///
    /// Returns:
    ///      Graph: The loaded Graph object.
    #[staticmethod]
    #[pyo3(signature = (edge_df, edge_src, edge_dst, edge_time, edge_props = None, edge_const_props=None, edge_shared_const_props=None,
    edge_layer = None, layer_in_df = true, node_df = None, node_id = None, node_time = None, node_props = None, node_const_props = None, node_shared_const_props = None))]
    fn load_from_pandas(
        edge_df: &PyAny,
        edge_src: &str,
        edge_dst: &str,
        edge_time: &str,
        edge_props: Option<Vec<&str>>,
        edge_const_props: Option<Vec<&str>>,
        edge_shared_const_props: Option<HashMap<String, Prop>>,
        edge_layer: Option<&str>,
        layer_in_df: Option<bool>,
        node_df: Option<&PyAny>,
        node_id: Option<&str>,
        node_time: Option<&str>,
        node_props: Option<Vec<&str>>,
        node_const_props: Option<Vec<&str>>,
        node_shared_const_props: Option<HashMap<String, Prop>>,
    ) -> Result<Graph, GraphError> {
        let graph = PyGraph {
            graph: Graph::new(),
        };
        graph.load_edges_from_pandas(
            edge_df,
            edge_src,
            edge_dst,
            edge_time,
            edge_props,
            edge_const_props,
            edge_shared_const_props,
            edge_layer,
            layer_in_df,
        )?;
        if let (Some(node_df), Some(node_id), Some(node_time)) = (node_df, node_id, node_time) {
            graph.load_nodes_from_pandas(
                node_df,
                node_id,
                node_time,
                node_props,
                node_const_props,
                node_shared_const_props,
            )?;
        }
        Ok(graph.graph)
    }

    /// Converts the graph's nodes into a Pandas DataFrame.
    ///
    /// This method will create a DataFrame with the following columns:
    /// - "name": The name of the node.
    /// - "properties": The properties of the node. This column will be included if `include_node_properties` is set to `true`.
    /// - "property_history": The history of the node's properties. This column will be included if both `include_node_properties` and `include_property_histories` are set to `true`.
    /// - "update_history": The update history of the node. This column will be included if `include_update_history` is set to `true`.
    ///
    /// Args:
    ///     include_node_properties (bool): A boolean wrapped in an Option. If set to `true`, the "properties" and "property_history" columns will be included in the DataFrame. Defaults to `true`.
    ///     include_update_history (bool): A boolean wrapped in an Option. If set to `true`, the "update_history" column will be included in the DataFrame. Defaults to `true`.
    ///     include_property_histories (bool): A boolean wrapped in an Option. If set to `true`, the "property_history" column will be included in the DataFrame. Defaults to `true`.
    ///
    /// Returns:
    ///     If successful, this PyObject will be a Pandas DataFrame.
    #[pyo3(signature = (include_node_properties=true, include_update_history=true, include_property_histories=true))]
    pub fn to_node_df(
        &self,
        include_node_properties: Option<bool>,
        include_update_history: Option<bool>,
        include_property_histories: Option<bool>,
    ) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let pandas = PyModule::import(py, "pandas")?;
            let column_names = vec!["name", "properties", "property_history", "update_history"];
            let node_tuples: Vec<_> = self
                .graph
                .nodes()
                .map(|g, b| {
                    let v = g.node(b).unwrap();
                    let mut properties: Option<HashMap<ArcStr, Prop>> = None;
                    let mut temporal_properties: Option<Vec<(ArcStr, (i64, Prop))>> = None;
                    let mut update_history: Option<Vec<_>> = None;
                    if include_node_properties == Some(true) {
                        if include_property_histories == Some(true) {
                            properties = Some(v.properties().constant().as_map());
                            temporal_properties = Some(v.properties().temporal().histories());
                        } else {
                            properties = Some(v.properties().as_map());
                        }
                    }
                    if include_update_history == Some(true) {
                        update_history = Some(v.history());
                    }
                    (v.name(), properties, temporal_properties, update_history)
                })
                .collect();
            let kwargs = PyDict::new(py);
            kwargs.set_item("columns", column_names)?;
            let df = pandas.call_method("DataFrame", (node_tuples,), Some(kwargs))?;
            let kwargs_drop = PyDict::new(py);
            kwargs_drop.set_item("how", "all")?;
            kwargs_drop.set_item("axis", 1)?;
            kwargs_drop.set_item("inplace", true)?;
            df.call_method("dropna", (), Some(kwargs_drop))?;
            Ok(df.to_object(py))
        })
    }

    /// Converts the graph's edges into a Pandas DataFrame.
    ///
    /// This method will create a DataFrame with the following columns:
    /// - "src": The source node of the edge.
    /// - "dst": The destination node of the edge.
    /// - "layer": The layer of the edge.
    /// - "properties": The properties of the edge. This column will be included if `include_edge_properties` is set to `true`.
    /// - "property_histories": The history of the edge's properties. This column will be included if both `include_edge_properties` and `include_property_histories` are set to `true`.
    /// - "update_history": The update history of the edge. This column will be included if `include_update_history` is set to `true`.
    /// - "update_history_exploded": The exploded update history of the edge. This column will be included if `explode_edges` is set to `true`.
    ///
    /// Args:
    ///     explode_edges (bool): A boolean wrapped in an Option. If set to `true`, the "update_history_exploded" column will be included in the DataFrame. Defaults to `false`.
    ///     include_edge_properties (bool): A boolean wrapped in an Option. If set to `true`, the "properties" and "property_histories" columns will be included in the DataFrame. Defaults to `true`.
    ///     include_update_history (bool): A boolean wrapped in an Option. If set to `true`, the "update_history" column will be included in the DataFrame. Defaults to `true`.
    ///     include_property_histories (bool): A boolean wrapped in an Option. If set to `true`, the "property_histories" column will be included in the DataFrame. Defaults to `true`.
    ///
    /// Returns:
    ///     If successful, this PyObject will be a Pandas DataFrame.
    #[pyo3(signature = (explode_edges=false, include_edge_properties=true, include_update_history=true, include_property_histories=true))]
    pub fn to_edge_df(
        &self,
        explode_edges: Option<bool>,
        include_edge_properties: Option<bool>,
        include_update_history: Option<bool>,
        include_property_histories: Option<bool>,
    ) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let pandas = PyModule::import(py, "pandas")?;
            let column_names = vec![
                "src",
                "dst",
                "layer",
                "properties",
                "property_histories",
                "update_history",
                "update_history_exploded",
            ];
            let mut edges: Edges<Graph, Graph> = self.graph.edges();
            if explode_edges == Some(true) {
                edges = self.graph.edges().explode_layers().explode();
            }
            let edge_tuples: Vec<_> = edges
                .iter()
                .map(|e| {
                    let mut properties: Option<HashMap<ArcStr, Prop>> = None;
                    let mut temporal_properties: Option<Vec<(ArcStr, (i64, Prop))>> = None;
                    if include_edge_properties == Some(true) {
                        if include_property_histories == Some(true) {
                            properties = Some(e.properties().constant().as_map());
                            temporal_properties = Some(e.properties().temporal().histories());
                        } else {
                            properties = Some(e.properties().as_map());
                        }
                    }
                    let mut update_history_exploded: Option<i64> = None;
                    let mut update_history: Option<Vec<_>> = None;
                    if include_update_history == Some(true) {
                        if explode_edges == Some(true) {
                            update_history_exploded = e.time();
                        } else {
                            update_history = Some(e.history());
                        }
                    }
                    (
                        e.src().name(),
                        e.dst().name(),
                        e.layer_name(),
                        properties,
                        temporal_properties,
                        update_history,
                        update_history_exploded,
                    )
                })
                .collect();
            let kwargs = PyDict::new(py);
            kwargs.set_item("columns", column_names)?;
            let df = pandas.call_method("DataFrame", (edge_tuples,), Some(kwargs))?;
            let kwargs_drop = PyDict::new(py);
            kwargs_drop.set_item("how", "all")?;
            kwargs_drop.set_item("axis", 1)?;
            kwargs_drop.set_item("inplace", true)?;
            df.call_method("dropna", (), Some(kwargs_drop))?;
            Ok(df.to_object(py))
        })
    }

    /// Load nodes from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (pandas.DataFrame): The Pandas DataFrame containing the nodes.
    ///     id (str): The column name for the node IDs.
    ///     time (str): The column name for the timestamps.
    ///     props (List<str>): List of node property column names. Defaults to None. (optional)
    ///     const_props (List<str>): List of constant node property column names. Defaults to None.  (optional)
    ///     shared_const_props (Dictionary/Hashmap of properties): A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(signature = (df, id, time, props = None, const_props = None, shared_const_props = None))]
    fn load_nodes_from_pandas(
        &self,
        df: &PyAny,
        id: &str,
        time: &str,
        props: Option<Vec<&str>>,
        const_props: Option<Vec<&str>>,
        shared_const_props: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        let graph = &self.graph;
        Python::with_gil(|py| {
            let size: usize = py
                .eval(
                    "index.__len__()",
                    Some([("index", df.getattr("index")?)].into_py_dict(py)),
                    None,
                )?
                .extract()?;

            let mut cols_to_check = vec![id, time];
            cols_to_check.extend(props.as_ref().unwrap_or(&Vec::new()));
            cols_to_check.extend(const_props.as_ref().unwrap_or(&Vec::new()));

            let df = process_pandas_py_df(df, py, size, cols_to_check.clone())?;
            df.check_cols_exist(&cols_to_check)?;

            load_nodes_from_df(
                &df,
                size,
                id,
                time,
                props,
                const_props,
                shared_const_props,
                graph,
            )
            .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;
            Ok::<(), PyErr>(())
        })
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
        Ok(())
    }

    /// Load edges from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing the edges.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     time (str): The column name for the update timestamps.
    ///     props (List<str>): List of edge property column names. Defaults to None. (optional)
    ///     const_props (List<str>): List of constant edge property column names. Defaults to None. (optional)
    ///     shared_const_props (dict): A dictionary of constant properties that will be added to every edge. Defaults to None. (optional)
    ///     edge_layer (str): The edge layer name (optional) Defaults to None.
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the dateframe or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(signature = (df, src, dst, time, props = None, const_props=None,shared_const_props=None,layer=None,layer_in_df=true))]
    fn load_edges_from_pandas(
        &self,
        df: &PyAny,
        src: &str,
        dst: &str,
        time: &str,
        props: Option<Vec<&str>>,
        const_props: Option<Vec<&str>>,
        shared_const_props: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_in_df: Option<bool>,
    ) -> Result<(), GraphError> {
        let graph = &self.graph;
        Python::with_gil(|py| {
            let size: usize = py
                .eval(
                    "index.__len__()",
                    Some([("index", df.getattr("index")?)].into_py_dict(py)),
                    None,
                )?
                .extract()?;

            let mut cols_to_check = vec![src, dst, time];
            cols_to_check.extend(props.as_ref().unwrap_or(&Vec::new()));
            cols_to_check.extend(const_props.as_ref().unwrap_or(&Vec::new()));
            if layer_in_df.unwrap_or(false) {
                if let Some(ref layer) = layer {
                    cols_to_check.push(layer.as_ref());
                }
            }

            let df = process_pandas_py_df(df, py, size, cols_to_check.clone())?;

            df.check_cols_exist(&cols_to_check)?;
            load_edges_from_df(
                &df,
                size,
                src,
                dst,
                time,
                props,
                const_props,
                shared_const_props,
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
    ///     const_props (List<str>): List of constant node property column names. Defaults to None. (optional)
    ///     shared_const_props (<HashMap<String, Prop>>):  A dictionary of constant properties that will be added to every node. Defaults to None. (optional)
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(signature = (df, id , const_props = None, shared_const_props = None))]
    fn load_node_props_from_pandas(
        &self,
        df: &PyAny,
        id: &str,
        const_props: Option<Vec<&str>>,
        shared_const_props: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        let graph = &self.graph;
        Python::with_gil(|py| {
            let size: usize = py
                .eval(
                    "index.__len__()",
                    Some([("index", df.getattr("index")?)].into_py_dict(py)),
                    None,
                )?
                .extract()?;
            let mut cols_to_check = vec![id];
            cols_to_check.extend(const_props.as_ref().unwrap_or(&Vec::new()));
            let df = process_pandas_py_df(df, py, size, cols_to_check.clone())?;
            df.check_cols_exist(&cols_to_check)?;

            load_node_props_from_df(&df, size, id, const_props, shared_const_props, graph)
                .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;

            Ok::<(), PyErr>(())
        })
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
        Ok(())
    }

    /// Load edge properties from a Pandas DataFrame.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing edge information.
    ///     src (str): The column name for the source node.
    ///     dst (str): The column name for the destination node.
    ///     const_props (List<str>): List of constant edge property column names. Defaults to None. (optional)
    ///     shared_const_props (dict): A dictionary of constant properties that will be added to every edge. Defaults to None. (optional)
    ///     layer (str): Layer name. Defaults to None.  (optional)
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the data frame or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(signature = (df, src, dst, const_props=None,shared_const_props=None,layer=None,layer_in_df=true))]
    fn load_edge_props_from_pandas(
        &self,
        df: &PyAny,
        src: &str,
        dst: &str,
        const_props: Option<Vec<&str>>,
        shared_const_props: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_in_df: Option<bool>,
    ) -> Result<(), GraphError> {
        let graph = &self.graph;
        Python::with_gil(|py| {
            let size: usize = py
                .eval(
                    "index.__len__()",
                    Some([("index", df.getattr("index")?)].into_py_dict(py)),
                    None,
                )?
                .extract()?;
            let mut cols_to_check = vec![src, dst];
            if layer_in_df.unwrap_or(false) {
                if let Some(ref layer) = layer {
                    cols_to_check.push(layer.as_ref());
                }
            }
            cols_to_check.extend(const_props.as_ref().unwrap_or(&Vec::new()));
            let df = process_pandas_py_df(df, py, size, cols_to_check.clone())?;
            df.check_cols_exist(&cols_to_check)?;
            load_edges_props_from_df(
                &df,
                size,
                src,
                dst,
                const_props,
                shared_const_props,
                layer,
                layer_in_df.unwrap_or(true),
                graph,
            )
            .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;
            df.check_cols_exist(&cols_to_check)?;
            Ok::<(), PyErr>(())
        })
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
        Ok(())
    }
}
