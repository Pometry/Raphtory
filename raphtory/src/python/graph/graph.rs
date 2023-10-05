//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add vertices and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//! In Python, this class wraps around the rust graph.
use crate::{
    core::utils::errors::GraphError,
    db::api::view::internal::MaterializedGraph,
    prelude::*,
    python::{
        graph::{graph_with_deletions::PyGraphWithDeletions, views::graph_view::PyGraphView},
        utils::{PyInputVertex, PyTime},
    },
};
use pyo3::prelude::*;

use crate::{
    core::entities::vertices::vertex_ref::VertexRef,
    db::{
        api::view::internal::{DynamicGraph, IntoDynamic},
        graph::{edge::EdgeView, vertex::VertexView},
    },
    python::graph::pandas::{
        dataframe::{process_pandas_py_df, GraphLoadException},
        loaders::{load_edges_props_from_df, load_vertex_props_from_df},
    },
};
use pyo3::types::{IntoPyDict, PyBytes};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
};

use super::pandas::loaders::{load_edges_from_df, load_vertices_from_df};

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

    /// Adds a new vertex with the given id and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the vertex.
    ///    id (str or int): The id of the vertex.
    ///    properties (dict): The properties of the vertex (optional).
    ///
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, id, properties=None))]
    pub fn add_vertex(
        &self,
        timestamp: PyTime,
        id: PyInputVertex,
        properties: Option<HashMap<String, Prop>>,
    ) -> Result<VertexView<Graph>, GraphError> {
        self.graph
            .add_vertex(timestamp, id, properties.unwrap_or_default())
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

    /// Adds a new edge with the given source and destination vertices and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the edge.
    ///    src (str or int): The id of the source vertex.
    ///    dst (str or int): The id of the destination vertex.
    ///    properties (dict): The properties of the edge, as a dict of string and properties (optional).
    ///    layer (str): The layer of the edge (optional).
    ///
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, src, dst, properties=None, layer=None))]
    pub fn add_edge(
        &self,
        timestamp: PyTime,
        src: PyInputVertex,
        dst: PyInputVertex,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<EdgeView<Graph>, GraphError> {
        self.graph
            .add_edge(timestamp, src, dst, properties.unwrap_or_default(), layer)
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the vertex with the specified id
    ///
    /// Arguments:
    ///   id (str or int): the vertex id
    ///
    /// Returns:
    ///   the vertex with the specified id, or None if the vertex does not exist
    pub fn vertex(&self, id: VertexRef) -> Option<VertexView<Graph>> {
        self.graph.vertex(id)
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the edge with the specified source and destination vertices
    ///
    /// Arguments:
    ///     src (str or int): the source vertex id
    ///     dst (str or int): the destination vertex id
    ///
    /// Returns:
    ///     the edge with the specified source and destination vertices, or None if the edge does not exist
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: VertexRef, dst: VertexRef) -> Option<EdgeView<Graph>> {
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
    ///     edge_src (str): The column name for the source vertex ids.
    ///     edge_dst (str): The column name for the destination vertex ids.
    ///     edge_time (str): The column name for the timestamps.
    ///     edge_props (list): The column names for the temporal properties (optional) Defaults to None.
    ///     edge_const_props (list): The column names for the constant properties (optional) Defaults to None.
    ///     edge_shared_const_props (dict): A dictionary of constant properties that will be added to every edge (optional) Defaults to None.
    ///     edge_layer (str): The edge layer name (optional) Defaults to None.
    ///     layer_in_df (bool): Whether the layer name should be used to look up the values in a column of the edge_df or if it should be used directly as the layer for all edges (optional) defaults to True.
    ///     vertex_df (pandas.DataFrame): The DataFrame containing the vertices (optional) Defaults to None.
    ///     vertex_id (str): The column name for the vertex ids (optional) Defaults to None.
    ///     vertex_time (str): The column name for the vertex timestamps (optional) Defaults to None.
    ///     vertex_props (list): The column names for the vertex temporal properties (optional) Defaults to None.
    ///     vertex_const_props (list): The column names for the vertex constant properties (optional) Defaults to None.
    ///     vertex_shared_const_props (dict): A dictionary of constant properties that will be added to every vertex (optional) Defaults to None.
    ///
    /// Returns:
    ///      Graph: The loaded Graph object.
    #[staticmethod]
    #[pyo3(signature = (edge_df, edge_src, edge_dst, edge_time, edge_props = None, edge_const_props=None, edge_shared_const_props=None,
    edge_layer = None, layer_in_df = true, vertex_df = None, vertex_id = None, vertex_time = None, vertex_props = None, vertex_const_props = None, vertex_shared_const_props = None))]
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
        vertex_df: Option<&PyAny>,
        vertex_id: Option<&str>,
        vertex_time: Option<&str>,
        vertex_props: Option<Vec<&str>>,
        vertex_const_props: Option<Vec<&str>>,
        vertex_shared_const_props: Option<HashMap<String, Prop>>,
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
        if let (Some(vertex_df), Some(vertex_id), Some(vertex_time)) =
            (vertex_df, vertex_id, vertex_time)
        {
            graph.load_vertices_from_pandas(
                vertex_df,
                vertex_id,
                vertex_time,
                vertex_props,
                vertex_const_props,
                vertex_shared_const_props,
            )?;
        }
        Ok(graph.graph)
    }

    /// Load vertices from a Pandas DataFrame into the graph.
    ///
    /// Arguments:
    ///     df (pandas.DataFrame): The Pandas DataFrame containing the vertices.
    ///     id (str): The column name for the vertex IDs.
    ///     time (str): The column name for the timestamps.
    ///     props (List<str>): List of vertex property column names. Defaults to None. (optional)
    ///     const_props (List<str>): List of constant vertex property column names. Defaults to None.  (optional)
    ///     shared_const_props (Dictionary/Hashmap of properties): A dictionary of constant properties that will be added to every vertex. Defaults to None. (optional)
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(signature = (df, id, time, props = None, const_props = None, shared_const_props = None))]
    fn load_vertices_from_pandas(
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
            let df = process_pandas_py_df(df, py, size)?;
            load_vertices_from_df(
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
    ///     src (str): The column name for the source vertex ids.
    ///     dst (str): The column name for the destination vertex ids.
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
            let df = process_pandas_py_df(df, py, size)?;
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

    /// Load vertex properties from a Pandas DataFrame.
    ///
    /// Arguments:
    ///     df (Dataframe): The Pandas DataFrame containing vertex information.
    ///     id(str): The column name for the vertex IDs.
    ///     const_props (List<str>): List of constant vertex property column names. Defaults to None. (optional)
    ///     shared_const_props (<HashMap<String, Prop>>):  A dictionary of constant properties that will be added to every vertex. Defaults to None. (optional)
    ///
    /// Returns:
    ///     Result<(), GraphError>: Result of the operation.
    #[pyo3(signature = (df, id , const_props = None, shared_const_props = None))]
    fn load_vertex_props_from_pandas(
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
            let df = process_pandas_py_df(df, py, size)?;
            load_vertex_props_from_df(&df, size, id, const_props, shared_const_props, graph)
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
    ///     src (str): The column name for the source vertex.
    ///     dst (str): The column name for the destination vertex.
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
            let df = process_pandas_py_df(df, py, size)?;
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

            Ok::<(), PyErr>(())
        })
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
        Ok(())
    }
}
