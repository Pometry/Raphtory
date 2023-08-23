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
    db::{
        api::view::internal::{DynamicGraph, IntoDynamic},
        graph::{edge::EdgeView, vertex::VertexView},
    },
    python::graph::pandas::{load_edges_props_from_df, load_vertex_props_from_df},
};
use pyo3::types::IntoPyDict;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
};

use super::pandas::{
    load_edges_from_df, load_vertices_from_df, process_pandas_py_df, GraphLoadException,
};

/// A temporal graph.
#[derive(Clone)]
#[pyclass(name="Graph", extends=PyGraphView)]
pub struct PyGraph {
    pub(crate) graph: Graph,
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
    ///    properties (dict): The properties of the vertex.
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

    /// add_vertex_properties(self, id: str | int, properties: dict) -> None
    /// Adds properties to an existing vertex.
    ///
    /// Arguments:
    ///     id (str or int): The id of the vertex.
    ///     properties (dict): The properties of the vertex.
    ///
    /// Returns:
    ///    None
    pub fn add_vertex_properties(
        &self,
        id: PyInputVertex,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        self.graph.add_vertex_properties(id, properties)
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
    pub fn add_static_property(&self, properties: HashMap<String, Prop>) -> Result<(), GraphError> {
        self.graph.add_static_properties(properties)
    }

    /// Adds a new edge with the given source and destination vertices and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int): The timestamp of the edge.
    ///    src (str or int): The id of the source vertex.
    ///    dst (str or int): The id of the destination vertex.
    ///    properties (dict): The properties of the edge, as a dict of string and properties
    ///    layer (str): The layer of the edge.
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

    /// Adds properties to an existing edge.
    ///
    /// Arguments:
    ///    src (str or int): The id of the source vertex.
    ///    dst (str or int): The id of the destination vertex.
    ///    properties (dict): The properties of the edge, as a dict of string and properties
    ///    layer (str): The layer of the edge.
    ///
    /// Returns:
    ///  None
    #[pyo3(signature = (src, dst, properties, layer=None))]
    pub fn add_edge_properties(
        &self,
        src: PyInputVertex,
        dst: PyInputVertex,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.graph.add_edge_properties(src, dst, properties, layer)
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

    #[staticmethod]
    #[pyo3(signature = (edges_df, src = "source", dst = "destination", time = "time", props = None, const_props=None,shared_const_props=None,layer = None, layer_in_df = None, vertex_df = None, vertex_col = None, vertex_time_col = None, vertex_props = None, vertex_const_props = None, vertex_shared_const_props = None))]
    fn load_from_pandas(
        edges_df: &PyAny,
        src: &str,
        dst: &str,
        time: &str,
        props: Option<Vec<&str>>,
        const_props: Option<Vec<&str>>,
        shared_const_props: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_in_df: Option<&str>,
        vertex_df: Option<&PyAny>,
        vertex_col: Option<&str>,
        vertex_time_col: Option<&str>,
        vertex_props: Option<Vec<&str>>,
        vertex_const_props: Option<Vec<&str>>,
        vertex_shared_const_props: Option<HashMap<String, Prop>>,
    ) -> Result<Graph, GraphError> {
        let graph = PyGraph {
            graph: Graph::new(),
        };
        graph.load_edges_from_pandas(
            edges_df,
            src,
            dst,
            time,
            props,
            const_props,
            shared_const_props,
            layer,
            layer_in_df,
        )?;
        if let (Some(vertex_df), Some(vertex_col), Some(vertex_time_col)) =
            (vertex_df, vertex_col, vertex_time_col)
        {
            graph.load_vertices_from_pandas(
                vertex_df,
                vertex_col,
                vertex_time_col,
                vertex_props,
                vertex_const_props,
                vertex_shared_const_props,
            )?;
        }
        Ok(graph.graph)
    }

    #[pyo3(signature = (vertices_df, vertex_col = "id", time_col = "time", props = None, const_props = None, shared_const_props = None))]
    fn load_vertices_from_pandas(
        &self,
        vertices_df: &PyAny,
        vertex_col: &str,
        time_col: &str,
        props: Option<Vec<&str>>,
        const_props: Option<Vec<&str>>,
        shared_const_props: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        let graph = &self.graph;
        Python::with_gil(|py| {
            let size: usize = py
                .eval(
                    "index.__len__()",
                    Some([("index", vertices_df.getattr("index")?)].into_py_dict(py)),
                    None,
                )?
                .extract()?;
            let df = process_pandas_py_df(vertices_df, py, size)?;
            load_vertices_from_df(
                &df,
                size,
                vertex_col,
                time_col,
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

    #[pyo3(signature = (edge_df, src_col = "source", dst_col = "destination", time_col = "time", props = None, const_props=None,shared_const_props=None,layer=None,layer_in_df=None))]
    fn load_edges_from_pandas(
        &self,
        edge_df: &PyAny,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
        props: Option<Vec<&str>>,
        const_props: Option<Vec<&str>>,
        shared_const_props: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_in_df: Option<&str>,
    ) -> Result<(), GraphError> {
        let graph = &self.graph;
        Python::with_gil(|py| {
            let size: usize = py
                .eval(
                    "index.__len__()",
                    Some([("index", edge_df.getattr("index")?)].into_py_dict(py)),
                    None,
                )?
                .extract()?;
            let df = process_pandas_py_df(edge_df, py, size)?;
            load_edges_from_df(
                &df,
                size,
                src_col,
                dst_col,
                time_col,
                props,
                const_props,
                shared_const_props,
                layer,
                layer_in_df,
                graph,
            )
            .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;

            Ok::<(), PyErr>(())
        })
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
        Ok(())
    }

    #[pyo3(signature = (vertices_df, vertex_col = "id", const_props = None, shared_const_props = None))]
    fn load_vertex_props_from_pandas(
        &self,
        vertices_df: &PyAny,
        vertex_col: &str,
        const_props: Option<Vec<&str>>,
        shared_const_props: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        let graph = &self.graph;
        Python::with_gil(|py| {
            let size: usize = py
                .eval(
                    "index.__len__()",
                    Some([("index", vertices_df.getattr("index")?)].into_py_dict(py)),
                    None,
                )?
                .extract()?;
            let df = process_pandas_py_df(vertices_df, py, size)?;
            load_vertex_props_from_df(
                &df,
                size,
                vertex_col,
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

    #[pyo3(signature = (edge_df, src_col = "source", dst_col = "destination", const_props=None,shared_const_props=None,layer=None,layer_in_df=None))]
    fn load_edge_props_from_pandas(
        &self,
        edge_df: &PyAny,
        src_col: &str,
        dst_col: &str,
        const_props: Option<Vec<&str>>,
        shared_const_props: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_in_df: Option<&str>,
    ) -> Result<(), GraphError> {
        let graph = &self.graph;
        Python::with_gil(|py| {
            let size: usize = py
                .eval(
                    "index.__len__()",
                    Some([("index", edge_df.getattr("index")?)].into_py_dict(py)),
                    None,
                )?
                .extract()?;
            let df = process_pandas_py_df(edge_df, py, size)?;
            load_edges_props_from_df(
                &df,
                size,
                src_col,
                dst_col,
                const_props,
                shared_const_props,
                layer,
                layer_in_df,
                graph,
            )
            .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;

            Ok::<(), PyErr>(())
        })
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
        Ok(())
    }
}
