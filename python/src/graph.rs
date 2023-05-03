//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add vertices and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//! In Python, this class wraps around the rust graph.

use crate::dynamic::IntoDynamic;
use crate::graph_view::PyGraphView;
use crate::utils::{adapt_result, extract_input_vertex, extract_into_time, InputVertexBox};
use crate::wrappers::prop::Prop;
use itertools::Itertools;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use raphtory::core as dbc;
use raphtory::db::graph::Graph;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A temporal graph.
#[pyclass(name="Graph", extends=PyGraphView)]
pub struct PyGraph {
    pub(crate) graph: Graph,
}

impl From<Graph> for PyGraph {
    fn from(value: Graph) -> Self {
        Self { graph: value }
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
    #[pyo3(signature = (nr_shards=1))]
    pub fn py_new(nr_shards: usize) -> (Self, PyGraphView) {
        let graph = Graph::new(nr_shards);
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
    ///    timestamp (int): The timestamp of the vertex.
    ///    id (str or int): The id of the vertex.
    ///    properties (dict): The properties of the vertex.
    ///
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, id, properties=None))]
    pub fn add_vertex(
        &self,
        timestamp: i64,
        id: &PyAny,
        properties: Option<HashMap<String, Prop>>,
    ) -> PyResult<()> {
        let v = Self::extract_id(id)?;
        let result = self
            .graph
            .add_vertex(timestamp, v, &Self::transform_props(properties));
        adapt_result(result)
    }

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
        id: &PyAny,
        properties: HashMap<String, Prop>,
    ) -> PyResult<()> {
        let v = Self::extract_id(id)?;
        let result = self
            .graph
            .add_vertex_properties(v, &Self::transform_props(Some(properties)));
        adapt_result(result)
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
        timestamp: &PyAny,
        src: &PyAny,
        dst: &PyAny,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> PyResult<()> {
        let time = extract_into_time(timestamp)?;
        let src = Self::extract_id(src)?;
        let dst = Self::extract_id(dst)?;
        adapt_result(
            self.graph
                .add_edge(time, src, dst, &Self::transform_props(properties), layer),
        )
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
        src: &PyAny,
        dst: &PyAny,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> PyResult<()> {
        let src = Self::extract_id(src)?;
        let dst = Self::extract_id(dst)?;
        let result = self.graph.add_edge_properties(
            src,
            dst,
            &Self::transform_props(Some(properties)),
            layer,
        );
        adapt_result(result)
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
    pub fn load_from_file(path: String) -> PyResult<Py<PyGraph>> {
        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), &path].iter().collect();

        match Graph::load_from_file(file_path) {
            Ok(g) => Self::py_from_db_graph(g),
            Err(e) => Err(PyException::new_err(format!(
                "Failed to load graph from the files. Reason: {}",
                e
            ))),
        }
    }

    /// Saves the graph to the given path.
    ///
    /// Arguments:
    ///  path (str): The path to the graph.
    ///
    /// Returns:
    /// None
    pub fn save_to_file(&self, path: String) -> PyResult<()> {
        match self.graph.save_to_file(Path::new(&path)) {
            Ok(()) => Ok(()),
            Err(e) => Err(PyException::new_err(format!(
                "Failed to save graph to the files. Reason: {}",
                e
            ))),
        }
    }
}

impl PyGraph {
    fn transform_props(props: Option<HashMap<String, Prop>>) -> Vec<(String, dbc::Prop)> {
        props
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect_vec()
    }

    /// Extracts the id from the given python vertex
    ///
    /// Arguments:
    ///     id (str or int): The id of the vertex.
    pub(crate) fn extract_id(id: &PyAny) -> PyResult<InputVertexBox> {
        extract_input_vertex(id)
    }
}
