//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add vertices and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//! In Python, this class wraps around the rust graph.
use crate::core::utils::errors::GraphError;
use crate::core::Prop;
use crate::db::api::mutation::{AdditionOps, PropertyAdditionOps};
use crate::prelude::*;
use crate::python;
use crate::python::utils::PyTime;
use pyo3::prelude::*;
use python::graph_view::PyGraphView;
use python::utils::PyInputVertex;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};

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

impl IntoPy<PyObject> for Graph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, (PyGraph::from(self.clone()), PyGraphView::from(self)))
            .unwrap() // I think this only fails if we are out of memory? Seems to be unavoidable if we want to create an actual graph.
            .into_py(py)
    }
}

impl<'source> FromPyObject<'source> for Graph {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        ob.extract()
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
    ) -> Result<(), GraphError> {
        self.graph
            .add_vertex(timestamp, id, properties.unwrap_or_default())
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
    ) -> Result<(), GraphError> {
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
}
