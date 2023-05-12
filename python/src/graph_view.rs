//! The API for querying a view of the graph in a read-only state
use crate::dynamic::{DynamicGraph, IntoDynamic};
use crate::edge::{PyEdge, PyEdges};
use crate::utils::{
    at_impl, expanding_impl, extract_vertex_ref, rolling_impl, window_impl, IntoPyObject,
    PyWindowSet,
};
use crate::vertex::{PyVertex, PyVertices};
use pyo3::prelude::*;
use raphtory::db::view_api::layer::LayerOps;
use raphtory::db::view_api::*;
use raphtory::*;

/// Graph view is a read-only version of a graph at a certain point in time.
#[pyclass(name = "GraphView", frozen, subclass)]
pub struct PyGraphView {
    pub(crate) graph: DynamicGraph,
}

/// Graph view is a read-only version of a graph at a certain point in time.
impl<G: GraphViewOps + IntoDynamic> From<G> for PyGraphView {
    fn from(value: G) -> Self {
        PyGraphView {
            graph: value.into_dynamic(),
        }
    }
}

impl<G: GraphViewOps + IntoDynamic> IntoPyObject for G {
    fn into_py_object(self) -> PyObject {
        let py_version: PyGraphView = self.into();
        Python::with_gil(|py| py_version.into_py(py))
    }
}

/// The API for querying a view of the graph in a read-only state
#[pymethods]
impl PyGraphView {
    pub fn get_unique_layers(&self) -> Vec<String> {
        self.graph.get_unique_layers()
    }

    //******  Metrics APIs ******//

    /// Timestamp of earliest activity in the graph
    ///
    /// Returns:
    ///     the timestamp of the earliest activity in the graph
    pub fn earliest_time(&self) -> Option<i64> {
        self.graph.earliest_time()
    }

    /// Timestamp of latest activity in the graph
    ///
    /// Returns:
    ///     the timestamp of the latest activity in the graph
    pub fn latest_time(&self) -> Option<i64> {
        self.graph.latest_time()
    }

    /// Number of edges in the graph
    ///
    /// Returns:
    ///    the number of edges in the graph
    pub fn num_edges(&self) -> usize {
        self.graph.num_edges()
    }

    /// Number of vertices in the graph
    ///
    /// Returns:
    ///   the number of vertices in the graph
    pub fn num_vertices(&self) -> usize {
        self.graph.num_vertices()
    }

    /// Returns true if the graph contains the specified vertex
    ///
    /// Arguments:
    ///    id (str or int): the vertex id
    ///
    /// Returns:
    ///   true if the graph contains the specified vertex, false otherwise
    pub fn has_vertex(&self, id: &PyAny) -> PyResult<bool> {
        let v = extract_vertex_ref(id)?;
        Ok(self.graph.has_vertex(v))
    }

    /// Returns true if the graph contains the specified edge
    ///
    /// Arguments:
    ///   src (str or int): the source vertex id
    ///   dst (str or int): the destination vertex id  
    ///   layer (str): the edge layer (optional)
    ///
    /// Returns:
    ///  true if the graph contains the specified edge, false otherwise
    #[pyo3(signature = (src, dst, layer=None))]
    pub fn has_edge(&self, src: &PyAny, dst: &PyAny, layer: Option<&str>) -> PyResult<bool> {
        let src = extract_vertex_ref(src)?;
        let dst = extract_vertex_ref(dst)?;
        Ok(self.graph.has_edge(src, dst, layer))
    }

    //******  Getter APIs ******//

    /// Gets the vertex with the specified id
    ///
    /// Arguments:
    ///   id (str or int): the vertex id
    ///
    /// Returns:
    ///   the vertex with the specified id, or None if the vertex does not exist
    pub fn vertex(&self, id: &PyAny) -> PyResult<Option<PyVertex>> {
        let v = extract_vertex_ref(id)?;
        Ok(self.graph.vertex(v).map(|v| v.into()))
    }

    /// Gets the vertices in the graph
    ///
    /// Returns:
    ///  the vertices in the graph
    #[getter]
    pub fn vertices(&self) -> PyVertices {
        self.graph.vertices().into()
    }

    /// Gets the edge with the specified source and destination vertices
    ///
    /// Arguments:
    ///     src (str or int): the source vertex id
    ///     dst (str or int): the destination vertex id
    ///     layer (str): the edge layer (optional)
    ///
    /// Returns:
    ///     the edge with the specified source and destination vertices, or None if the edge does not exist
    #[pyo3(signature = (src, dst, layer=None))]
    pub fn edge(&self, src: &PyAny, dst: &PyAny, layer: Option<&str>) -> PyResult<Option<PyEdge>> {
        let src = extract_vertex_ref(src)?;
        let dst = extract_vertex_ref(dst)?;
        Ok(self.graph.edge(src, dst, layer).map(|we| we.into()))
    }

    /// Gets all edges in the graph
    ///
    /// Returns:
    ///  the edges in the graph
    pub fn edges(&self) -> PyEdges {
        let clone = self.graph.clone();
        (move || clone.edges()).into()
    }

    //******  Perspective APIS  ******//

    /// Returns the default start time for perspectives over the view
    ///
    /// Returns:
    ///     the default start time for perspectives over the view
    pub fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    /// Returns the default end time for perspectives over the view
    ///
    /// Returns:
    ///    the default end time for perspectives over the view
    pub fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    #[doc = window_size_doc_string!()]
    pub fn window_size(&self) -> Option<u64> {
        self.graph.window_size()
    }

    /// Creates a `WindowSet` with the given `step` size and optional `start` and `end` times,    
    /// using an expanding window.
    ///
    /// An expanding window is a window that grows by `step` size at each iteration.
    ///
    /// Arguments:
    ///     step (int) : the size of the window
    ///     start (int): the start time of the window (optional)
    ///     end (int): the end time of the window (optional)
    ///
    /// Returns:
    ///     A `WindowSet` with the given `step` size and optional `start` and `end` times,
    #[pyo3(signature = (step))]
    fn expanding(&self, step: &PyAny) -> PyResult<PyWindowSet> {
        expanding_impl(&self.graph, step)
    }

    /// Creates a `WindowSet` with the given `window` size and optional `step`, `start` and `end` times,
    /// using a rolling window.
    ///
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    ///
    /// Arguments:
    ///     window (int): the size of the window
    ///     step (int): the size of the step (optional)
    ///     start (int): the start time of the window (optional)
    ///     end: the end time of the window (optional)
    ///
    /// Returns:
    ///  a `WindowSet` with the given `window` size and optional `step`, `start` and `end` times,
    fn rolling(&self, window: &PyAny, step: Option<&PyAny>) -> PyResult<PyWindowSet> {
        rolling_impl(&self.graph, window, step)
    }

    /// Create a view including all events between `t_start` (inclusive) and `t_end` (exclusive)
    ///
    /// Arguments:
    ///   start (int): the start time of the window (optional)
    ///   end (int): the end time of the window (optional)
    ///
    /// Returns:
    ///     a view including all events between `t_start` (inclusive) and `t_end` (exclusive)
    #[pyo3(signature = (start=None, end=None))]
    pub fn window(&self, start: Option<&PyAny>, end: Option<&PyAny>) -> PyResult<PyGraphView> {
        window_impl(&self.graph, start, end).map(|g| g.into())
    }

    /// Create a view including all events until `end` (inclusive)
    ///
    /// Arguments:
    ///     end (int) : the end time of the window
    ///
    /// Returns:
    ///     a view including all events until `end` (inclusive)
    #[pyo3(signature = (end))]
    pub fn at(&self, end: &PyAny) -> PyResult<PyGraphView> {
        at_impl(&self.graph, end).map(|g| g.into())
    }

    #[doc = default_layer_doc_string!()]
    pub fn default_layer(&self) -> PyGraphView {
        self.graph.default_layer().into()
    }

    #[doc = layer_doc_string!()]
    #[pyo3(signature = (name))]
    pub fn layer(&self, name: &str) -> Option<PyGraphView> {
        self.graph.layer(name).map(|layer| layer.into())
    }

    /// Displays the graph
    pub fn __repr__(&self) -> String {
        let num_edges = self.graph.num_edges();
        let num_vertices = self.graph.num_vertices();
        let earliest_time = self.graph.earliest_time().unwrap_or_default();
        let latest_time = self.graph.latest_time().unwrap_or_default();

        format!(
            "Graph(number_of_edges={:?}, number_of_vertices={:?}, earliest_time={:?}, latest_time={:?})",
            num_edges, num_vertices, earliest_time, latest_time
        )
    }
}
