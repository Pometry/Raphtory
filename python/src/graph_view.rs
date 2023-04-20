//! The API for querying a view of the graph in a read-only state
use crate::dynamic::DynamicGraph;
use crate::edge::{PyEdge, PyEdges};

use crate::util::{extract_vertex_ref, through_impl, window_impl};
use crate::vertex::{PyVertex, PyVertices};
use docbrown::db::graph_window::WindowSet;
use docbrown::db::view_api::*;
use pyo3::prelude::*;

/// Graph view is a read-only version of a graph at a certain point in time.
#[pyclass(name = "GraphView", frozen, subclass)]
pub struct PyGraphView {
    pub(crate) graph: DynamicGraph,
}

/// Graph view is a read-only version of a graph at a certain point in time.
impl<G: GraphViewOps> From<G> for PyGraphView {
    fn from(value: G) -> Self {
        PyGraphView {
            graph: DynamicGraph::new(value),
        }
    }
}

/// A set of windowed views of a `Graph`, allows user to iterating over a Graph broken
/// down into multiple windowed views.
#[pyclass(name = "GraphWindowSet")]
pub struct PyGraphWindowSet {
    window_set: WindowSet<DynamicGraph>,
}

impl From<WindowSet<DynamicGraph>> for PyGraphWindowSet {
    fn from(value: WindowSet<DynamicGraph>) -> Self {
        Self { window_set: value }
    }
}

/// A set of windowed views of a `Graph`, allows user to iterating over a Graph broken
/// down into multiple windowed views.
#[pymethods]
impl PyGraphWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    /// gets the next windowed view of the graph
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyGraphView> {
        slf.window_set.next().map(|g| g.into())
    }
}

/// The API for querying a view of the graph in a read-only state
#[pymethods]
impl PyGraphView {
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
    #[pyo3(signature = (step, start=None, end=None))]
    fn expanding(&self, step: u64, start: Option<i64>, end: Option<i64>) -> PyGraphWindowSet {
        self.graph.expanding(step, start, end).into()
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
    fn rolling(
        &self,
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyGraphWindowSet {
        self.graph.rolling(window, step, start, end).into()
    }

    /// Create a view including all events between `t_start` (inclusive) and `t_end` (exclusive)
    ///
    /// Arguments:
    ///   t_start (int): the start time of the window (optional)
    ///   t_end (int): the end time of the window (optional)
    ///
    /// Returns:
    ///     a view including all events between `t_start` (inclusive) and `t_end` (exclusive)
    #[pyo3(signature = (t_start=None, t_end=None))]
    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyGraphView {
        window_impl(&self.graph, t_start, t_end).into()
    }

    /// Create a view including all events until `end` (inclusive)
    ///
    /// Arguments:
    ///     end (int) : the end time of the window
    ///
    /// Returns:
    ///     a view including all events until `end` (inclusive)
    #[pyo3(signature = (end))]
    pub fn at(&self, end: i64) -> PyGraphView {
        self.graph.at(end).into()
    }

    /// Given a PerspectiveSet this returns an iterator of windowed graphs,
    /// creating one window graph for each perspecive in the collection
    ///
    /// Arguments:
    ///   perspectives: the perspectives to use
    ///
    /// Returns:
    ///  an iterator of windowed graphs
    fn through(&self, perspectives: &PyAny) -> PyResult<PyGraphWindowSet> {
        through_impl(&self.graph, perspectives).map(|p| p.into())
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
