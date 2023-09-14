//! The API for querying a view of the graph in a read-only state

use crate::{
    core::{
        entities::vertices::vertex_ref::VertexRef,
        utils::{errors::GraphError, time::error::ParseTimeError},
    },
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{DynamicGraph, IntoDynamic, MaterializedGraph},
                LayerOps, WindowSet,
            },
        },
        graph::{
            edge::EdgeView,
            vertex::VertexView,
            views::{
                layer_graph::LayeredGraph, vertex_subgraph::VertexSubgraph,
                window_graph::WindowedGraph,
            },
        },
    },
    prelude::*,
    python::{
        graph::{edge::PyEdges, vertex::PyVertices},
        types::repr::Repr,
        utils::{PyInterval, PyTime},
    },
    *,
};
use chrono::prelude::*;
use itertools::Itertools;
use pyo3::{prelude::*, types::PyBytes};
use std::ops::Deref;

impl IntoPy<PyObject> for MaterializedGraph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            MaterializedGraph::EventGraph(g) => g.into_py(py),
            MaterializedGraph::PersistentGraph(g) => g.into_py(py),
        }
    }
}

impl IntoPy<PyObject> for DynamicGraph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyGraphView::from(self).into_py(py)
    }
}

impl<'source> FromPyObject<'source> for DynamicGraph {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        ob.extract::<PyRef<PyGraphView>>()
            .map(|g| g.graph.clone())
            .or_else(|err| {
                let res = ob.call_method0("bincode").map_err(|_| err)?; // return original error as probably more helpful
                                                                        // assume we have a graph at this point, the res probably should not fail
                let b = res.extract::<&[u8]>()?;
                let g = MaterializedGraph::from_bincode(b)?;
                Ok(g.into_dynamic())
            })
    }
}
/// Graph view is a read-only version of a graph at a certain point in time.

#[pyclass(name = "GraphView", frozen, subclass)]
#[repr(C)]
pub struct PyGraphView {
    pub graph: DynamicGraph,
}

/// Graph view is a read-only version of a graph at a certain point in time.
impl<G: GraphViewOps + IntoDynamic> From<G> for PyGraphView {
    fn from(value: G) -> Self {
        PyGraphView {
            graph: value.into_dynamic(),
        }
    }
}

impl<G: GraphViewOps + IntoDynamic> IntoPy<PyObject> for WindowedGraph<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyGraphView::from(self).into_py(py)
    }
}

impl<G: GraphViewOps + IntoDynamic> IntoPy<PyObject> for LayeredGraph<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyGraphView::from(self).into_py(py)
    }
}

impl<G: GraphViewOps + IntoDynamic> IntoPy<PyObject> for VertexSubgraph<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyGraphView::from(self).into_py(py)
    }
}

/// The API for querying a view of the graph in a read-only state
#[pymethods]
impl PyGraphView {
    /// Return all the layer ids in the graph
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

    /// DateTime of earliest activity in the graph
    ///
    /// Returns:
    ///     the datetime of the earliest activity in the graph
    pub fn earliest_date_time(&self) -> Option<NaiveDateTime> {
        let earliest_time = self.graph.earliest_time()?;
        NaiveDateTime::from_timestamp_millis(earliest_time)
    }

    /// Timestamp of latest activity in the graph
    ///
    /// Returns:
    ///     the timestamp of the latest activity in the graph
    pub fn latest_time(&self) -> Option<i64> {
        self.graph.latest_time()
    }

    /// DateTime of latest activity in the graph
    ///
    /// Returns:
    ///     the datetime of the latest activity in the graph
    pub fn latest_date_time(&self) -> Option<NaiveDateTime> {
        let latest_time = self.graph.latest_time()?;
        NaiveDateTime::from_timestamp_millis(latest_time)
    }

    /// Number of edges in the graph
    ///
    /// Returns:
    ///    the number of edges in the graph
    pub fn num_edges(&self) -> usize {
        self.graph.num_edges()
    }

    /// Number of edges in the graph
    ///
    /// Returns:
    ///    the number of temporal edges in the graph
    pub fn num_temporal_edges(&self) -> usize {
        self.graph.num_temporal_edges()
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
    pub fn has_vertex(&self, id: VertexRef) -> bool {
        self.graph.has_vertex(id)
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
    pub fn has_edge(&self, src: VertexRef, dst: VertexRef, layer: Option<&str>) -> bool {
        self.graph.has_edge(src, dst, layer)
    }

    //******  Getter APIs ******//

    /// Gets the vertex with the specified id
    ///
    /// Arguments:
    ///   id (str or int): the vertex id
    ///
    /// Returns:
    ///   the vertex with the specified id, or None if the vertex does not exist
    pub fn vertex(&self, id: VertexRef) -> Option<VertexView<DynamicGraph>> {
        self.graph.vertex(id)
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
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: VertexRef, dst: VertexRef) -> Option<EdgeView<DynamicGraph>> {
        self.graph.edge(src, dst)
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

    /// Returns the default start datetime for perspectives over the view
    ///
    /// Returns:
    ///     the default start datetime for perspectives over the view
    pub fn start_date_time(&self) -> Option<NaiveDateTime> {
        let start_time = self.graph.start()?;
        NaiveDateTime::from_timestamp_millis(start_time)
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

    /// Returns the default end datetime for perspectives over the view
    ///
    /// Returns:
    ///    the default end datetime for perspectives over the view
    pub fn end_date_time(&self) -> Option<NaiveDateTime> {
        let end_time = self.graph.end()?;
        NaiveDateTime::from_timestamp_millis(end_time)
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
    fn expanding(&self, step: PyInterval) -> Result<WindowSet<DynamicGraph>, ParseTimeError> {
        self.graph.expanding(step)
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
        window: PyInterval,
        step: Option<PyInterval>,
    ) -> Result<WindowSet<DynamicGraph>, ParseTimeError> {
        self.graph.rolling(window, step)
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
    pub fn window(
        &self,
        start: Option<PyTime>,
        end: Option<PyTime>,
    ) -> WindowedGraph<DynamicGraph> {
        self.graph
            .window(start.unwrap_or(PyTime::MIN), end.unwrap_or(PyTime::MAX))
    }

    /// Create a view including all events until `end` (inclusive)
    ///
    /// Arguments:
    ///     end (int) : the end time of the window
    ///
    /// Returns:
    ///     a view including all events until `end` (inclusive)
    #[pyo3(signature = (end))]
    pub fn at(&self, end: PyTime) -> WindowedGraph<DynamicGraph> {
        self.graph.at(end)
    }

    #[doc = default_layer_doc_string!()]
    pub fn default_layer(&self) -> LayeredGraph<DynamicGraph> {
        self.graph.default_layer()
    }

    #[doc = layers_doc_string!()]
    #[pyo3(signature = (names))]
    pub fn layers(&self, names: Vec<String>) -> Option<LayeredGraph<DynamicGraph>> {
        self.graph.layer(names)
    }

    #[doc = layers_doc_string!()]
    #[pyo3(signature = (name))]
    pub fn layer(&self, name: String) -> Option<LayeredGraph<DynamicGraph>> {
        self.graph.layer(name)
    }

    /// Get all graph properties
    ///
    ///
    /// Returns:
    ///    HashMap<String, Prop> - Properties paired with their names
    #[getter]
    fn properties(&self) -> Properties<DynamicGraph> {
        self.graph.properties()
    }

    /// Returns a subgraph given a set of vertices
    ///
    /// Arguments:
    ///   * `vertices`: set of vertices
    ///
    /// Returns:
    ///    GraphView - Returns the subgraph
    fn subgraph(&self, vertices: Vec<VertexRef>) -> VertexSubgraph<DynamicGraph> {
        self.graph.subgraph(vertices)
    }

    /// Returns a graph clone
    ///
    /// Arguments:
    ///
    /// Returns:
    ///    GraphView - Returns a graph clone
    fn materialize(&self) -> Result<MaterializedGraph, GraphError> {
        self.graph.materialize()
    }

    /// Get bincode encoded graph
    pub fn bincode<'py>(&'py self, py: Python<'py>) -> Result<&'py PyBytes, GraphError> {
        let bytes = self.graph.materialize()?.bincode()?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Displays the graph
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyGraphView {
    fn repr(&self) -> String {
        let num_edges = self.graph.num_edges();
        let num_vertices = self.graph.num_vertices();
        let num_temporal_edges: usize = self.graph.num_temporal_edges();
        let earliest_time = self.graph.earliest_time().repr();
        let latest_time = self.graph.latest_time().repr();
        let properties: String = self
            .graph
            .properties()
            .iter()
            .map(|(k, v)| format!("{}: {}", k.deref(), v))
            .join(", ");
        if properties.is_empty() {
            return format!(
                "Graph(number_of_edges={:?}, number_of_vertices={:?}, number_of_temporal_edges={:?}, earliest_time={:?}, latest_time={:?})",
                num_edges, num_vertices, num_temporal_edges, earliest_time, latest_time
            );
        } else {
            let property_string: String = format!("{{{properties}}}");
            return format!(
                "Graph(number_of_edges={:?}, number_of_vertices={:?}, number_of_temporal_edges={:?}, earliest_time={:?}, latest_time={:?}, properties={})",
                num_edges, num_vertices, num_temporal_edges, earliest_time, latest_time, property_string
            );
        }
    }
}
