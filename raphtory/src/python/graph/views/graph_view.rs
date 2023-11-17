//! The API for querying a view of the graph in a read-only state

use crate::{
    core::{
        entities::vertices::vertex_ref::VertexRef,
        utils::{errors::GraphError, time::error::ParseTimeError},
        ArcStr,
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
        graph::{edge::PyEdges, index::GraphIndex, vertex::PyVertices},
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
    #[getter]
    pub fn unique_layers(&self) -> Vec<ArcStr> {
        self.graph.unique_layers().collect()
    }

    //******  Metrics APIs ******//

    /// Timestamp of earliest activity in the graph
    ///
    /// Returns:
    ///     the timestamp of the earliest activity in the graph
    #[getter]
    pub fn earliest_time(&self) -> Option<i64> {
        self.graph.earliest_time()
    }

    /// DateTime of earliest activity in the graph
    ///
    /// Returns:
    ///     the datetime of the earliest activity in the graph
    #[getter]
    pub fn earliest_date_time(&self) -> Option<NaiveDateTime> {
        let earliest_time = self.graph.earliest_time()?;
        NaiveDateTime::from_timestamp_millis(earliest_time)
    }

    /// Timestamp of latest activity in the graph
    ///
    /// Returns:
    ///     the timestamp of the latest activity in the graph
    #[getter]
    pub fn latest_time(&self) -> Option<i64> {
        self.graph.latest_time()
    }

    /// DateTime of latest activity in the graph
    ///
    /// Returns:
    ///     the datetime of the latest activity in the graph
    #[getter]
    pub fn latest_date_time(&self) -> Option<NaiveDateTime> {
        let latest_time = self.graph.latest_time()?;
        NaiveDateTime::from_timestamp_millis(latest_time)
    }

    /// Number of edges in the graph
    ///
    /// Returns:
    ///    the number of edges in the graph
    pub fn count_edges(&self) -> usize {
        self.graph.count_edges()
    }

    /// Number of edges in the graph
    ///
    /// Returns:
    ///    the number of temporal edges in the graph
    pub fn count_temporal_edges(&self) -> usize {
        self.graph.count_temporal_edges()
    }

    /// Number of vertices in the graph
    ///
    /// Returns:
    ///   the number of vertices in the graph
    pub fn count_vertices(&self) -> usize {
        self.graph.count_vertices()
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
    #[getter]
    pub fn edges(&self) -> PyEdges {
        let clone = self.graph.clone();
        (move || clone.edges()).into()
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

    /// Returns a 'materialized' clone of the graph view - i.e. a new graph with a copy of the data seen within the view instead of just a mask over the original graph
    ///
    /// Returns:
    ///    GraphView - Returns a graph clone
    fn materialize(&self) -> Result<MaterializedGraph, GraphError> {
        self.graph.materialize()
    }

    /// Indexes all vertex and edge properties.
    /// Returns a GraphIndex which allows the user to search the edges and vertices of the graph via tantivity fuzzy matching queries.
    /// Note this is currently immutable and will not update if the graph changes. This is to be improved in a future release.
    ///
    /// Returns:
    ///    GraphIndex - Returns a GraphIndex
    fn index(&self) -> GraphIndex {
        GraphIndex::new(self.graph.clone())
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

impl_timeops!(PyGraphView, graph, DynamicGraph, "graph");

impl Repr for PyGraphView {
    fn repr(&self) -> String {
        let num_edges = self.graph.count_edges();
        let num_vertices = self.graph.count_vertices();
        let num_temporal_edges: usize = self.graph.count_temporal_edges();
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
