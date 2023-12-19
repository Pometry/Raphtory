//! The API for querying a view of the graph in a read-only state

use crate::{
    core::{
        entities::nodes::node_ref::NodeRef,
        utils::{errors::GraphError, time::error::ParseTimeError},
        ArcStr,
    },
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{DynamicGraph, IntoDynamic, MaterializedGraph},
                LayerOps, StaticGraphViewOps, WindowSet,
            },
        },
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::{
                layer_graph::LayeredGraph, node_subgraph::NodeSubgraph, window_graph::WindowedGraph,
            },
        },
    },
    prelude::*,
    python::{
        graph::{edge::PyEdges, node::PyNodes},
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
impl<G: StaticGraphViewOps + IntoDynamic> From<G> for PyGraphView {
    fn from(value: G) -> Self {
        PyGraphView {
            graph: value.into_dynamic(),
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject> for WindowedGraph<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyGraphView::from(self).into_py(py)
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject> for LayeredGraph<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyGraphView::from(self).into_py(py)
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject> for NodeSubgraph<G> {
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

    /// Number of nodes in the graph
    ///
    /// Returns:
    ///   the number of nodes in the graph
    pub fn count_nodes(&self) -> usize {
        self.graph.count_nodes()
    }

    /// Returns true if the graph contains the specified node
    ///
    /// Arguments:
    ///    id (str or int): the node id
    ///
    /// Returns:
    ///   true if the graph contains the specified node, false otherwise
    pub fn has_node(&self, id: NodeRef) -> bool {
        self.graph.has_node(id)
    }

    /// Returns true if the graph contains the specified edge
    ///
    /// Arguments:
    ///   src (str or int): the source node id
    ///   dst (str or int): the destination node id
    ///   layer (str): the edge layer (optional)
    ///
    /// Returns:
    ///  true if the graph contains the specified edge, false otherwise
    #[pyo3(signature = (src, dst, layer=None))]
    pub fn has_edge(&self, src: NodeRef, dst: NodeRef, layer: Option<&str>) -> bool {
        self.graph.has_edge(src, dst, layer)
    }

    //******  Getter APIs ******//

    /// Gets the node with the specified id
    ///
    /// Arguments:
    ///   id (str or int): the node id
    ///
    /// Returns:
    ///   the node with the specified id, or None if the node does not exist
    pub fn node(&self, id: NodeRef) -> Option<NodeView<DynamicGraph>> {
        self.graph.node(id)
    }

    /// Gets the nodes in the graph
    ///
    /// Returns:
    ///  the nodes in the graph
    #[getter]
    pub fn nodes(&self) -> PyNodes {
        self.graph.nodes().into()
    }

    /// Gets the edge with the specified source and destination nodes
    ///
    /// Arguments:
    ///     src (str or int): the source node id
    ///     dst (str or int): the destination node id
    ///     layer (str): the edge layer (optional)
    ///
    /// Returns:
    ///     the edge with the specified source and destination nodes, or None if the edge does not exist
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: NodeRef, dst: NodeRef) -> Option<EdgeView<DynamicGraph, DynamicGraph>> {
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

    /// Returns a subgraph given a set of nodes
    ///
    /// Arguments:
    ///   * `nodes`: set of nodes
    ///
    /// Returns:
    ///    GraphView - Returns the subgraph
    fn subgraph(&self, nodes: Vec<NodeRef>) -> NodeSubgraph<DynamicGraph> {
        self.graph.subgraph(nodes)
    }

    /// Returns a 'materialized' clone of the graph view - i.e. a new graph with a copy of the data seen within the view instead of just a mask over the original graph
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

impl_timeops!(PyGraphView, graph, DynamicGraph, "graph");

impl Repr for PyGraphView {
    fn repr(&self) -> String {
        let num_edges = self.graph.count_edges();
        let num_nodes = self.graph.count_nodes();
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
            format!(
                "Graph(number_of_nodes={:?}, number_of_edges={:?}, number_of_temporal_edges={:?}, earliest_time={:?}, latest_time={:?})",
                num_edges, num_nodes, num_temporal_edges, earliest_time, latest_time
            )
        } else {
            let property_string: String = format!("{{{properties}}}");
            format!(
                "Graph(number_of_nodes={:?}, number_of_edges={:?}, number_of_temporal_edges={:?}, earliest_time={:?}, latest_time={:?}, properties={})",
                num_edges, num_nodes, num_temporal_edges, earliest_time, latest_time, property_string
            )
        }
    }
}
