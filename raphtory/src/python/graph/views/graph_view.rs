//! The API for querying a view of the graph in a read-only state

use rayon::prelude::*;
use std::collections::HashMap;

use crate::{
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError},
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{DynamicGraph, IntoDynamic, MaterializedGraph, OneHopFilter},
                LayerOps, StaticGraphViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            edges::Edges,
            graph::graph_equal,
            node::NodeView,
            nodes::Nodes,
            views::{
                layer_graph::LayeredGraph,
                node_subgraph::NodeSubgraph,
                node_type_filtered_subgraph::TypeFilteredSubgraph,
                property_filter::{
                    edge_property_filter::EdgePropertyFilteredGraph,
                    exploded_edge_property_filter::ExplodedEdgePropertyFilteredGraph, internal::*,
                },
                window_graph::WindowedGraph,
            },
        },
    },
    prelude::*,
    python::{
        graph::{edge::PyEdge, node::PyNode},
        types::{
            repr::{Repr, StructReprBuilder},
            wrappers::prop::PyPropertyFilter,
        },
        utils::PyTime,
    },
};
use chrono::prelude::*;
use pyo3::prelude::*;
use raphtory_api::core::storage::arc_str::ArcStr;

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
        ob.extract::<PyRef<PyGraphView>>().map(|g| g.graph.clone())
    }
}
/// Graph view is a read-only version of a graph at a certain point in time.

#[pyclass(name = "GraphView", frozen, subclass)]
#[derive(Clone)]
#[repr(C)]
pub struct PyGraphView {
    pub graph: DynamicGraph,
}

impl_timeops!(PyGraphView, graph, DynamicGraph, "GraphView");
impl_layerops!(PyGraphView, graph, DynamicGraph, "GraphView");
impl_edge_property_filter_ops!(PyGraphView<DynamicGraph>, graph, "GraphView");

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

impl<G: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject> for TypeFilteredSubgraph<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyGraphView::from(self).into_py(py)
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject> for EdgePropertyFilteredGraph<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyGraphView::from(self).into_py(py)
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject>
    for ExplodedEdgePropertyFilteredGraph<G>
{
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
    pub fn earliest_date_time(&self) -> Option<DateTime<Utc>> {
        self.graph.earliest_date_time()
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
    pub fn latest_date_time(&self) -> Option<DateTime<Utc>> {
        self.graph.latest_date_time()
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
    ///
    /// Returns:
    ///  true if the graph contains the specified edge, false otherwise
    #[pyo3(signature = (src, dst))]
    pub fn has_edge(&self, src: NodeRef, dst: NodeRef) -> bool {
        self.graph.has_edge(src, dst)
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

    /// Get the nodes that match the properties name and value
    /// Arguments:
    ///     property_dict (dict): the properties name and value
    /// Returns:
    ///    the nodes that match the properties name and value
    #[pyo3(signature = (properties_dict))]
    pub fn find_nodes(&self, properties_dict: HashMap<String, Prop>) -> Vec<PyNode> {
        let iter = self.nodes().into_iter().par_bridge();
        let out = iter
            .filter(|n| {
                let props = n.properties();
                properties_dict.iter().all(|(k, v)| {
                    if let Some(prop) = props.get(k) {
                        &prop == v
                    } else {
                        false
                    }
                })
            })
            .map(|n| PyNode::from(n))
            .collect::<Vec<_>>();

        out
    }

    /// Gets the nodes in the graph
    ///
    /// Returns:
    ///  the nodes in the graph
    #[getter]
    pub fn nodes(&self) -> Nodes<'static, DynamicGraph> {
        self.graph.nodes()
    }

    /// Gets the edge with the specified source and destination nodes
    ///
    /// Arguments:
    ///     src (str or int): the source node id
    ///     dst (str or int): the destination node id
    ///
    /// Returns:
    ///     the edge with the specified source and destination nodes, or None if the edge does not exist
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: NodeRef, dst: NodeRef) -> Option<EdgeView<DynamicGraph, DynamicGraph>> {
        self.graph.edge(src, dst)
    }

    /// Get the edges that match the properties name and value
    /// Arguments:
    ///     property_dict (dict): the properties name and value
    /// Returns:
    ///    the edges that match the properties name and value
    #[pyo3(signature = (properties_dict))]
    pub fn find_edges(&self, properties_dict: HashMap<String, Prop>) -> Vec<PyEdge> {
        let iter = self.edges().into_iter().par_bridge();
        let out = iter
            .filter(|e| {
                let props = e.properties();
                properties_dict.iter().all(|(k, v)| {
                    if let Some(prop) = props.get(k) {
                        &prop == v
                    } else {
                        false
                    }
                })
            })
            .map(|e| PyEdge::from(e))
            .collect::<Vec<_>>();

        out
    }

    /// Gets all edges in the graph
    ///
    /// Returns:
    ///  the edges in the graph
    #[getter]
    pub fn edges(&self) -> Edges<'static, DynamicGraph> {
        self.graph.edges()
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

    /// Returns a subgraph filtered by node types given a set of node types
    ///
    /// Arguments:
    ///   * `node_types`: set of node types
    ///
    /// Returns:
    ///    GraphView - Returns the subgraph
    fn subgraph_node_types(&self, node_types: Vec<ArcStr>) -> TypeFilteredSubgraph<DynamicGraph> {
        self.graph.subgraph_node_types(node_types)
    }

    /// Returns a subgraph given a set of nodes that are excluded from the subgraph
    ///
    /// Arguments:
    ///   * `nodes`: set of nodes
    ///
    /// Returns:
    ///    GraphView - Returns the subgraph
    fn exclude_nodes(&self, nodes: Vec<NodeRef>) -> NodeSubgraph<DynamicGraph> {
        self.graph.exclude_nodes(nodes)
    }

    /// Returns a 'materialized' clone of the graph view - i.e. a new graph with a copy of the data seen within the view instead of just a mask over the original graph
    ///
    /// Returns:
    ///    GraphView - Returns a graph clone
    fn materialize(&self) -> Result<MaterializedGraph, GraphError> {
        self.graph.materialize()
    }

    /// Displays the graph
    pub fn __repr__(&self) -> String {
        self.repr()
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        graph_equal(&self.graph.clone(), &other.graph.clone())
    }
}

impl Repr for PyGraphView {
    fn repr(&self) -> String {
        if self.properties().is_empty() {
            StructReprBuilder::new("Graph")
                .add_field("number_of_nodes", self.graph.count_nodes())
                .add_field("number_of_edges", self.graph.count_edges())
                .add_field(
                    "number_of_temporal_edges",
                    self.graph.count_temporal_edges(),
                )
                .add_field("earliest_time", self.earliest_time())
                .add_field("latest_time", self.latest_time())
                .finish()
        } else {
            StructReprBuilder::new("Graph")
                .add_field("number_of_nodes", self.graph.count_nodes())
                .add_field("number_of_edges", self.graph.count_edges())
                .add_field(
                    "number_of_temporal_edges",
                    self.graph.count_temporal_edges(),
                )
                .add_field("earliest_time", self.earliest_time())
                .add_field("latest_time", self.latest_time())
                .add_field("properties", self.properties())
                .finish()
        }
    }
}
