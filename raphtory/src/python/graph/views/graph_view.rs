//! The API for querying a view of the graph in a read-only state
use crate::{
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{
                    DynamicGraph, IntoDynHop, IntoDynamic, MaterializedGraph, OneHopFilter,
                },
                ExplodedEdgePropertyFilterOps, LayerOps, StaticGraphViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            edges::Edges,
            graph::graph_equal,
            node::NodeView,
            nodes::Nodes,
            views::{
                cached_view::CachedView,
                filter::{
                    edge_property_filtered_graph::EdgePropertyFilteredGraph,
                    exploded_edge_property_filter::ExplodedEdgePropertyFilteredGraph,
                    node_property_filtered_graph::NodePropertyFilteredGraph,
                    node_type_filtered_graph::NodeTypeFilteredGraph,
                },
                layer_graph::LayeredGraph,
                node_subgraph::NodeSubgraph,
                valid_graph::ValidGraph,
                window_graph::WindowedGraph,
            },
        },
    },
    errors::GraphError,
    prelude::*,
    python::{
        graph::{edge::PyEdge, node::PyNode},
        types::{
            repr::{Repr, StructReprBuilder},
            wrappers::filter_expr::PyFilterExpr,
        },
        utils::PyNodeRef,
    },
};
use chrono::prelude::*;
use pyo3::prelude::*;
use raphtory_api::core::storage::arc_str::ArcStr;
use rayon::prelude::*;
use std::collections::HashMap;

impl<'py> IntoPyObject<'py> for MaterializedGraph {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            MaterializedGraph::EventGraph(g) => g.into_pyobject(py).map(|b| b.into_any()),
            MaterializedGraph::PersistentGraph(g) => g.into_pyobject(py).map(|b| b.into_any()),
        }
    }
}

impl<'py> IntoPyObject<'py> for DynamicGraph {
    type Target = PyGraphView;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphView::from(self).into_pyobject(py)
    }
}

impl<'source> FromPyObject<'source> for DynamicGraph {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        ob.extract::<PyRef<PyGraphView>>().map(|g| g.graph.clone())
    }
}
/// Graph view is a read-only version of a graph at a certain point in time.

#[pyclass(name = "GraphView", frozen, subclass, module = "raphtory")]
#[derive(Clone)]
#[repr(C)]
pub struct PyGraphView {
    pub graph: DynamicGraph,
}

impl_timeops!(PyGraphView, graph, DynamicGraph, "GraphView");
impl_node_property_filter_ops!(PyGraphView<DynamicGraph>, graph, "GraphView");
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

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for WindowedGraph<G> {
    type Target = PyGraphView;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphView::from(self).into_pyobject(py)
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for LayeredGraph<G> {
    type Target = PyGraphView;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphView::from(self).into_pyobject(py)
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for NodeSubgraph<G> {
    type Target = PyGraphView;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphView::from(self).into_pyobject(py)
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for CachedView<G> {
    type Target = PyGraphView;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphView::from(self).into_pyobject(py)
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for NodeTypeFilteredGraph<G> {
    type Target = PyGraphView;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphView::from(self).into_pyobject(py)
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for EdgePropertyFilteredGraph<G> {
    type Target = PyGraphView;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphView::from(self).into_pyobject(py)
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for NodePropertyFilteredGraph<G> {
    type Target = PyGraphView;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphView::from(self).into_pyobject(py)
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py>
    for ExplodedEdgePropertyFilteredGraph<G>
{
    type Target = PyGraphView;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphView::from(self).into_pyobject(py)
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for ValidGraph<G> {
    type Target = PyGraphView;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphView::from(self).into_pyobject(py)
    }
}

/// The API for querying a view of the graph in a read-only state
#[pymethods]
impl PyGraphView {
    /// Return all the layer ids in the graph
    ///
    /// Returns:
    ///     list[str]: the names of all layers in the graph
    #[getter]
    pub fn unique_layers(&self) -> Vec<ArcStr> {
        self.graph.unique_layers().collect()
    }

    //******  Metrics APIs ******//

    /// Timestamp of earliest activity in the graph
    ///
    /// Returns:
    ///     Optional[int]: the timestamp of the earliest activity in the graph
    #[getter]
    pub fn earliest_time(&self) -> Option<i64> {
        self.graph.earliest_time()
    }

    /// DateTime of earliest activity in the graph
    ///
    /// Returns:
    ///     Optional[datetime]: the datetime of the earliest activity in the graph
    #[getter]
    pub fn earliest_date_time(&self) -> Option<DateTime<Utc>> {
        self.graph.earliest_date_time()
    }

    /// Timestamp of latest activity in the graph
    ///
    /// Returns:
    ///     Optional[int]: the timestamp of the latest activity in the graph
    #[getter]
    pub fn latest_time(&self) -> Option<i64> {
        self.graph.latest_time()
    }

    /// DateTime of latest activity in the graph
    ///
    /// Returns:
    ///     Optional[datetime]: the datetime of the latest activity in the graph
    #[getter]
    pub fn latest_date_time(&self) -> Option<DateTime<Utc>> {
        self.graph.latest_date_time()
    }

    /// Number of edges in the graph
    ///
    /// Returns:
    ///    int: the number of edges in the graph
    pub fn count_edges(&self) -> usize {
        self.graph.count_edges()
    }

    /// Number of edges in the graph
    ///
    /// Returns:
    ///    int: the number of temporal edges in the graph
    pub fn count_temporal_edges(&self) -> usize {
        self.graph.count_temporal_edges()
    }

    /// Number of nodes in the graph
    ///
    /// Returns:
    ///   int: the number of nodes in the graph
    pub fn count_nodes(&self) -> usize {
        self.graph.count_nodes()
    }

    /// Returns true if the graph contains the specified node
    ///
    /// Arguments:
    ///    id (NodeInput): the node id
    ///
    /// Returns:
    ///   bool: true if the graph contains the specified node, false otherwise
    pub fn has_node(&self, id: PyNodeRef) -> bool {
        self.graph.has_node(id)
    }

    /// Returns true if the graph contains the specified edge
    ///
    /// Arguments:
    ///   src (NodeInput): the source node id
    ///   dst (NodeInput): the destination node id
    ///
    /// Returns:
    ///     bool: true if the graph contains the specified edge, false otherwise
    #[pyo3(signature = (src, dst))]
    pub fn has_edge(&self, src: PyNodeRef, dst: PyNodeRef) -> bool {
        self.graph.has_edge(src, dst)
    }

    //******  Getter APIs ******//

    /// Gets the node with the specified id
    ///
    /// Arguments:
    ///   id (NodeInput): the node id
    ///
    /// Returns:
    ///     Optional[Node]: the node with the specified id, or None if the node does not exist
    pub fn node(&self, id: PyNodeRef) -> Option<NodeView<'static, DynamicGraph>> {
        self.graph.node(id)
    }

    /// Get the nodes that match the properties name and value
    /// Arguments:
    ///     properties_dict (dict[str, Prop]): the properties name and value
    /// Returns:
    ///    list[Node]: the nodes that match the properties name and value
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
            .map(PyNode::from)
            .collect::<Vec<_>>();

        out
    }

    /// Gets the nodes in the graph
    ///
    /// Returns:
    ///   Nodes: the nodes in the graph
    #[getter]
    pub fn nodes(&self) -> Nodes<'static, DynamicGraph> {
        self.graph.nodes()
    }

    /// Gets the edge with the specified source and destination nodes
    ///
    /// Arguments:
    ///     src (NodeInput): the source node id
    ///     dst (NodeInput): the destination node id
    ///
    /// Returns:
    ///     Optional[Edge]: the edge with the specified source and destination nodes, or None if the edge does not exist
    #[pyo3(signature = (src, dst))]
    pub fn edge(
        &self,
        src: PyNodeRef,
        dst: PyNodeRef,
    ) -> Option<EdgeView<DynamicGraph, DynamicGraph>> {
        self.graph.edge(src, dst)
    }

    /// Get the edges that match the properties name and value
    /// Arguments:
    ///     properties_dict (dict[str, Prop]): the properties name and value
    /// Returns:
    ///    list[Edge]: the edges that match the properties name and value
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
            .map(PyEdge::from)
            .collect::<Vec<_>>();

        out
    }

    /// Gets all edges in the graph
    ///
    /// Returns:
    ///   Edges: the edges in the graph
    #[getter]
    pub fn edges(&self) -> Edges<'static, DynamicGraph> {
        self.graph.edges()
    }

    /// Get all graph properties
    ///
    ///
    /// Returns:
    ///     Properties: Properties paired with their names
    #[getter]
    fn properties(&self) -> Properties<DynamicGraph> {
        self.graph.properties()
    }

    /// Returns a subgraph given a set of nodes
    ///
    /// Arguments:
    ///   nodes (list[NodeInput]): set of nodes
    ///
    /// Returns:
    ///    GraphView: Returns the subgraph
    fn subgraph(&self, nodes: Vec<PyNodeRef>) -> NodeSubgraph<DynamicGraph> {
        self.graph.subgraph(nodes)
    }

    /// Return a view of the graph that only includes valid edges
    ///
    /// Note:
    ///
    ///     The semantics for `valid` depend on the time semantics of the underlying graph.
    ///     In the case of a persistent graph, an edge is valid if its last update is an addition.
    ///     In the case of an event graph, an edge is valid if it has at least one addition event.
    ///
    /// Returns:
    ///     GraphView: The filtered graph
    fn valid(&self) -> ValidGraph<DynamicGraph> {
        self.graph.valid()
    }

    /// Applies the filters to the graph and retains the node ids and the edge ids
    /// in the graph that satisfy the filters
    /// creates bitsets per layer for nodes and edges
    ///
    /// Returns:
    ///   GraphView: Returns the masked graph
    fn cache_view(&self) -> CachedView<DynamicGraph> {
        self.graph.cache_view()
    }

    /// Returns a subgraph filtered by node types given a set of node types
    ///
    /// Arguments:
    ///   node_types (list[str]): set of node types
    ///
    /// Returns:
    ///    GraphView: Returns the subgraph
    fn subgraph_node_types(&self, node_types: Vec<ArcStr>) -> NodeTypeFilteredGraph<DynamicGraph> {
        self.graph.subgraph_node_types(node_types)
    }

    /// Returns a subgraph given a set of nodes that are excluded from the subgraph
    ///
    /// Arguments:
    ///   nodes (list[NodeInput]): set of nodes
    ///
    /// Returns:
    ///    GraphView: Returns the subgraph
    fn exclude_nodes(&self, nodes: Vec<PyNodeRef>) -> NodeSubgraph<DynamicGraph> {
        self.graph.exclude_nodes(nodes)
    }

    /// Returns a 'materialized' clone of the graph view - i.e. a new graph with a copy of the data seen within the view instead of just a mask over the original graph
    ///
    /// Returns:
    ///    GraphView: Returns a graph clone
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
