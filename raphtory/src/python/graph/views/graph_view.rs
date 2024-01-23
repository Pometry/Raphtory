//! The API for querying a view of the graph in a read-only state

use crate::{
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError, ArcStr},
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{DynamicGraph, IntoDynamic, MaterializedGraph},
                qnode::BaseNodeViewOps,
                LayerOps, StaticGraphViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            edges::Edges,
            node::NodeView,
            nodes::Nodes,
            views::{
                layer_graph::LayeredGraph, node_subgraph::NodeSubgraph, window_graph::WindowedGraph,
            },
        },
    },
    prelude::*,
    python::{
        types::repr::{Repr, StructReprBuilder},
        utils::PyTime,
    },
};
use chrono::prelude::*;
use pyo3::{
    prelude::*,
    types::{PyBytes, PyDict},
};
use std::collections::HashMap;

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
#[derive(Clone)]
#[repr(C)]
pub struct PyGraphView {
    pub graph: DynamicGraph,
}

impl_timeops!(PyGraphView, graph, DynamicGraph, "GraphView");
impl_layerops!(PyGraphView, graph, DynamicGraph, "GraphView");

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

    /// Converts the graph's nodes into a Pandas DataFrame.
    ///
    /// This method will create a DataFrame with the following columns:
    /// - "name": The name of the node.
    /// - "properties": The properties of the node. This column will be included if `include_node_properties` is set to `true`.
    /// - "property_history": The history of the node's properties. This column will be included if both `include_node_properties` and `include_property_histories` are set to `true`.
    /// - "update_history": The update history of the node. This column will be included if `include_update_history` is set to `true`.
    ///
    /// Args:
    ///     include_node_properties (bool): A boolean wrapped in an Option. If set to `true`, the "properties" and "property_history" columns will be included in the DataFrame. Defaults to `true`.
    ///     include_update_history (bool): A boolean wrapped in an Option. If set to `true`, the "update_history" column will be included in the DataFrame. Defaults to `true`.
    ///     include_property_histories (bool): A boolean wrapped in an Option. If set to `true`, the "property_history" column will be included in the DataFrame. Defaults to `true`.
    ///
    /// Returns:
    ///     If successful, this PyObject will be a Pandas DataFrame.
    #[pyo3(signature = (include_node_properties=true, include_update_history=true, include_property_histories=true))]
    pub fn to_node_df(
        &self,
        include_node_properties: Option<bool>,
        include_update_history: Option<bool>,
        include_property_histories: Option<bool>,
    ) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let pandas = PyModule::import(py, "pandas")?;
            let column_names = vec!["name", "properties", "property_history", "update_history"];
            let node_tuples: Vec<_> = self
                .graph
                .nodes()
                .map(|g, b| {
                    let v = g.node(b).unwrap();
                    let mut properties: Option<HashMap<ArcStr, Prop>> = None;
                    let mut temporal_properties: Option<Vec<(ArcStr, (i64, Prop))>> = None;
                    let mut update_history: Option<Vec<_>> = None;
                    if include_node_properties == Some(true) {
                        if include_property_histories == Some(true) {
                            properties = Some(v.properties().constant().as_map());
                            temporal_properties = Some(v.properties().temporal().histories());
                        } else {
                            properties = Some(v.properties().as_map());
                        }
                    }
                    if include_update_history == Some(true) {
                        update_history = Some(v.history());
                    }
                    (v.name(), properties, temporal_properties, update_history)
                })
                .collect();
            let kwargs = PyDict::new(py);
            kwargs.set_item("columns", column_names)?;
            let df = pandas.call_method("DataFrame", (node_tuples,), Some(kwargs))?;
            let kwargs_drop = PyDict::new(py);
            kwargs_drop.set_item("how", "all")?;
            kwargs_drop.set_item("axis", 1)?;
            kwargs_drop.set_item("inplace", true)?;
            df.call_method("dropna", (), Some(kwargs_drop))?;
            Ok(df.to_object(py))
        })
    }

    /// Converts the graph's edges into a Pandas DataFrame.
    ///
    /// This method will create a DataFrame with the following columns:
    /// - "src": The source node of the edge.
    /// - "dst": The destination node of the edge.
    /// - "layer": The layer of the edge.
    /// - "properties": The properties of the edge. This column will be included if `include_edge_properties` is set to `true`.
    /// - "property_histories": The history of the edge's properties. This column will be included if both `include_edge_properties` and `include_property_histories` are set to `true`.
    /// - "update_history": The update history of the edge. This column will be included if `include_update_history` is set to `true`.
    /// - "update_history_exploded": The exploded update history of the edge. This column will be included if `explode_edges` is set to `true`.
    ///
    /// Args:
    ///     explode_edges (bool): A boolean wrapped in an Option. If set to `true`, the "update_history_exploded" column will be included in the DataFrame. Defaults to `false`.
    ///     include_edge_properties (bool): A boolean wrapped in an Option. If set to `true`, the "properties" and "property_histories" columns will be included in the DataFrame. Defaults to `true`.
    ///     include_update_history (bool): A boolean wrapped in an Option. If set to `true`, the "update_history" column will be included in the DataFrame. Defaults to `true`.
    ///     include_property_histories (bool): A boolean wrapped in an Option. If set to `true`, the "property_histories" column will be included in the DataFrame. Defaults to `true`.
    ///
    /// Returns:
    ///     If successful, this PyObject will be a Pandas DataFrame.
    #[pyo3(signature = (explode_edges=false, include_edge_properties=true, include_update_history=true, include_property_histories=true))]
    pub fn to_edge_df(
        &self,
        explode_edges: Option<bool>,
        include_edge_properties: Option<bool>,
        include_update_history: Option<bool>,
        include_property_histories: Option<bool>,
    ) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let pandas = PyModule::import(py, "pandas")?;
            let column_names = vec![
                "src",
                "dst",
                "layer",
                "properties",
                "property_histories",
                "update_history",
                "update_history_exploded",
            ];
            let mut edges = self.graph.edges();
            if explode_edges == Some(true) {
                edges = self.graph.edges().explode_layers().explode();
            }
            let edge_tuples: Vec<_> = edges
                .iter()
                .map(|e| {
                    let mut properties: Option<HashMap<ArcStr, Prop>> = None;
                    let mut temporal_properties: Option<Vec<(ArcStr, (i64, Prop))>> = None;
                    if include_edge_properties == Some(true) {
                        if include_property_histories == Some(true) {
                            properties = Some(e.properties().constant().as_map());
                            temporal_properties = Some(e.properties().temporal().histories());
                        } else {
                            properties = Some(e.properties().as_map());
                        }
                    }
                    let mut update_history_exploded: Option<i64> = None;
                    let mut update_history: Option<Vec<_>> = None;
                    if include_update_history == Some(true) {
                        if explode_edges == Some(true) {
                            update_history_exploded = e.time();
                        } else {
                            update_history = Some(e.history());
                        }
                    }
                    (
                        e.src().name(),
                        e.dst().name(),
                        e.layer_name(),
                        properties,
                        temporal_properties,
                        update_history,
                        update_history_exploded,
                    )
                })
                .collect();
            let kwargs = PyDict::new(py);
            kwargs.set_item("columns", column_names)?;
            let df = pandas.call_method("DataFrame", (edge_tuples,), Some(kwargs))?;
            let kwargs_drop = PyDict::new(py);
            kwargs_drop.set_item("how", "all")?;
            kwargs_drop.set_item("axis", 1)?;
            kwargs_drop.set_item("inplace", true)?;
            df.call_method("dropna", (), Some(kwargs_drop))?;
            Ok(df.to_object(py))
        })
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
