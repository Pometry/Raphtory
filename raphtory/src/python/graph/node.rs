//! Defines the `Node`, which represents a node in the graph.
//! A node is a node in the graph, and can have properties and edges.
//! It can also be used to navigate the graph.
use crate::{
    core::{
        entities::nodes::node_ref::NodeRef,
        utils::{errors::GraphError, time::error::ParseTimeError},
        Prop,
    },
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{DynamicGraph, Immutable, IntoDynamic, MaterializedGraph},
                *,
            },
        },
        graph::{
            // path::{PathFromGraph, PathFromNode},
            node::NodeView,
            nodes::Nodes,
            views::{deletion_graph::GraphWithDeletions, layer_graph::LayeredGraph},
        },
    },
    prelude::Graph,
    python::{
        graph::{
            edge::{PyEdges, PyNestedEdges},
            properties::{PyNestedPropsIterable, PyPropsList},
        },
        types::wrappers::iterators::*,
        utils::{PyInterval, PyTime},
    },
    *,
};
use crate::{
    db::graph::path::{PathFromGraph, PathFromNode},
    python::types::repr::StructReprBuilder,
};
use chrono::NaiveDateTime;

use pyo3::{
    exceptions::{PyIndexError, PyKeyError},
    prelude::*,
    pyclass,
    pyclass::CompareOp,
    pymethods, PyAny, PyObject, PyRef, PyRefMut, PyResult, Python,
};
use python::types::repr::{iterator_repr, Repr};
use std::collections::HashMap;

/// A node (or node) in the graph.
#[pyclass(name = "Node", subclass)]
#[derive(Clone)]
pub struct PyNode {
    node: NodeView<DynamicGraph, DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<NodeView<G, GH>> for PyNode
{
    fn from(value: NodeView<G, GH>) -> Self {
        let base_graph = value.base_graph.into_dynamic();
        let graph = value.graph.into_dynamic();
        let node = NodeView {
            base_graph,
            graph,
            node: value.node,
        };
        Self { node }
    }
}

/// Converts a python node into a rust node.
impl From<PyNode> for NodeRef {
    fn from(value: PyNode) -> Self {
        value.node.into()
    }
}

/// Defines the `Node`, which represents a node in the graph.
/// A node is a node in the graph, and can have properties and edges.
/// It can also be used to navigate the graph.
#[pymethods]
impl PyNode {
    /// Rich Comparison for Node objects
    pub fn __richcmp__(&self, other: PyRef<PyNode>, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        match op {
            CompareOp::Eq => (self.node.id() == other.id()).into_py(py),
            CompareOp::Ne => (self.node.id() != other.id()).into_py(py),
            CompareOp::Lt => (self.node.id() < other.id()).into_py(py),
            CompareOp::Le => (self.node.id() <= other.id()).into_py(py),
            CompareOp::Gt => (self.node.id() > other.id()).into_py(py),
            CompareOp::Ge => (self.node.id() >= other.id()).into_py(py),
        }
    }

    /// TODO: uncomment when we update to py03 0.2
    /// checks if a node is equal to another by their id (ids are unqiue)
    ///
    /// Arguments:
    ///    other: The other node to compare to.
    ///
    /// Returns:
    ///   True if the nodes are equal, false otherwise.
    // pub fn __eq__(&self, other: &PyNode) -> bool {
    //     self.node.id() == other.node.id()
    // }

    /// Returns the hash of the node.
    ///
    /// Returns:
    ///   The node id.
    pub fn __hash__(&self) -> u64 {
        self.node.id()
    }

    /// Returns the id of the node.
    /// This is a unique identifier for the node.
    ///
    /// Returns:
    ///    The id of the node as an integer.
    #[getter]
    pub fn id(&self) -> u64 {
        self.node.id()
    }

    /// Returns the name of the node.
    ///
    /// Returns:
    ///     The name of the node as a string.
    #[getter]
    pub fn name(&self) -> String {
        self.node.name()
    }

    /// Returns the earliest time that the node exists.
    ///
    /// Returns:
    ///     The earliest time that the node exists as an integer.
    #[getter]
    pub fn earliest_time(&self) -> Option<i64> {
        self.node.earliest_time()
    }

    /// Returns the earliest datetime that the node exists.
    ///
    /// Returns:
    ///     The earliest datetime that the node exists as an integer.
    #[getter]
    pub fn earliest_date_time(&self) -> Option<NaiveDateTime> {
        self.node.earliest_date_time()
    }

    /// Returns the latest time that the node exists.
    ///
    /// Returns:
    ///     The latest time that the node exists as an integer.
    #[getter]
    pub fn latest_time(&self) -> Option<i64> {
        self.node.latest_time()
    }

    /// Returns the latest datetime that the node exists.
    ///
    /// Arguments:
    ///    None
    ///
    /// Returns:
    ///     The latest datetime that the node exists as an integer.
    #[getter]
    pub fn latest_date_time(&self) -> Option<NaiveDateTime> {
        self.node.latest_date_time()
    }

    /// The properties of the node
    ///
    /// Returns:
    ///     A list of properties.
    #[getter]
    pub fn properties(&self) -> Properties<NodeView<DynamicGraph, DynamicGraph>> {
        self.node.properties()
    }

    /// Get the degree of this node (i.e., the number of edges that are incident to it).
    ///
    /// Returns
    ///     The degree of this node.
    pub fn degree(&self) -> usize {
        self.node.degree()
    }

    /// Get the in-degree of this node (i.e., the number of edges that are incident to it from other nodes).
    ///
    /// Returns:
    ///    The in-degree of this node.
    pub fn in_degree(&self) -> usize {
        self.node.in_degree()
    }

    /// Get the out-degree of this node (i.e., the number of edges that are incident to it from this node).
    ///
    /// Returns:
    ///   The out-degree of this node.
    pub fn out_degree(&self) -> usize {
        self.node.out_degree()
    }

    /// Get the edges that are pointing to or from this node.
    ///
    /// Returns:
    ///     A list of `Edge` objects.
    #[getter]
    pub fn edges(&self) -> PyEdges {
        let node = self.node.clone();
        (move || node.edges()).into()
    }

    /// Get the edges that are pointing to this node.
    ///
    /// Returns:
    ///     A list of `Edge` objects.
    #[getter]
    pub fn in_edges(&self) -> PyEdges {
        let node = self.node.clone();
        (move || node.in_edges()).into()
    }

    /// Get the edges that are pointing from this node.
    ///
    /// Returns:
    ///    A list of `Edge` objects.
    #[getter]
    pub fn out_edges(&self) -> PyEdges {
        let node = self.node.clone();
        (move || node.out_edges()).into()
    }

    /// Get the neighbours of this node.
    ///
    /// Returns:
    ///
    ///    A list of `Node` objects.
    #[getter]
    pub fn neighbours(&self) -> PyPathFromNode {
        self.node.neighbours().into()
    }

    /// Get the neighbours of this node that are pointing to it.
    ///
    /// Returns:
    ///   A list of `Node` objects.
    #[getter]
    pub fn in_neighbours(&self) -> PyPathFromNode {
        self.node.in_neighbours().into()
    }

    /// Get the neighbours of this node that are pointing from it.
    ///
    /// Returns:
    ///   A list of `Node` objects.
    #[getter]
    pub fn out_neighbours(&self) -> PyPathFromNode {
        self.node.out_neighbours().into()
    }

    #[doc = default_layer_doc_string!()]
    pub fn default_layer(&self) -> PyNode {
        self.node.default_layer().into()
    }

    #[doc = layers_doc_string!()]
    #[pyo3(signature = (names))]
    pub fn layers(
        &self,
        names: Vec<String>,
    ) -> Option<NodeView<DynamicGraph, LayeredGraph<DynamicGraph>>> {
        self.node.layer(names)
    }

    #[doc = layers_name_doc_string!()]
    #[pyo3(signature = (name))]
    pub fn layer(
        &self,
        name: String,
    ) -> Option<NodeView<DynamicGraph, LayeredGraph<DynamicGraph>>> {
        self.node.layer(name)
    }

    /// Returns the history of a node, including node additions and changes made to node.
    ///
    /// Returns:
    ///     A list of unix timestamps of the event history of node.
    pub fn history(&self) -> Vec<i64> {
        self.node.history()
    }

    /// Returns the history of a node, including node additions and changes made to node.
    ///
    /// Returns:
    ///     A list of timestamps of the event history of node.
    ///
    pub fn history_date_time(&self) -> Option<Vec<NaiveDateTime>> {
        self.node.history_date_time()
    }

    //******  Python  ******//
    pub fn __getitem__(&self, name: &str) -> PyResult<Prop> {
        self.node
            .properties()
            .get(name)
            .ok_or(PyKeyError::new_err(format!("Unknown property {}", name)))
    }

    /// Display the node as a string.
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl_timeops!(PyNode, node, NodeView<DynamicGraph, DynamicGraph>, "node");

impl Repr for PyNode {
    fn repr(&self) -> String {
        self.node.repr()
    }
}

impl<G: StaticGraphViewOps, GH: StaticGraphViewOps> Repr for NodeView<G, GH> {
    fn repr(&self) -> String {
        if self.properties().is_empty() {
            StructReprBuilder::new("Node")
                .add_field("name", self.name())
                .add_field("earliest_time", self.earliest_time())
                .add_field("latest_time", self.latest_time())
                .finish()
        } else {
            StructReprBuilder::new("Node")
                .add_field("name", self.name())
                .add_field("earliest_time", self.earliest_time())
                .add_field("latest_time", self.latest_time())
                .add_field("properties", self.properties())
                .finish()
        }
    }
}

#[pyclass(name = "MutableNode", extends=PyNode)]
pub struct PyMutableNode {
    node: NodeView<MaterializedGraph, MaterializedGraph>,
}

impl Repr for PyMutableNode {
    fn repr(&self) -> String {
        self.node.repr()
    }
}

impl From<NodeView<MaterializedGraph, MaterializedGraph>> for PyMutableNode {
    fn from(node: NodeView<MaterializedGraph, MaterializedGraph>) -> Self {
        Self { node }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic + Immutable>
    IntoPy<PyObject> for NodeView<G, GH>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyNode::from(self).into_py(py)
    }
}

impl IntoPy<PyObject> for NodeView<Graph, Graph> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let graph: MaterializedGraph = self.graph.into();
        let base_graph = graph.clone();
        let node = self.node;
        let node = NodeView {
            base_graph,
            graph,
            node,
        };
        node.into_py(py)
    }
}

impl IntoPy<PyObject> for NodeView<GraphWithDeletions, GraphWithDeletions> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let graph: MaterializedGraph = self.graph.into();
        let base_graph = graph.clone();
        let node = self.node;
        let node = NodeView {
            base_graph,
            graph,
            node,
        };
        node.into_py(py)
    }
}

impl IntoPy<PyObject> for NodeView<MaterializedGraph, MaterializedGraph> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, (PyMutableNode::from(self.clone()), PyNode::from(self)))
            .unwrap() // I think this only fails if we are out of memory? Seems to be unavoidable!
            .into_py(py)
    }
}

#[pymethods]
impl PyMutableNode {
    /// Add updates to a node in the graph at a specified time.
    /// This function allows for the addition of property updates to a node within the graph. The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Parameters:
    ///     t (PyTime): The timestamp at which the updates should be applied.
    ///     properties (Optional[Dict[str, Prop]]): A dictionary of properties to update.
    ///         Each key is a string representing the property name, and each value is of type Prop representing the property value.
    ///         If None, no properties are updated.
    ///
    /// Returns:
    ///     Result: A result object indicating success or failure. On failure, it contains a GraphError.
    pub fn add_updates(
        &self,
        t: PyTime,
        properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        self.node.add_updates(t, properties.unwrap_or_default())
    }

    /// Add constant properties to a node in the graph.
    /// This function is used to add properties to a node that remain constant and do not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Parameters:
    ///     properties (Dict[str, Prop]): A dictionary of properties to be added to the node.
    ///     Each key is a string representing the property name, and each value is of type Prop
    ///     representing the property value.
    ///
    /// Returns:
    ///     Result: A result object indicating success or failure. On failure, it contains a GraphError..
    pub fn add_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        self.node.add_constant_properties(properties)
    }

    /// Return a string representation of the node.
    /// This method provides a human-readable representation of the node, which is useful for
    /// debugging and logging purposes.
    ///
    /// Returns:
    ///     str: A string representation of the node.
    fn __repr__(&self) -> String {
        self.repr()
    }
}

/// A list of nodes that can be iterated over.
#[pyclass(name = "Nodes")]
pub struct PyNodes {
    pub(crate) nodes: Nodes<'static, DynamicGraph, DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<Nodes<'static, G, GH>> for PyNodes
{
    fn from(value: Nodes<'static, G, GH>) -> Self {
        let graph = value.graph.into_dynamic();
        let base_graph = value.base_graph.into_dynamic();
        Self {
            nodes: Nodes::new_filtered(base_graph, graph),
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject>
    for Nodes<'static, G, GH>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyNodes::from(self).into_py(py)
    }
}

/// Operations on a list of nodes.
/// These use all the same functions as a normal node except it returns a list of results.
#[pymethods]
impl PyNodes {
    /// checks if a list of nodes is equal to another list by their idd (ids are unique)
    ///
    /// Arguments:
    ///    other: The other nodes to compare to.
    ///
    /// Returns:
    ///   True if the nodes are equal, false otherwise.
    fn __eq__(&self, other: &PyNodes) -> bool {
        for (v1, v2) in self.nodes.iter().zip(other.nodes.iter()) {
            if v1.id() != v2.id() {
                return false;
            }
        }
        true
    }

    /// Returns an iterator over the nodes ids
    #[getter]
    fn id(&self) -> U64Iterable {
        let nodes = self.nodes.clone();
        (move || nodes.id()).into()
    }

    /// Returns an iterator over the nodes name
    #[getter]
    fn name(&self) -> StringIterable {
        let nodes = self.nodes.clone();
        (move || nodes.name()).into()
    }

    /// Returns an iterator over the nodes earliest time
    #[getter]
    fn earliest_time(&self) -> OptionI64Iterable {
        let nodes = self.nodes.clone();
        (move || nodes.earliest_time()).into()
    }

    /// Returns an iterator over the nodes latest time
    #[getter]
    fn latest_time(&self) -> OptionI64Iterable {
        let nodes = self.nodes.clone();
        (move || nodes.latest_time()).into()
    }

    /// The properties of the node
    ///
    /// Returns:
    ///     A List of properties
    #[getter]
    fn properties(&self) -> PyPropsList {
        let nodes = self.nodes.clone();
        (move || nodes.properties()).into()
    }

    /// Returns the number of edges of the nodes
    ///
    /// Returns:
    ///     An iterator of the number of edges of the nodes
    fn degree(&self) -> UsizeIterable {
        let nodes = self.nodes.clone();
        (move || nodes.degree()).into()
    }

    /// Returns the number of in edges of the nodes
    ///
    /// Returns:
    ///     An iterator of the number of in edges of the nodes
    fn in_degree(&self) -> UsizeIterable {
        let nodes = self.nodes.clone();
        (move || nodes.in_degree()).into()
    }

    /// Returns the number of out edges of the nodes
    ///
    /// Returns:
    ///     An iterator of the number of out edges of the nodes
    fn out_degree(&self) -> UsizeIterable {
        let nodes = self.nodes.clone();
        (move || nodes.out_degree()).into()
    }

    /// Returns the edges of the nodes
    ///
    /// Returns:
    ///     An iterator of edges of the nodes
    #[getter]
    fn edges(&self) -> PyNestedEdges {
        let clone = self.nodes.clone();
        (move || clone.edges()).into()
    }

    /// Returns the in edges of the nodes
    ///
    /// Returns:
    ///     An iterator of in edges of the nodes
    #[getter]
    fn in_edges(&self) -> PyNestedEdges {
        let clone = self.nodes.clone();
        (move || clone.in_edges()).into()
    }

    /// Returns the out edges of the nodes
    ///
    /// Returns:
    ///     An iterator of out edges of the nodes
    #[getter]
    fn out_edges(&self) -> PyNestedEdges {
        let clone = self.nodes.clone();
        (move || clone.out_edges()).into()
    }

    /// Get the neighbours of the nodes
    ///
    /// Returns:
    ///     An iterator of the neighbours of the nodes
    #[getter]
    fn neighbours(&self) -> PyPathFromGraph {
        self.nodes.neighbours().into()
    }

    /// Get the in neighbours of the nodes
    ///
    /// Returns:
    ///     An iterator of the in neighbours of the nodes
    #[getter]
    fn in_neighbours(&self) -> PyPathFromGraph {
        self.nodes.in_neighbours().into()
    }

    /// Get the out neighbours of the nodes
    ///
    /// Returns:
    ///     An iterator of the out neighbours of the nodes
    #[getter]
    fn out_neighbours(&self) -> PyPathFromGraph {
        self.nodes.out_neighbours().into()
    }

    /// Collects all nodes into a list
    fn collect(&self) -> Vec<PyNode> {
        self.__iter__().into_iter().collect()
    }
    #[doc = default_layer_doc_string!()]
    pub fn default_layer(&self) -> PyNodes {
        self.nodes.default_layer().into()
    }

    #[doc = layers_doc_string!()]
    #[pyo3(signature = (name))]
    pub fn layer(
        &self,
        name: &str,
    ) -> Option<Nodes<'static, DynamicGraph, LayeredGraph<DynamicGraph>>> {
        self.nodes.layer(name)
    }

    //****** Python *******
    pub fn __iter__(&self) -> PyNodeIterator {
        self.nodes.iter().into()
    }

    pub fn __len__(&self) -> usize {
        self.nodes.len()
    }

    pub fn __bool__(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn __getitem__(&self, node: NodeRef) -> PyResult<NodeView<DynamicGraph, DynamicGraph>> {
        self.nodes
            .get(node)
            .ok_or_else(|| PyIndexError::new_err("Node does not exist"))
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl_timeops!(
    PyNodes,
    nodes,
    Nodes<'static, DynamicGraph, DynamicGraph>,
    "nodes"
);

impl Repr for PyNodes {
    fn repr(&self) -> String {
        format!("Nodes({})", iterator_repr(self.__iter__().into_iter()))
    }
}

#[pyclass(name = "PathFromGraph")]
pub struct PyPathFromGraph {
    path: PathFromGraph<'static, DynamicGraph, DynamicGraph>,
}

#[pymethods]
impl PyPathFromGraph {
    fn __iter__(&self) -> PathIterator {
        self.path.iter().into()
    }

    fn collect(&self) -> Vec<Vec<PyNode>> {
        self.__iter__().into_iter().map(|it| it.collect()).collect()
    }
    #[getter]
    fn id(&self) -> NestedU64Iterable {
        let path = self.path.clone();
        (move || path.id()).into()
    }

    #[getter]
    fn name(&self) -> NestedStringIterable {
        let path = self.path.clone();
        (move || path.name()).into()
    }

    #[getter]
    fn earliest_time(&self) -> NestedOptionI64Iterable {
        let path = self.path.clone();
        (move || path.earliest_time()).into()
    }

    #[getter]
    fn latest_time(&self) -> NestedOptionI64Iterable {
        let path = self.path.clone();
        (move || path.latest_time()).into()
    }

    #[getter]
    fn properties(&self) -> PyNestedPropsIterable {
        let path = self.path.clone();
        (move || path.properties()).into()
    }

    fn degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.degree()).into()
    }

    fn in_degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.in_degree()).into()
    }

    fn out_degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.out_degree()).into()
    }

    #[getter]
    fn edges(&self) -> PyNestedEdges {
        let clone = self.path.clone();
        (move || clone.edges()).into()
    }

    #[getter]
    fn in_edges(&self) -> PyNestedEdges {
        let clone = self.path.clone();
        (move || clone.in_edges()).into()
    }

    #[getter]
    fn out_edges(&self) -> PyNestedEdges {
        let clone = self.path.clone();
        (move || clone.out_edges()).into()
    }

    #[getter]
    fn out_neighbours(&self) -> Self {
        self.path.out_neighbours().into()
    }

    #[getter]
    fn in_neighbours(&self) -> Self {
        self.path.in_neighbours().into()
    }

    #[getter]
    fn neighbours(&self) -> Self {
        self.path.neighbours().into()
    }

    #[doc = default_layer_doc_string!()]
    pub fn default_layer(&self) -> Self {
        self.path.default_layer().into()
    }

    #[doc = layers_doc_string!()]
    #[pyo3(signature = (name))]
    pub fn layer(
        &self,
        name: &str,
    ) -> Option<PathFromGraph<'static, DynamicGraph, LayeredGraph<DynamicGraph>>> {
        self.path.layer(name)
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}

impl_timeops!(
    PyPathFromGraph,
    path,
    PathFromGraph<'static, DynamicGraph, DynamicGraph>,
    "path"
);

impl Repr for PyPathFromGraph {
    fn repr(&self) -> String {
        format!(
            "PathFromGraph({})",
            iterator_repr(self.__iter__().into_iter())
        )
    }
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<PathFromGraph<'static, G, GH>> for PyPathFromGraph
{
    fn from(value: PathFromGraph<'static, G, GH>) -> Self {
        Self {
            path: PathFromGraph {
                base_graph: value.base_graph.clone().into_dynamic(),
                graph: value.graph.clone().into_dynamic(),
                op: value.op.clone(),
            },
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject>
    for PathFromGraph<'static, G, GH>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyPathFromGraph::from(self).into_py(py)
    }
}

#[pyclass(name = "PathFromNode")]
pub struct PyPathFromNode {
    path: PathFromNode<'static, DynamicGraph, DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<PathFromNode<'static, G, GH>> for PyPathFromNode
{
    fn from(value: PathFromNode<'static, G, GH>) -> Self {
        Self {
            path: PathFromNode {
                graph: value.graph.clone().into_dynamic(),
                base_graph: value.base_graph.clone().into_dynamic(),
                node: value.node,
                op: value.op.clone(),
            },
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject>
    for PathFromNode<'static, G, GH>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyPathFromNode::from(self).into_py(py)
    }
}

#[pymethods]
impl PyPathFromNode {
    fn __iter__(&self) -> PyNodeIterator {
        self.path.iter().into()
    }

    fn collect(&self) -> Vec<PyNode> {
        self.__iter__().into_iter().collect()
    }

    #[getter]
    fn id(&self) -> U64Iterable {
        let path = self.path.clone();
        (move || path.id()).into()
    }

    #[getter]
    fn name(&self) -> StringIterable {
        let path = self.path.clone();
        (move || path.name()).into()
    }

    #[getter]
    fn earliest_time(&self) -> OptionI64Iterable {
        let path = self.path.clone();
        (move || path.earliest_time()).into()
    }

    #[getter]
    fn latest_time(&self) -> OptionI64Iterable {
        let path = self.path.clone();
        (move || path.latest_time()).into()
    }

    #[getter]
    fn properties(&self) -> PyPropsList {
        let path = self.path.clone();
        (move || path.properties()).into()
    }

    fn in_degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.in_degree()).into()
    }

    fn out_degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.out_degree()).into()
    }

    fn degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.degree()).into()
    }

    #[getter]
    fn edges(&self) -> PyEdges {
        let path = self.path.clone();
        (move || path.edges()).into()
    }

    #[getter]
    fn in_edges(&self) -> PyEdges {
        let path = self.path.clone();
        (move || path.in_edges()).into()
    }

    #[getter]
    fn out_edges(&self) -> PyEdges {
        let path = self.path.clone();
        (move || path.out_edges()).into()
    }

    #[getter]
    fn out_neighbours(&self) -> Self {
        self.path.out_neighbours().into()
    }

    #[getter]
    fn in_neighbours(&self) -> Self {
        self.path.in_neighbours().into()
    }

    #[getter]
    fn neighbours(&self) -> Self {
        self.path.neighbours().into()
    }

    pub fn default_layer(&self) -> Self {
        self.path.default_layer().into()
    }

    #[doc = layers_doc_string!()]
    #[pyo3(signature = (name))]
    pub fn layer(
        &self,
        name: &str,
    ) -> Option<PathFromNode<'static, DynamicGraph, LayeredGraph<DynamicGraph>>> {
        self.path.layer(name)
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}

impl_timeops!(
    PyPathFromNode,
    path,
    PathFromNode<'static, DynamicGraph, DynamicGraph>,
    "path"
);

impl Repr for PyPathFromNode {
    fn repr(&self) -> String {
        format!(
            "PathFromNode({})",
            iterator_repr(self.__iter__().into_iter())
        )
    }
}

#[pyclass(name = "NodeIterator")]
pub struct PyNodeIterator {
    iter: Box<dyn Iterator<Item = PyNode> + Send>,
}

impl<I: Iterator<Item = NodeView<DynamicGraph, DynamicGraph>> + Send + 'static> From<I>
    for PyNodeIterator
{
    fn from(value: I) -> Self {
        Self {
            iter: Box::new(value.map(|v| v.into())),
        }
    }
}

impl IntoIterator for PyNodeIterator {
    type Item = PyNode;
    type IntoIter = Box<dyn Iterator<Item = PyNode> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

#[pymethods]
impl PyNodeIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyNode> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct PathIterator {
    pub(crate) iter: Box<dyn Iterator<Item = PyPathFromNode> + Send>,
}

impl IntoIterator for PathIterator {
    type Item = PyPathFromNode;
    type IntoIter = Box<dyn Iterator<Item = PyPathFromNode> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

impl<I: Iterator<Item = P> + Send + 'static, P: Into<PyPathFromNode>> From<I> for PathIterator {
    fn from(value: I) -> Self {
        Self {
            iter: Box::new(value.map(|path| path.into())),
        }
    }
}

#[pymethods]
impl PathIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyPathFromNode> {
        slf.iter.next()
    }
}

py_iterable!(PyNodeIterable, NodeView<DynamicGraph, DynamicGraph>, PyNode);

#[pymethods]
impl PyNodeIterable {
    #[getter]
    fn id(&self) -> U64Iterable {
        let builder = self.builder.clone();
        (move || builder().id()).into()
    }

    #[getter]
    fn name(&self) -> StringIterable {
        let nodes = self.builder.clone();
        (move || nodes().name()).into()
    }

    #[getter]
    fn earliest_time(&self) -> OptionI64Iterable {
        let nodes = self.builder.clone();
        (move || nodes().earliest_time()).into()
    }

    #[getter]
    fn latest_time(&self) -> OptionI64Iterable {
        let nodes = self.builder.clone();
        (move || nodes().latest_time()).into()
    }

    #[getter]
    fn properties(&self) -> PyPropsList {
        let nodes = self.builder.clone();
        (move || nodes().properties()).into()
    }

    fn degree(&self) -> UsizeIterable {
        let nodes = self.builder.clone();
        (move || nodes().degree()).into()
    }

    fn in_degree(&self) -> UsizeIterable {
        let nodes = self.builder.clone();
        (move || nodes().in_degree()).into()
    }

    fn out_degree(&self) -> UsizeIterable {
        let nodes = self.builder.clone();
        (move || nodes().out_degree()).into()
    }

    #[getter]
    fn edges(&self) -> PyEdges {
        let clone = self.builder.clone();
        (move || clone().edges()).into()
    }

    #[getter]
    fn in_edges(&self) -> PyEdges {
        let clone = self.builder.clone();
        (move || clone().in_edges()).into()
    }

    #[getter]
    fn out_edges(&self) -> PyEdges {
        let clone = self.builder.clone();
        (move || clone().out_edges()).into()
    }

    #[getter]
    fn out_neighbours(&self) -> Self {
        let builder = self.builder.clone();
        (move || builder().out_neighbours()).into()
    }

    #[getter]
    fn in_neighbours(&self) -> Self {
        let builder = self.builder.clone();
        (move || builder().in_neighbours()).into()
    }

    #[getter]
    fn neighbours(&self) -> Self {
        let builder = self.builder.clone();
        (move || builder().neighbours()).into()
    }
}

py_nested_iterable!(PyNestedNodeIterable, NodeView<DynamicGraph, DynamicGraph>);

#[pymethods]
impl PyNestedNodeIterable {
    #[getter]
    fn id(&self) -> NestedU64Iterable {
        let builder = self.builder.clone();
        (move || builder().id()).into()
    }

    #[getter]
    fn name(&self) -> NestedStringIterable {
        let nodes = self.builder.clone();
        (move || nodes().name()).into()
    }

    #[getter]
    fn earliest_time(&self) -> NestedOptionI64Iterable {
        let nodes = self.builder.clone();
        (move || nodes().earliest_time()).into()
    }

    #[getter]
    fn latest_time(&self) -> NestedOptionI64Iterable {
        let nodes = self.builder.clone();
        (move || nodes().latest_time()).into()
    }

    #[getter]
    fn properties(&self) -> PyNestedPropsIterable {
        let nodes = self.builder.clone();
        (move || nodes().properties()).into()
    }

    fn degree(&self) -> NestedUsizeIterable {
        let nodes = self.builder.clone();
        (move || nodes().degree()).into()
    }

    fn in_degree(&self) -> NestedUsizeIterable {
        let nodes = self.builder.clone();
        (move || nodes().in_degree()).into()
    }

    fn out_degree(&self) -> NestedUsizeIterable {
        let nodes = self.builder.clone();
        (move || nodes().out_degree()).into()
    }

    #[getter]
    fn edges(&self) -> PyNestedEdges {
        let clone = self.builder.clone();
        (move || clone().edges()).into()
    }

    #[getter]
    fn in_edges(&self) -> PyNestedEdges {
        let clone = self.builder.clone();
        (move || clone().in_edges()).into()
    }

    #[getter]
    fn out_edges(&self) -> PyNestedEdges {
        let clone = self.builder.clone();
        (move || clone().out_edges()).into()
    }

    #[getter]
    fn out_neighbours(&self) -> Self {
        let builder = self.builder.clone();
        (move || builder().out_neighbours()).into()
    }

    #[getter]
    fn in_neighbours(&self) -> Self {
        let builder = self.builder.clone();
        (move || builder().in_neighbours()).into()
    }

    #[getter]
    fn neighbours(&self) -> Self {
        let builder = self.builder.clone();
        (move || builder().neighbours()).into()
    }
}
