//! Defines the `Node`, which represents a node in the graph.
//! A node is a node in the graph, and can have properties and edges.
//! It can also be used to navigate the graph.
use crate::{
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError, ArcStr, Prop},
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{DynamicGraph, Immutable, IntoDynamic, MaterializedGraph},
                *,
            },
        },
        graph::{
            node::NodeView,
            nodes::Nodes,
            path::{PathFromGraph, PathFromNode},
            views::deletion_graph::GraphWithDeletions,
        },
    },
    prelude::Graph,
    python::{
        graph::properties::{PyNestedPropsIterable, PyPropsList},
        types::{repr::StructReprBuilder, wrappers::iterators::*},
        utils::PyTime,
    },
    *,
};
use chrono::{DateTime, Utc};
use pyo3::{
    exceptions::{PyIndexError, PyKeyError},
    prelude::*,
    pyclass,
    pyclass::CompareOp,
    pymethods, PyAny, PyObject, PyRef, PyResult, Python,
};
use python::types::repr::{iterator_repr, Repr};
use std::collections::HashMap;

/// A node (or node) in the graph.
#[pyclass(name = "Node", subclass)]
#[derive(Clone)]
pub struct PyNode {
    pub node: NodeView<DynamicGraph, DynamicGraph>,
}

impl_nodeviewops!(PyNode, node, NodeView<DynamicGraph>, "Node");

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
    pub fn earliest_date_time(&self) -> Option<DateTime<Utc>> {
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
    pub fn latest_date_time(&self) -> Option<DateTime<Utc>> {
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

    /// Returns the type of node
    #[getter]
    pub fn node_type(&self) -> Option<ArcStr> {
        self.node.node_type()
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
    pub fn history_date_time(&self) -> Option<Vec<DateTime<Utc>>> {
        self.node.history_date_time()
    }

    //******  Python  ******//
    pub fn __getitem__(&self, name: &str) -> PyResult<Prop> {
        self.node
            .properties()
            .get(name)
            .ok_or(PyKeyError::new_err(format!("Unknown property {}", name)))
    }
}

impl Repr for PyNode {
    fn repr(&self) -> String {
        self.node.repr()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Repr for NodeView<G, GH> {
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

impl_nodeviewops!(
    PyNodes,
    nodes,
    Nodes<'static, DynamicGraph, DynamicGraph>,
    "Nodes"
);
impl_iterable_mixin!(
    PyNodes,
    nodes,
    Vec<NodeView<DynamicGraph>>,
    "list[Node]",
    "node"
);

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

    /// Returns the earliest time of the nodes.
    ///
    /// Returns:
    /// Earliest time of the nodes.
    #[getter]
    fn earliest_date_time(&self) -> OptionUtcDateTimeIterable {
        let nodes = self.nodes.clone();
        (move || nodes.earliest_date_time()).into()
    }

    /// Returns an iterator over the nodes latest time
    #[getter]
    fn latest_time(&self) -> OptionI64Iterable {
        let nodes = self.nodes.clone();
        (move || nodes.latest_time()).into()
    }

    /// Returns the latest date time of the nodes.
    ///
    /// Returns:
    ///   Latest date time of the nodes.
    #[getter]
    fn latest_date_time(&self) -> OptionUtcDateTimeIterable {
        let nodes = self.nodes.clone();
        (move || nodes.latest_date_time()).into()
    }

    /// Returns all timestamps of nodes, when an node is added or change to an node is made.
    ///
    /// Returns:
    ///    A list of unix timestamps.
    ///
    fn history(&self) -> I64VecIterable {
        let nodes = self.nodes.clone();
        (move || nodes.history()).into()
    }

    /// Returns the type of node
    #[getter]
    fn node_type(&self) -> OptionArcStringIterable {
        let nodes = self.nodes.clone();
        (move || nodes.node_type()).into()
    }

    /// Returns all timestamps of nodes, when an node is added or change to an node is made.
    ///
    /// Returns:
    ///    An  list of timestamps.
    ///
    fn history_date_time(&self) -> OptionVecUtcDateTimeIterable {
        let nodes = self.nodes.clone();
        (move || nodes.history_date_time()).into()
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

    pub fn __getitem__(&self, node: NodeRef) -> PyResult<NodeView<DynamicGraph, DynamicGraph>> {
        self.nodes
            .get(node)
            .ok_or_else(|| PyIndexError::new_err("Node does not exist"))
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Repr for Nodes<'graph, G, GH> {
    fn repr(&self) -> String {
        format!("Nodes({})", iterator_repr(self.iter()))
    }
}

#[pyclass(name = "PathFromGraph")]
pub struct PyPathFromGraph {
    path: PathFromGraph<'static, DynamicGraph, DynamicGraph>,
}

impl_nodeviewops!(
    PyPathFromGraph,
    path,
    PathFromGraph<'static, DynamicGraph, DynamicGraph>,
    "PathFromGraph"
);
impl_iterable_mixin!(
    PyPathFromGraph,
    path,
    Vec<Vec<NodeView<DynamicGraph>>>,
    "list[list[Node]]",
    "node"
);

#[pymethods]
impl PyPathFromGraph {
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
    fn node_type(&self) -> NestedOptionArcStringIterable {
        let path = self.path.clone();
        (move || path.node_type()).into()
    }

    #[getter]
    fn earliest_time(&self) -> NestedOptionI64Iterable {
        let path = self.path.clone();
        (move || path.earliest_time()).into()
    }

    /// Returns the earliest date time of the nodes.
    #[getter]
    fn earliest_date_time(&self) -> NestedUtcDateTimeIterable {
        let path = self.path.clone();
        (move || path.earliest_date_time()).into()
    }

    #[getter]
    fn latest_time(&self) -> NestedOptionI64Iterable {
        let path = self.path.clone();
        (move || path.latest_time()).into()
    }

    /// Returns the latest date time of the nodes.
    #[getter]
    fn latest_date_time(&self) -> NestedUtcDateTimeIterable {
        let path = self.path.clone();
        (move || path.latest_date_time()).into()
    }

    /// Returns all timestamps of nodes, when an node is added or change to an node is made.
    fn history(&self) -> NestedI64VecIterable {
        let path = self.path.clone();
        (move || path.history()).into()
    }

    /// Returns all timestamps of nodes, when an node is added or change to an node is made.
    fn history_date_time(&self) -> NestedVecUtcDateTimeIterable {
        let path = self.path.clone();
        (move || path.history_date_time()).into()
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
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Repr
    for PathFromGraph<'graph, G, GH>
{
    fn repr(&self) -> String {
        format!("PathFromGraph({})", iterator_repr(self.iter()))
    }
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<PathFromGraph<'static, G, GH>> for PyPathFromGraph
{
    fn from(value: PathFromGraph<'static, G, GH>) -> Self {
        Self {
            path: PathFromGraph {
                base_graph: value.base_graph.into_dynamic(),
                graph: value.graph.into_dynamic(),
                op: value.op,
                nodes: value.nodes,
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

impl_nodeviewops!(
    PyPathFromNode,
    path,
    PathFromNode<'static, DynamicGraph, DynamicGraph>,
    "PathFromNode"
);
impl_iterable_mixin!(
    PyPathFromNode,
    path,
    Vec<NodeView<DynamicGraph>>,
    "list[Node]",
    "node"
);

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<PathFromNode<'static, G, GH>> for PyPathFromNode
{
    fn from(value: PathFromNode<'static, G, GH>) -> Self {
        Self {
            path: PathFromNode {
                graph: value.graph.clone().into_dynamic(),
                base_graph: value.base_graph.clone().into_dynamic(),
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
    fn node_type(&self) -> OptionArcStringIterable {
        let path = self.path.clone();
        (move || path.node_type()).into()
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
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Repr
    for PathFromNode<'graph, G, GH>
{
    fn repr(&self) -> String {
        format!("PathFromNode({})", iterator_repr(self.iter()))
    }
}
