//! Defines the `Node`, which represents a node in the graph.
//! A node is a node in the graph, and can have properties and edges.
//! It can also be used to navigate the graph.
use crate::{
    core::{
        entities::nodes::node_ref::{AsNodeRef, NodeRef},
        utils::errors::GraphError,
        Prop,
    },
    db::{
        api::{
            properties::Properties,
            state::{ops, LazyNodeState, NodeStateOps},
            view::{
                internal::{
                    CoreGraphOps, DynOrMutableGraph, DynamicGraph, IntoDynamic,
                    IntoDynamicOrMutable, MaterializedGraph,
                },
                *,
            },
        },
        graph::{
            node::NodeView,
            nodes::Nodes,
            path::{PathFromGraph, PathFromNode},
            views::property_filter::internal::*,
        },
    },
    python::{
        graph::{
            node::internal::OneHopFilter,
            properties::{PropertiesView, PyNestedPropsIterable},
        },
        types::{
            repr::StructReprBuilder,
            wrappers::{iterables::*, prop::PyPropertyFilter},
        },
        utils::{PyNodeRef, PyTime},
    },
    *,
};
use chrono::{DateTime, Utc};
use numpy::{IntoPyArray, Ix1, PyArray};
use pyo3::{
    exceptions::{PyIndexError, PyKeyError},
    prelude::*,
    pybacked::PyBackedStr,
    pyclass, pymethods,
    types::PyDict,
    IntoPyObjectExt, PyObject, PyResult, Python,
};
use python::{
    types::repr::{iterator_repr, Repr},
    utils::{
        export::{create_row, extract_properties, get_column_names_from_props},
        PyGenericIterator,
    },
};
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr, utils::hashing::calculate_hash};
use rayon::{iter::IntoParallelIterator, prelude::*};
use std::collections::HashMap;

/// A node (or node) in the graph.
#[pyclass(name = "Node", subclass, module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyNode {
    pub node: NodeView<DynamicGraph, DynamicGraph>,
}

impl_nodeviewops!(
    PyNode,
    node,
    NodeView<DynamicGraph>,
    "Node",
    "Edges",
    "PathFromNode"
);
impl_edge_property_filter_ops!(PyNode<NodeView<DynamicGraph, DynamicGraph>>, node, "Node");

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
impl AsNodeRef for PyNode {
    fn as_node_ref(&self) -> NodeRef {
        self.node.as_node_ref()
    }
}

/// Defines the `Node`, which represents a node in the graph.
/// A node is a node in the graph, and can have properties and edges.
/// It can also be used to navigate the graph.
#[pymethods]
impl PyNode {
    /// checks if a node is equal to another by their id (ids are unique)
    ///
    /// Arguments:
    ///    other: The other node to compare to.
    ///
    /// Returns:
    ///   True if the nodes are equal, false otherwise.
    fn __eq__(&self, other: Bound<PyNode>) -> bool {
        self.node.id() == other.get().node.id()
    }

    fn __ne__(&self, other: Bound<PyNode>) -> bool {
        self.node.id() != other.get().node.id()
    }

    fn __lt__(&self, other: Bound<PyNode>) -> bool {
        self.node.id() < other.get().node.id()
    }

    fn __le__(&self, other: Bound<PyNode>) -> bool {
        self.node.id() <= other.get().node.id()
    }

    fn __gt__(&self, other: Bound<PyNode>) -> bool {
        self.node.id() > other.get().node.id()
    }

    fn __ge__(&self, other: Bound<PyNode>) -> bool {
        self.node.id() >= other.get().node.id()
    }

    /// Returns the hash of the node.
    ///
    /// Returns:
    ///   The node id.
    pub fn __hash__(&self) -> u64 {
        calculate_hash(&self.node.id())
    }

    /// Returns the id of the node.
    /// This is a unique identifier for the node.
    ///
    /// Returns:
    ///    (str|int): The id of the node.
    #[getter]
    pub fn id(&self) -> GID {
        self.node.id()
    }

    /// Returns the name of the node.
    ///
    /// Returns:
    ///     str: The id of the node as a string.
    #[getter]
    pub fn name(&self) -> String {
        self.node.name()
    }

    /// Returns the earliest time that the node exists.
    ///
    /// Returns:
    ///     int: The earliest time that the node exists as an integer.
    #[getter]
    pub fn earliest_time(&self) -> Option<i64> {
        self.node.earliest_time()
    }

    /// Returns the earliest datetime that the node exists.
    ///
    /// Returns:
    ///     datetime: The earliest datetime that the node exists as a Datetime.
    #[getter]
    pub fn earliest_date_time(&self) -> Option<DateTime<Utc>> {
        self.node.earliest_date_time()
    }

    /// Returns the latest time that the node exists.
    ///
    /// Returns:
    ///    int:  The latest time that the node exists as an integer.
    #[getter]
    pub fn latest_time(&self) -> Option<i64> {
        self.node.latest_time()
    }

    /// Returns the latest datetime that the node exists.
    ///
    /// Returns:
    ///     datetime: The latest datetime that the node exists as a Datetime.
    #[getter]
    pub fn latest_date_time(&self) -> Option<DateTime<Utc>> {
        self.node.latest_date_time()
    }

    /// The properties of the node
    ///
    /// Returns:
    ///     Properties: A list of properties.
    #[getter]
    pub fn properties(&self) -> Properties<NodeView<DynamicGraph, DynamicGraph>> {
        self.node.properties()
    }

    /// Returns the type of node
    ///
    /// Returns:
    ///     Optional[str]: The node type if it is set or `None` otherwise.
    #[getter]
    pub fn node_type(&self) -> Option<ArcStr> {
        self.node.node_type()
    }

    /// Get the degree of this node (i.e., the number of edges that are incident to it).
    ///
    /// Returns:
    ///     int: The degree of this node.
    pub fn degree(&self) -> usize {
        self.node.degree()
    }

    /// Get the in-degree of this node (i.e., the number of edges that are incident to it from other nodes).
    ///
    /// Returns:
    ///    int: The in-degree of this node.
    pub fn in_degree(&self) -> usize {
        self.node.in_degree()
    }

    /// Get the out-degree of this node (i.e., the number of edges that are incident to it from this node).
    ///
    /// Returns:
    ///   int: The out-degree of this node.
    pub fn out_degree(&self) -> usize {
        self.node.out_degree()
    }

    /// Returns the history of a node, including node additions and changes made to node.
    ///
    /// Returns:
    ///     List[int]: A list of unix timestamps of the event history of node.
    pub fn history<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let history = self.node.history();
        history.into_pyarray(py)
    }

    /// Returns the history of a node, including node additions and changes made to node.
    ///
    /// Returns:
    ///     List[datetime]: A list of timestamps of the event history of node.
    ///
    pub fn history_date_time(&self) -> Option<Vec<DateTime<Utc>>> {
        self.node.history_date_time()
    }

    /// Check if the node is active, i.e., it's history is not empty
    ///
    /// Returns:
    ///     bool:
    pub fn is_active(&self) -> bool {
        self.node.is_active()
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
        let repr_struc = StructReprBuilder::new("Node")
            .add_field("name", self.name())
            .add_field("earliest_time", self.earliest_time())
            .add_field("latest_time", self.latest_time());

        match self.node_type() {
            None => {
                if self.properties().is_empty() {
                    repr_struc.finish()
                } else {
                    repr_struc
                        .add_field("properties", self.properties())
                        .finish()
                }
            }
            Some(node_type) => {
                if self.properties().is_empty() {
                    repr_struc.add_field("node_type", node_type).finish()
                } else {
                    repr_struc
                        .add_field("properties", self.properties())
                        .add_field("node_type", node_type)
                        .finish()
                }
            }
        }
    }
}

#[pyclass(name = "MutableNode", extends = PyNode, module="raphtory", frozen)]
pub struct PyMutableNode {
    node: NodeView<MaterializedGraph, MaterializedGraph>,
}

impl PyMutableNode {
    fn new_bound<G: StaticGraphViewOps + IntoDynamic + Into<MaterializedGraph>>(
        node: NodeView<G>,
        py: Python,
    ) -> PyResult<Bound<PyMutableNode>> {
        Bound::new(py, (PyMutableNode::from(node.clone()), PyNode::from(node)))
    }
}

impl Repr for PyMutableNode {
    fn repr(&self) -> String {
        self.node.repr()
    }
}
impl<
        'py,
        G: StaticGraphViewOps + IntoDynamicOrMutable,
        GH: StaticGraphViewOps + IntoDynamicOrMutable,
    > IntoPyObject<'py> for NodeView<G, GH>
{
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let graph = self.graph.into_dynamic_or_mutable();
        match graph {
            DynOrMutableGraph::Dyn(graph) => {
                let base_graph = self.base_graph.into_dynamic();
                PyNode::from(NodeView::new_one_hop_filtered(base_graph, graph, self.node))
                    .into_bound_py_any(py)
            }
            DynOrMutableGraph::Mutable(graph) => {
                let base_graph = self.base_graph.into_dynamic_or_mutable();
                match base_graph {
                    DynOrMutableGraph::Dyn(_) => {
                        unreachable!()
                    }
                    DynOrMutableGraph::Mutable(base_graph) => PyMutableNode::new_bound(
                        NodeView::new_one_hop_filtered(base_graph, graph, self.node),
                        py,
                    )?
                    .into_bound_py_any(py),
                }
            }
        }
    }
}

impl<G: Into<MaterializedGraph>> From<NodeView<G>> for PyMutableNode {
    fn from(value: NodeView<G>) -> Self {
        let graph = value.graph.into();
        let node = NodeView::new_internal(graph, value.node);
        PyMutableNode { node }
    }
}

#[pymethods]
impl PyMutableNode {
    /// Set the type on the node. This only works if the type has not been previously set, otherwise will
    /// throw an error
    ///
    /// Parameters:
    ///     new_type (str): The new type to be set
    pub fn set_node_type(&self, new_type: &str) -> Result<(), GraphError> {
        self.node.set_node_type(new_type)
    }

    /// Add updates to a node in the graph at a specified time.
    /// This function allows for the addition of property updates to a node within the graph. The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Parameters:
    ///    t (TimeInput): The timestamp at which the updates should be applied.
    ///    properties (PropInput, optional): A dictionary of properties to update. Each key is a
    ///                                      string representing the property name, and each value
    ///                                      is of type Prop representing the property value.
    ///                                      If None, no properties are updated.
    ///    secondary_index (int, optional): The optional integer which will be used as a secondary index
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (t, properties=None, secondary_index=None))]
    pub fn add_updates(
        &self,
        t: PyTime,
        properties: Option<HashMap<String, Prop>>,
        secondary_index: Option<usize>,
    ) -> Result<(), GraphError> {
        match secondary_index {
            None => self.node.add_updates(t, properties.unwrap_or_default()),
            Some(secondary_index) => self
                .node
                .add_updates((t, secondary_index), properties.unwrap_or_default()),
        }
    }

    /// Add constant properties to a node in the graph.
    /// This function is used to add properties to a node that remain constant and do not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Parameters:
    ///     properties (PropInput): A dictionary of properties to be added to the node. Each key is a string representing the property name, and each value is of type Prop representing the property value.
    pub fn add_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        self.node.add_constant_properties(properties)
    }

    /// Update constant properties of a node in the graph overwriting existing values.
    /// This function is used to add properties to a node that remain constant and do not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Parameters:
    ///     properties (PropInput): A dictionary of properties to be added to the node. Each key is a string representing the property name, and each value is of type Prop representing the property value.
    pub fn update_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        self.node.update_constant_properties(properties)
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
#[pyclass(name = "Nodes", module = "raphtory", frozen)]
pub struct PyNodes {
    pub(crate) nodes: Nodes<'static, DynamicGraph, DynamicGraph>,
}

impl_nodeviewops!(
    PyNodes,
    nodes,
    Nodes<'static, DynamicGraph, DynamicGraph>,
    "Nodes",
    "NestedEdges",
    "PathFromGraph"
);
impl_edge_property_filter_ops!(
    PyNodes<Nodes<'static, DynamicGraph, DynamicGraph>>,
    nodes,
    "Nodes"
);

#[pymethods]
impl PyNodes {
    fn __len__(&self) -> usize {
        self.nodes.len()
    }
    fn __bool__(&self) -> bool {
        !self.nodes.is_empty()
    }
    fn __iter__(&self) -> PyGenericIterator {
        self.nodes.iter_owned().into()
    }
    #[doc = concat!(" Collect all ","node","s into a list")]
    #[doc = r""]
    #[doc = r" Returns:"]
    #[doc = concat!("     ","list[Node]",": the list of ","node","s")]
    fn collect(&self) -> Vec<NodeView<DynamicGraph>> {
        self.nodes.collect()
    }
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<Nodes<'static, G, GH>> for PyNodes
{
    fn from(value: Nodes<'static, G, GH>) -> Self {
        let graph = value.graph.into_dynamic();
        let base_graph = value.base_graph.into_dynamic();
        Self {
            nodes: Nodes::new_filtered(base_graph, graph, value.nodes, value.node_types_filter),
        }
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    IntoPyObject<'py> for Nodes<'static, G, GH>
{
    type Target = PyNodes;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyNodes::from(self).into_pyobject(py)
    }
}

/// Operations on a list of nodes.
/// These use all the same functions as a normal node except it returns a list of results.
#[pymethods]
impl PyNodes {
    /// checks if a list of nodes is equal to another list by their idd (ids are unique)
    ///
    /// Arguments:
    ///    other (Nodes): The other nodes to compare to.
    ///
    /// Returns:
    ///   bool: True if the nodes are equal, false otherwise.
    fn __eq__(&self, other: &PyNodes) -> bool {
        for (v1, v2) in self.nodes.iter().zip(other.nodes.iter()) {
            if v1.id() != v2.id() {
                return false;
            }
        }
        true
    }

    /// The node ids
    ///
    /// Returns:
    ///     IdView: a view of the node ids
    #[getter]
    fn id(&self) -> LazyNodeState<'static, ops::Id, DynamicGraph, DynamicGraph> {
        self.nodes.id()
    }

    /// The node names
    ///
    /// Returns:
    ///     NameView: a view of the node names
    #[getter]
    fn name(&self) -> LazyNodeState<'static, ops::Name, DynamicGraph, DynamicGraph> {
        self.nodes.name()
    }

    /// The earliest times nodes are active
    ///
    /// Returns:
    ///     EarliestTimeView: a view of the earliest active times
    #[getter]
    fn earliest_time(
        &self,
    ) -> LazyNodeState<'static, ops::EarliestTime<DynamicGraph>, DynamicGraph, DynamicGraph> {
        self.nodes.earliest_time()
    }

    /// The earliest time nodes are active as datetime objects
    ///
    /// Returns:
    ///     EarliestDateTimeView: a view of the earliest active times.
    #[getter]
    fn earliest_date_time(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<ops::EarliestTime<DynamicGraph>, Option<DateTime<Utc>>>,
        DynamicGraph,
    > {
        self.nodes.earliest_date_time()
    }

    /// The latest time nodes are active
    ///
    /// Returns:
    ///     LatestTimeView: a view of the latest active times
    #[getter]
    fn latest_time(&self) -> LazyNodeState<'static, ops::LatestTime<DynamicGraph>, DynamicGraph> {
        self.nodes.latest_time()
    }

    /// The latest time nodes are active as datetime objects
    ///
    /// Returns:
    ///   LatestDateTimeView: a view of the latest active times
    #[getter]
    fn latest_date_time(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<ops::LatestTime<DynamicGraph>, Option<DateTime<Utc>>>,
        DynamicGraph,
    > {
        self.nodes.latest_date_time()
    }

    /// Returns all timestamps of nodes, when a node is added or change to a node is made.
    ///
    /// Returns:
    ///    HistoryView: a view of the node histories
    ///
    fn history(&self) -> LazyNodeState<'static, ops::History<DynamicGraph>, DynamicGraph> {
        self.nodes.history()
    }

    /// The node types
    ///
    /// Returns:
    ///     NodeTypeView: a view of the node types
    #[getter]
    fn node_type(&self) -> LazyNodeState<'static, ops::Type, DynamicGraph> {
        self.nodes.node_type()
    }

    /// Returns all timestamps of nodes, when a node is added or change to a node is made.
    ///
    /// Returns:
    ///    HistoryDateTimeView: a view of the node histories as datetime objects.
    ///
    fn history_date_time(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<ops::History<DynamicGraph>, Option<Vec<DateTime<Utc>>>>,
        DynamicGraph,
    > {
        self.nodes.history_date_time()
    }

    /// The properties of the node
    ///
    /// Returns:
    ///     PropertiesView: A view of the node properties
    #[getter]
    fn properties(&self) -> PropertiesView {
        let nodes = self.nodes.clone();
        (move || nodes.properties().into_iter_values()).into()
    }

    /// Returns the number of edges of the nodes
    ///
    /// Returns:
    ///     DegreeView: a view of the undirected node degrees
    fn degree(&self) -> LazyNodeState<'static, ops::Degree<DynamicGraph>, DynamicGraph> {
        self.nodes.degree()
    }

    /// Returns the number of in edges of the nodes
    ///
    /// Returns:
    ///     DegreeView: a view of the in-degrees of the nodes
    fn in_degree(&self) -> LazyNodeState<'static, ops::Degree<DynamicGraph>, DynamicGraph> {
        self.nodes.in_degree()
    }

    /// Returns the number of out edges of the nodes
    ///
    /// Returns:
    ///     DegreeView: a view of the out-degrees of the nodes
    fn out_degree(&self) -> LazyNodeState<'static, ops::Degree<DynamicGraph>, DynamicGraph> {
        self.nodes.out_degree()
    }

    pub fn __getitem__(&self, node: PyNodeRef) -> PyResult<NodeView<DynamicGraph, DynamicGraph>> {
        self.nodes
            .get(node)
            .ok_or_else(|| PyIndexError::new_err("Node does not exist"))
    }

    /// Converts the graph's nodes into a Pandas DataFrame.
    ///
    /// This method will create a DataFrame with the following columns:
    /// - "name": The name of the node.
    /// - "properties": The properties of the node.
    /// - "update_history": The update history of the node.
    ///
    /// Args:
    ///     include_property_history (bool): A boolean, if set to `True`, the history of each property is included, if `False`, only the latest value is shown. Defaults to False.
    ///     convert_datetime (bool): A boolean, if set to `True` will convert the timestamp to python datetimes. Defaults to False.
    ///
    /// Returns:
    ///     DataFrame: the view of the node data as a pandas Dataframe
    #[pyo3(signature = (include_property_history = false, convert_datetime = false))]
    pub fn to_df(
        &self,
        include_property_history: bool,
        convert_datetime: bool,
    ) -> PyResult<PyObject> {
        let mut column_names = vec![String::from("name"), String::from("type")];
        let meta = self.nodes.graph.node_meta();
        let is_prop_both_temp_and_const = get_column_names_from_props(&mut column_names, meta);

        let node_tuples: Vec<_> = self
            .nodes
            .collect()
            .into_par_iter()
            .flat_map(|item| {
                let mut properties_map: HashMap<String, Prop> = HashMap::new();
                let mut prop_time_dict: HashMap<i64, HashMap<String, Prop>> = HashMap::new();
                extract_properties(
                    include_property_history,
                    convert_datetime,
                    false,
                    &column_names,
                    &is_prop_both_temp_and_const,
                    &item.properties(),
                    &mut properties_map,
                    &mut prop_time_dict,
                    item.start().unwrap_or(0),
                );

                let row_header: Vec<Prop> = vec![
                    Prop::from(item.name()),
                    Prop::from(item.node_type().unwrap_or_else(|| ArcStr::from(""))),
                ];

                let start_point = 2;
                let history = item.history();

                create_row(
                    convert_datetime,
                    false,
                    &column_names,
                    properties_map,
                    prop_time_dict,
                    row_header,
                    start_point,
                    history,
                )
            })
            .collect();

        Python::with_gil(|py| {
            let kwargs = PyDict::new(py);
            kwargs.set_item("columns", column_names.clone())?;
            let pandas = PyModule::import(py, "pandas")?;
            let df_data = pandas.call_method("DataFrame", (node_tuples,), Some(&kwargs))?;
            Ok(df_data.unbind())
        })
    }

    /// Filter nodes by node type
    ///
    /// Arguments:
    ///     node_types (list[str]): the list of node types to keep
    ///
    /// Returns:
    ///     Nodes: the filtered view of the nodes
    pub fn type_filter(&self, node_types: Vec<PyBackedStr>) -> Nodes<'static, DynamicGraph> {
        self.nodes.type_filter(&node_types)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Repr for Nodes<'static, G, GH> {
    fn repr(&self) -> String {
        format!("Nodes({})", iterator_repr(self.iter()))
    }
}

#[pyclass(name = "PathFromGraph", module = "raphtory")]
pub struct PyPathFromGraph {
    path: PathFromGraph<'static, DynamicGraph, DynamicGraph>,
}

impl_nodeviewops!(
    PyPathFromGraph,
    path,
    PathFromGraph<'static, DynamicGraph, DynamicGraph>,
    "PathFromGraph",
    "NestedEdges",
    "PathFromGraph"
);
impl_iterable_mixin!(
    PyPathFromGraph,
    path,
    Vec<Vec<NodeView<DynamicGraph>>>,
    "list[list[Node]]",
    "node"
);
impl_edge_property_filter_ops!(
    PyPathFromGraph<PathFromGraph<'static, DynamicGraph, DynamicGraph>>,
    path,
    "PathFromGraph"
);

#[pymethods]
impl PyPathFromGraph {
    /// the node ids
    #[getter]
    fn id(&self) -> NestedGIDIterable {
        let path = self.path.clone();
        (move || path.id()).into()
    }

    /// the node names
    #[getter]
    fn name(&self) -> NestedStringIterable {
        let path = self.path.clone();
        (move || path.name()).into()
    }

    /// the node types
    #[getter]
    fn node_type(&self) -> NestedOptionArcStringIterable {
        let path = self.path.clone();
        (move || path.node_type()).into()
    }

    /// the node earliest times
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

    /// the node latest times
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

    /// the node properties
    #[getter]
    fn properties(&self) -> PyNestedPropsIterable {
        let path = self.path.clone();
        (move || path.properties()).into()
    }

    /// the node degrees
    fn degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.degree()).into()
    }

    /// the node in-degrees
    fn in_degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.in_degree()).into()
    }

    /// the node out-degrees
    fn out_degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.out_degree()).into()
    }

    /// filter nodes by type
    ///
    /// Arguments:
    ///     node_types (list[str]): the node types to keep
    ///
    /// Returns:
    ///     PathFromGraph: the filtered view
    pub fn type_filter(
        &self,
        node_types: Vec<PyBackedStr>,
    ) -> PathFromGraph<'static, DynamicGraph, DynamicGraph> {
        self.path.type_filter(&node_types)
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

impl<'py, G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    IntoPyObject<'py> for PathFromGraph<'static, G, GH>
{
    type Target = PyPathFromGraph;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyPathFromGraph::from(self).into_pyobject(py)
    }
}

#[pyclass(name = "PathFromNode", module = "raphtory")]
pub struct PyPathFromNode {
    path: PathFromNode<'static, DynamicGraph, DynamicGraph>,
}

impl_nodeviewops!(
    PyPathFromNode,
    path,
    PathFromNode<'static, DynamicGraph, DynamicGraph>,
    "PathFromNode",
    "Edges",
    "PathFromNode"
);
impl_iterable_mixin!(
    PyPathFromNode,
    path,
    Vec<NodeView<DynamicGraph>>,
    "list[Node]",
    "node"
);
impl_edge_property_filter_ops!(
    PyPathFromNode<PathFromNode<'static, DynamicGraph, DynamicGraph>>,
    path,
    "PathFromNode"
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

impl<'py, G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    IntoPyObject<'py> for PathFromNode<'static, G, GH>
{
    type Target = PyPathFromNode;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyPathFromNode::from(self).into_pyobject(py)
    }
}

#[pymethods]
impl PyPathFromNode {
    /// the node ids
    #[getter]
    fn id(&self) -> GIDIterable {
        let path = self.path.clone();
        (move || path.id()).into()
    }

    /// the node names
    #[getter]
    fn name(&self) -> StringIterable {
        let path = self.path.clone();
        (move || path.name()).into()
    }

    /// the node types
    #[getter]
    fn node_type(&self) -> OptionArcStringIterable {
        let path = self.path.clone();
        (move || path.node_type()).into()
    }

    /// the node earliest times
    #[getter]
    fn earliest_time(&self) -> OptionI64Iterable {
        let path = self.path.clone();
        (move || path.earliest_time()).into()
    }

    /// the node latest times
    #[getter]
    fn latest_time(&self) -> OptionI64Iterable {
        let path = self.path.clone();
        (move || path.latest_time()).into()
    }

    /// the node properties
    #[getter]
    fn properties(&self) -> PropertiesView {
        let path = self.path.clone();
        (move || path.properties()).into()
    }

    /// the node in-degrees
    fn in_degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.in_degree()).into()
    }

    /// the node out-degrees
    fn out_degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.out_degree()).into()
    }

    /// the node degrees
    fn degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.degree()).into()
    }

    /// filter nodes by type
    ///
    /// Arguments:
    ///     node_types (list[str]): the node types to keep
    ///
    /// Returns:
    ///     PathFromNode: the filtered view
    pub fn type_filter(
        &self,
        node_types: Vec<PyBackedStr>,
    ) -> PathFromNode<'static, DynamicGraph, DynamicGraph> {
        self.path.type_filter(&node_types)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Repr
    for PathFromNode<'graph, G, GH>
{
    fn repr(&self) -> String {
        format!("PathFromNode({})", iterator_repr(self.iter()))
    }
}
