//! Defines the `Node`, which represents a node in the graph.
//! A node is a node in the graph, and can have properties and edges.
//! It can also be used to navigate the graph.
use crate::{
    core::entities::nodes::node_ref::{AsNodeRef, NodeRef},
    db::{
        api::{
            properties::{Metadata, Properties},
            state::{
                ops,
                ops::{
                    filter::{AndOp, NodeTypeFilterOp, NO_FILTER},
                    node::Degree,
                    DynNodeFilter, IntoDynNodeOp, NodeFilterOp,
                },
                LazyNodeState, NodeStateOps,
            },
            view::{
                filter_ops::Select,
                history::History,
                internal::{
                    DynOrMutableGraph, DynamicGraph, IntoDynHop, IntoDynamic, IntoDynamicOrMutable,
                    MaterializedGraph,
                },
                *,
            },
        },
        graph::{
            node::NodeView,
            nodes::Nodes,
            path::{PathFromGraph, PathFromNode},
        },
    },
    errors::GraphError,
    export::{node_metadata, node_updates},
    prelude::PropertiesOps,
    python::{
        filter::filter_expr::PyFilterExpr,
        graph::{
            history::{NestedHistoryIterable, PyHistory},
            properties::{MetadataListList, MetadataView, PropertiesView, PyNestedPropsIterable},
            sorting::PyNodeSortBy,
        },
        types::{iterable::FromIterable, repr::StructReprBuilder, wrappers::iterables::*},
        utils::PyNodeRef,
    },
    *,
};
use pyo3::{
    exceptions::PyTypeError, prelude::*, pybacked::PyBackedStr, pyclass, pymethods, types::PyDict,
    Borrowed, IntoPyObjectExt, PyAny, PyResult, Python,
};
use pyo3_arrow::PyTable;
use python::{
    types::repr::{iterator_repr, Repr},
    utils::PyGenericIterator,
};
use raphtory_api::{
    core::{
        entities::{properties::prop::Prop, GID},
        storage::arc_str::ArcStr,
        utils::hashing::calculate_hash,
    },
    python::timeindex::{EventTimeComponent, PyOptionalEventTime},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// A node (or node) in the graph.
#[pyclass(name = "Node", subclass, module = "raphtory", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyNode {
    pub node: NodeView<'static, DynamicGraph>,
}

impl_nodeviewops!(
    PyNode,
    node,
    NodeView<'static, DynamicGraph>,
    "Node",
    "Edges",
    "PathFromNode"
);

impl<G: StaticGraphViewOps + IntoDynamic> From<NodeView<'static, G>> for PyNode {
    fn from(value: NodeView<'static, G>) -> Self {
        let graph = value.graph.into_dynamic();
        let node = NodeView::new_internal(graph, value.node);
        Self { node }
    }
}

/// Converts a python node into a rust node.
impl AsNodeRef for PyNode {
    fn as_node_ref(&self) -> NodeRef<'_> {
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
    ///   True if the nodes are equal, false otherwise.:
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
    ///   The node id.:
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
    ///     OptionalEventTime: The earliest time that the node exists.
    #[getter]
    pub fn earliest_time(&self) -> PyOptionalEventTime {
        self.node.earliest_time().into()
    }

    /// Returns the latest time that the node exists.
    ///
    /// Returns:
    ///    OptionalEventTime: The latest time that the node exists.
    #[getter]
    pub fn latest_time(&self) -> PyOptionalEventTime {
        self.node.latest_time().into()
    }

    /// The properties of the node
    ///
    /// Returns:
    ///     Properties: A list of properties.
    #[getter]
    pub fn properties(&self) -> Properties<NodeView<'static, DynamicGraph>> {
        self.node.properties()
    }

    /// The metadata of the node
    ///
    /// Returns:
    ///     Metadata:
    #[getter]
    pub fn metadata(&self) -> Metadata<'static, NodeView<'static, DynamicGraph>> {
        self.node.metadata()
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
    ///     History: A History object for the node, providing access to time entries.
    #[getter]
    pub fn history(&self) -> PyHistory {
        PyHistory::new(History::new(Arc::new(self.node.clone())))
    }

    /// Get the number of edge events for this node
    ///
    /// Returns:
    ///     int: The number of edge events
    pub fn edge_history_count(&self) -> usize {
        self.node.edge_history_count()
    }

    /// Check if the node is active (it's history is not empty).
    ///
    /// Returns:
    ///     bool:
    pub fn is_active(&self) -> bool {
        self.node.is_active()
    }
}

impl Repr for PyNode {
    fn repr(&self) -> String {
        self.node.repr()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Repr for NodeView<'graph, G> {
    fn repr(&self) -> String {
        let repr_struct = StructReprBuilder::new("Node")
            .add_field("name", self.name())
            .add_field("earliest_time", self.earliest_time())
            .add_field("latest_time", self.latest_time());

        match self.node_type() {
            None => {
                if self.properties().is_empty() {
                    repr_struct.finish()
                } else {
                    repr_struct
                        .add_field("properties", self.properties())
                        .finish()
                }
            }
            Some(node_type) => {
                if self.properties().is_empty() {
                    repr_struct.add_field("node_type", node_type).finish()
                } else {
                    repr_struct
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
    node: NodeView<'static, MaterializedGraph>,
}

impl PyMutableNode {
    fn new_bound<'py, G: StaticGraphViewOps + IntoDynamic + Into<MaterializedGraph>>(
        node: NodeView<'static, G>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyMutableNode>> {
        Bound::new(py, (PyMutableNode::from(node.clone()), PyNode::from(node)))
    }
}

impl Repr for PyMutableNode {
    fn repr(&self) -> String {
        self.node.repr()
    }
}
impl<'py, G: StaticGraphViewOps + IntoDynamicOrMutable> IntoPyObject<'py> for NodeView<'static, G> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let graph = self.graph.into_dynamic_or_mutable();
        match graph {
            DynOrMutableGraph::Dyn(graph) => {
                PyNode::from(NodeView::new_internal(graph, self.node)).into_bound_py_any(py)
            }
            DynOrMutableGraph::Mutable(graph) => {
                PyMutableNode::new_bound(NodeView::new_internal(graph, self.node), py)?
                    .into_bound_py_any(py)
            }
        }
    }
}

impl<G: Into<MaterializedGraph>> From<NodeView<'static, G>> for PyMutableNode {
    fn from(value: NodeView<'static, G>) -> Self {
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
    /// Arguments:
    ///     new_type (str): The new type to be set
    ///
    /// Returns:
    ///     None:
    pub fn set_node_type(&self, new_type: &str) -> Result<(), GraphError> {
        self.node.set_node_type(new_type)
    }

    /// Add updates to a node in the graph at a specified time.
    /// This function allows for the addition of property updates to a node within the graph. The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Arguments:
    ///    t (TimeInput): The timestamp at which the updates should be applied.
    ///    properties (PropInput, optional): A dictionary of properties to update. Each key is a
    ///                                      string representing the property name, and each value
    ///                                      is of type Prop representing the property value.
    ///                                      If None, no properties are updated.
    ///    event_id (int, optional): The optional integer which will be used as an event id.
    ///    layer (str, optional): The layer this update is recorded under. Defaults to None.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (t, properties=None, event_id=None, layer=None))]
    pub fn add_updates(
        &self,
        t: EventTimeComponent,
        properties: Option<HashMap<String, Prop>>,
        event_id: Option<usize>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        match event_id {
            None => self
                .node
                .add_updates(t, properties.unwrap_or_default(), layer),
            Some(event_id) => {
                self.node
                    .add_updates((t, event_id), properties.unwrap_or_default(), layer)
            }
        }
    }

    /// Add metadata to a node in the graph.
    /// This function is used to add properties to a node that do not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Arguments:
    ///     metadata (PropInput): A dictionary of properties to be added to the node. Each key is a string representing the property name, and each value is of type Prop representing the property value.
    ///
    /// Returns:
    ///     None:
    pub fn add_metadata(&self, metadata: HashMap<String, Prop>) -> Result<(), GraphError> {
        self.node.add_metadata(metadata)
    }

    /// Update metadata of a node in the graph overwriting existing values.
    /// This function is used to add properties to a node that do not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Arguments:
    ///     metadata (PropInput): A dictionary of properties to be added to the node. Each key is a string representing the property name, and each value is of type Prop representing the property value.
    ///
    /// Returns:
    ///     None:
    pub fn update_metadata(&self, metadata: HashMap<String, Prop>) -> Result<(), GraphError> {
        self.node.update_metadata(metadata)
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
#[derive(Clone)]
#[pyclass(name = "Nodes", module = "raphtory", frozen, from_py_object)]
pub struct PyNodes {
    pub(crate) nodes: Nodes<'static, DynamicGraph, DynamicGraph, DynNodeFilter>,
}

impl PyNodes {
    /// The node update table and the node metadata table of these nodes as pyarrow Tables.
    fn node_tables<'py>(
        &self,
        py: Python<'py>,
        include_property_history: bool,
        convert_datetime: bool,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let (updates, metadata) = py.detach(|| {
            let updates = node_updates(&self.nodes, !include_property_history, convert_datetime)?;
            Ok::<_, GraphError>((updates, node_metadata(&self.nodes)?))
        })?;
        Ok((
            PyTable::try_new(updates.batches, updates.schema)?.into_pyarrow(py)?,
            PyTable::try_new(metadata.batches, metadata.schema)?.into_pyarrow(py)?,
        ))
    }
}

impl<'py> FromPyObject<'_, 'py> for Nodes<'static, DynamicGraph, DynamicGraph, DynNodeFilter> {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        Ok(ob.cast::<PyNodes>()?.get().nodes.clone())
    }
}

impl<'py> FromPyObject<'_, 'py> for Nodes<'static, DynamicGraph> {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let bound = ob.cast::<PyNodes>()?;
        let nodes = &bound.get().nodes;
        if nodes.predicate.is_filtered() {
            Err(PyTypeError::new_err("Expected unfiltered nodes"))
        } else {
            Ok(Nodes::new_filtered(
                nodes.base_graph.clone(),
                nodes.graph.clone(),
                NO_FILTER,
                nodes.nodes.clone(),
            ))
        }
    }
}

impl_nodeviewops!(
    PyNodes,
    nodes,
    Nodes<'static, DynamicGraph, DynamicGraph, DynNodeFilter>,
    "Nodes",
    "NestedEdges",
    "PathFromGraph"
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
    fn collect(&self) -> Vec<NodeView<'static, DynamicGraph>> {
        self.nodes.collect()
    }

    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyNodes> {
        let r = self.nodes.select(filter)?;
        Ok(PyNodes::from(r))
    }

    /// Reorder this collection by an ordered list of sort keys. Multi-key
    /// sort is lexicographic (ties on key 1 break to key 2).
    ///
    /// Arguments:
    ///     sort_bys (list[NodeSortBy]): the ordered sort keys.
    ///
    /// Returns:
    ///     Nodes: a new collection in the sorted order.
    fn sorted(&self, sort_bys: Vec<PyNodeSortBy>) -> PyNodes {
        let sort_bys: Vec<_> = sort_bys.into_iter().map(|s| s.inner).collect();
        PyNodes::from(self.nodes.sorted(&sort_bys))
    }
}

impl<
        G: StaticGraphViewOps + IntoDynamic,
        GH: StaticGraphViewOps + IntoDynamic,
        F: IntoDynNodeOp<Output = bool>,
    > From<Nodes<'static, G, GH, F>> for PyNodes
{
    fn from(value: Nodes<'static, G, GH, F>) -> Self {
        let base_graph = value.base_graph.into_dynamic();
        let graph = value.graph.into_dynamic();
        let select = value.predicate.into_dynamic();
        Self {
            nodes: Nodes::new_filtered(base_graph, graph, select, value.nodes),
        }
    }
}

impl<
        'py,
        G: StaticGraphViewOps + IntoDynamic,
        GH: StaticGraphViewOps + IntoDynamic,
        F: IntoDynNodeOp<Output = bool>,
    > IntoPyObject<'py> for Nodes<'static, G, GH, F>
{
    type Target = PyNodes;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyNodes::from(self).into_pyobject(py)
    }
}

#[derive(FromPyObject)]
pub enum PyNodesCmp {
    Set(HashSet<PyNodeRef>),
    Iter(FromIterable<PyNodeRef>),
    Nodes(PyNodes),
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
    fn __eq__(&self, other: PyNodesCmp) -> bool {
        match other {
            PyNodesCmp::Iter(nodes) => {
                nodes.len() == self.nodes.len()
                    && self
                        .nodes
                        .iter()
                        .eq(nodes.iter().flat_map(|node_ref| self.nodes.get(node_ref)))
            }
            PyNodesCmp::Nodes(nodes) => self.nodes == nodes.nodes,
            PyNodesCmp::Set(nodes) => {
                nodes.len() == self.nodes.len()
                    && nodes.iter().all(|node_ref| self.nodes.contains(node_ref))
            }
        }
    }

    /// The node ids
    ///
    /// Returns:
    ///     IdView: a view of the node ids
    #[getter]
    fn id(&self) -> LazyNodeState<'static, ops::Id, DynamicGraph, DynamicGraph, DynNodeFilter> {
        self.nodes.id()
    }

    /// The node names
    ///
    /// Returns:
    ///     NameView: a view of the node names
    #[getter]
    fn name(&self) -> LazyNodeState<'static, ops::Name, DynamicGraph, DynamicGraph, DynNodeFilter> {
        self.nodes.name()
    }

    /// The earliest times nodes are active
    ///
    /// Returns:
    ///     EarliestTimeView: a view of the earliest active times
    #[getter]
    fn earliest_time(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::EarliestTime<DynamicGraph>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        self.nodes.earliest_time()
    }

    /// The latest time nodes are active
    ///
    /// Returns:
    ///     LatestTimeView: a view of the latest active times
    #[getter]
    fn latest_time(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::LatestTime<DynamicGraph>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        self.nodes.latest_time()
    }

    /// Returns all history objects of nodes, with information on when a node is added or change to a node is made.
    ///
    /// Returns:
    ///    HistoryView: a view of the node histories
    #[getter]
    fn history(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::HistoryOp<'static, DynamicGraph>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        self.nodes.history()
    }

    /// Return the number of edge updates for each node
    ///
    /// Returns:
    ///     EdgeHistoryCountView: a view of the edge history counts
    fn edge_history_count(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::EdgeHistoryCount<DynamicGraph>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        self.nodes.edge_history_count()
    }

    /// The node types
    ///
    /// Returns:
    ///     NodeTypeView: a view of the node types
    #[getter]
    fn node_type(
        &self,
    ) -> LazyNodeState<'static, ops::Type, DynamicGraph, DynamicGraph, DynNodeFilter> {
        self.nodes.node_type()
    }

    /// The properties of the node.
    ///
    /// Returns:
    ///     PropertiesView: A view of the node properties.
    #[getter]
    fn properties(&self) -> PropertiesView {
        let nodes = self.nodes.clone();
        (move || nodes.properties().into_iter_values()).into()
    }

    /// The metadata of the nodes.
    ///
    /// Returns:
    ///     MetadataView: A view of the node properties.
    #[getter]
    fn metadata(&self) -> MetadataView {
        let nodes = self.nodes.clone();
        (move || nodes.metadata().into_iter_values()).into()
    }

    /// Returns the number of edges of the nodes.
    ///
    /// Returns:
    ///     DegreeView: a view of the undirected node degrees.
    fn degree(
        &self,
    ) -> LazyNodeState<'static, Degree<DynamicGraph>, DynamicGraph, DynamicGraph, DynNodeFilter>
    {
        self.nodes.degree()
    }

    /// Returns the number of in edges of the nodes.
    ///
    /// Returns:
    ///     DegreeView: a view of the in-degrees of the nodes
    fn in_degree(
        &self,
    ) -> LazyNodeState<'static, Degree<DynamicGraph>, DynamicGraph, DynamicGraph, DynNodeFilter>
    {
        self.nodes.in_degree()
    }

    /// Returns the number of out edges of the nodes.
    ///
    /// Returns:
    ///     DegreeView: a view of the out-degrees of the nodes.
    fn out_degree(
        &self,
    ) -> LazyNodeState<'static, Degree<DynamicGraph>, DynamicGraph, DynamicGraph, DynNodeFilter>
    {
        self.nodes.out_degree()
    }

    /// Converts the nodes into two pandas DataFrames: their updates, with one row per update, and
    /// their metadata, with one row per node.
    ///
    /// The updates DataFrame has the following columns:
    /// - "rap_node_gid": the id of the node, an integer or a string depending on the graph.
    /// - "rap_node_type": the type of the node.
    /// - "rap_time": the time of the update, in milliseconds since the epoch.
    /// - "rap_secondary_index": the event id of the update, which orders updates made at the same time.
    /// - "rap_layer": the node layer of the update, empty for the default layer.
    ///
    /// These are followed by one column per temporal property. A cell is empty when the update did
    /// not set that property. By default, only the updates that hold the latest value of a property are included.
    ///
    /// The metadata DataFrame has the columns "rap_node_gid" and "rap_node_type", followed by one
    /// column per metadata key. A cell is empty when the node has no value for that key.
    ///
    /// Columns use pyarrow-backed dtypes, so integer columns with empty cells keep their type.
    ///
    /// Args:
    ///     include_property_history (bool): If True, include every update instead of only the updates that hold a property's latest value. Defaults to False.
    ///     convert_datetime (bool): If True, the "rap_time" column holds UTC datetimes instead of milliseconds. Defaults to False.
    ///
    /// Returns:
    ///     tuple[DataFrame, DataFrame]: the node updates and the node metadata as pandas DataFrames.
    #[pyo3(signature = (include_property_history = false, convert_datetime = false))]
    pub fn to_df<'py>(
        &self,
        py: Python<'py>,
        include_property_history: bool,
        convert_datetime: bool,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let (updates, metadata) =
            self.node_tables(py, include_property_history, convert_datetime)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("types_mapper", py.import("pandas")?.getattr("ArrowDtype")?)?;
        Ok((
            updates.call_method("to_pandas", (), Some(&kwargs))?,
            metadata.call_method("to_pandas", (), Some(&kwargs))?,
        ))
    }

    /// Converts the nodes into two pyarrow Tables: their updates, with one row per update, and
    /// their metadata, with one row per node.
    ///
    /// The updates Table has the following columns:
    /// - "rap_node_gid": the id of the node, an integer or a string depending on the graph.
    /// - "rap_node_type": the type of the node.
    /// - "rap_time": the time of the update, in milliseconds since the epoch.
    /// - "rap_secondary_index": the event id of the update, which orders updates made at the same time.
    /// - "rap_layer": the node layer of the update, empty for the default layer.
    ///
    /// These are followed by one column per temporal property. A cell is empty when the update did
    /// not set that property. By default, only the updates that hold the latest value of a property are included.
    ///
    /// The metadata Table has the columns "rap_node_gid" and "rap_node_type", followed by one
    /// column per metadata key. A cell is empty when the node has no value for that key.
    ///
    /// Args:
    ///     include_property_history (bool): If True, include every update instead of only the updates that hold a property's latest value. Defaults to False.
    ///     convert_datetime (bool): If True, the "rap_time" column holds UTC datetimes instead of milliseconds. Defaults to False.
    ///
    /// Returns:
    ///     tuple[pyarrow.Table, pyarrow.Table]: the node updates and the node metadata as pyarrow Tables.
    #[pyo3(signature = (include_property_history = false, convert_datetime = false))]
    pub fn to_arrow<'py>(
        &self,
        py: Python<'py>,
        include_property_history: bool,
        convert_datetime: bool,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        self.node_tables(py, include_property_history, convert_datetime)
    }

    /// Filter nodes by node type.
    ///
    /// Arguments:
    ///     node_types (list[str]): the list of node types to keep.
    ///
    /// Returns:
    ///     Nodes: the filtered view of the nodes
    pub fn type_filter(
        &self,
        node_types: Vec<String>,
    ) -> Nodes<'static, DynamicGraph, DynamicGraph, AndOp<DynNodeFilter, NodeTypeFilterOp>> {
        self.nodes.type_filter(&node_types)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, F: NodeFilterOp + 'graph> Repr
    for Nodes<'static, G, GH, F>
{
    fn repr(&self) -> String {
        format!("Nodes({})", iterator_repr(self.iter()))
    }
}

#[pyclass(name = "PathFromGraph", module = "raphtory")]
pub struct PyPathFromGraph {
    path: PathFromGraph<'static, DynamicGraph>,
}

impl_nodeviewops!(
    PyPathFromGraph,
    path,
    PathFromGraph<'static, DynamicGraph>,
    "PathFromGraph",
    "NestedEdges",
    "PathFromGraph"
);
impl_iterable_mixin!(
    PyPathFromGraph,
    path,
    Vec<Vec<NodeView<'static, DynamicGraph>>>,
    "list[list[Node]]",
    "node"
);

#[pymethods]
impl PyPathFromGraph {
    /// The node ids
    ///
    /// Returns:
    ///     NestedGIDIterable:
    #[getter]
    fn id(&self) -> NestedGIDIterable {
        let path = self.path.clone();
        (move || path.id().map(|(_, v)| v)).into()
    }

    /// The node names.
    ///
    /// Returns:
    ///     NestedStringIterable:
    #[getter]
    fn name(&self) -> NestedStringIterable {
        let path = self.path.clone();
        (move || path.name().map(|(_, v)| v)).into()
    }

    /// The node types.
    ///
    /// Returns:
    ///     NestedOptionArcStringIterable:
    #[getter]
    fn node_type(&self) -> NestedOptionArcStringIterable {
        let path = self.path.clone();
        (move || path.node_type().map(|(_, v)| v)).into()
    }

    /// The node earliest times.
    ///
    /// Returns:
    ///     NestedOptionEventTimeIterable:
    #[getter]
    fn earliest_time(&self) -> NestedOptionEventTimeIterable {
        let path = self.path.clone();
        (move || path.earliest_time().map(|(_, v)| v)).into()
    }

    /// The node latest times.
    ///
    /// Returns:
    ///     NestedOptionEventTimeIterable:
    #[getter]
    fn latest_time(&self) -> NestedOptionEventTimeIterable {
        let path = self.path.clone();
        (move || path.latest_time().map(|(_, v)| v)).into()
    }

    /// Returns a history object for each node with time entries for when a node is added or change to a node is made.
    ///
    /// Returns:
    ///     NestedHistoryIterable: A nested iterable of history objects, one for each node.
    #[getter]
    fn history(&self) -> NestedHistoryIterable {
        let path = self.path.clone();
        (move || {
            path.history()
                .map(|(_, h_iter)| h_iter.map(|h| h.into_arc_dyn()))
        })
        .into()
    }

    /// Returns a single history object containing time entries for all nodes in the path.
    ///
    /// Returns:
    ///     History: A history object with all time entries associated with the nodes.
    fn combined_history(&self) -> PyHistory {
        let path = self.path.clone();
        PyHistory::new(History::new(Arc::new(path)))
    }

    /// Returns the number of edge updates for each node.
    ///
    /// Returns:
    ///     NestedUsizeIterable:
    fn edge_history_count(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.edge_history_count().map(|(_, v)| v)).into()
    }

    /// Returns the node properties.
    ///
    /// Returns:
    ///     PyNestedPropsIterable:
    #[getter]
    fn properties(&self) -> PyNestedPropsIterable {
        let path = self.path.clone();
        (move || path.properties().map(|(_, v)| v)).into()
    }

    /// Returns the node metadata.
    ///
    /// Returns:
    ///     MetadataListList:
    #[getter]
    fn metadata(&self) -> MetadataListList {
        let path = self.path.clone();
        (move || path.metadata().map(|(_, v)| v)).into()
    }

    /// Returns the node degrees.
    ///
    /// Returns:
    ///     NestedUsizeIterable:
    fn degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.degree().map(|(_, v)| v)).into()
    }

    /// Returns the node in-degrees.
    ///
    /// Returns:
    ///     NestedUsizeIterable:
    fn in_degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.in_degree().map(|(_, v)| v)).into()
    }

    /// Returns the node out-degrees.
    ///
    /// Returns:
    ///     NestedUsizeIterable:
    fn out_degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.out_degree().map(|(_, v)| v)).into()
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
    ) -> PathFromGraph<'static, DynamicGraph> {
        self.path.type_filter(&node_types)
    }

    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyPathFromGraph> {
        let r = self.path.select(filter)?;
        Ok(PyPathFromGraph::from(r))
    }
}

impl<'graph, G: GraphViewOps<'graph>> Repr for PathFromGraph<'graph, G> {
    fn repr(&self) -> String {
        format!("PathFromGraph({})", iterator_repr(self.iter()))
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> From<PathFromGraph<'static, G>> for PyPathFromGraph {
    fn from(value: PathFromGraph<'static, G>) -> Self {
        let path = value.into_dyn_hop();
        Self { path }
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for PathFromGraph<'static, G> {
    type Target = PyPathFromGraph;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyPathFromGraph::from(self).into_pyobject(py)
    }
}

#[pyclass(name = "PathFromNode", module = "raphtory")]
pub struct PyPathFromNode {
    path: PathFromNode<'static, DynamicGraph>,
}

impl_nodeviewops!(
    PyPathFromNode,
    path,
    PathFromNode<'static, DynamicGraph>,
    "PathFromNode",
    "Edges",
    "PathFromNode"
);
impl_iterable_mixin!(
    PyPathFromNode,
    path,
    Vec<NodeView<'static, DynamicGraph>>,
    "list[Node]",
    "node"
);

impl<G: StaticGraphViewOps + IntoDynamic> From<PathFromNode<'static, G>> for PyPathFromNode {
    fn from(value: PathFromNode<'static, G>) -> Self {
        let path = value.into_dyn_hop();
        Self { path }
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for PathFromNode<'static, G> {
    type Target = PyPathFromNode;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyPathFromNode::from(self).into_pyobject(py)
    }
}

#[pymethods]
impl PyPathFromNode {
    /// The node IDs.
    ///
    /// Returns:
    ///     GIDIterable:
    #[getter]
    fn id(&self) -> GIDIterable {
        let path = self.path.clone();
        (move || path.id()).into()
    }

    /// The node names.
    ///
    /// Returns:
    ///     StringIterable:
    #[getter]
    fn name(&self) -> StringIterable {
        let path = self.path.clone();
        (move || path.name()).into()
    }

    /// The node types.
    ///
    /// Returns:
    ///     OptionArcStringIterable:
    #[getter]
    fn node_type(&self) -> OptionArcStringIterable {
        let path = self.path.clone();
        (move || path.node_type()).into()
    }

    /// Get the number of edge updates for each node.
    ///
    /// Returns:
    ///     UsizeIterable:
    fn edge_history_count(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.edge_history_count()).into()
    }

    /// The earliest time of each node.
    ///
    /// Returns:
    ///     OptionEventTimeIterable: An iterable of `EventTime`s.
    #[getter]
    fn earliest_time(&self) -> OptionEventTimeIterable {
        let path = self.path.clone();
        (move || path.earliest_time()).into()
    }

    /// The latest time of each node.
    ///
    /// Returns:
    ///     OptionEventTimeIterable: An iterable of `EventTime`s.
    #[getter]
    fn latest_time(&self) -> OptionEventTimeIterable {
        let path = self.path.clone();
        (move || path.latest_time()).into()
    }

    /// Returns a single history object containing time entries for all nodes in the path.
    ///
    /// Returns:
    ///     History: History object with all time entries for the nodes.
    fn combined_history(&self) -> PyHistory {
        let path = self.path.clone();
        PyHistory::new(History::new(Arc::new(path)))
    }

    /// The node properties.
    ///
    /// Returns:
    ///     PropertiesView:
    #[getter]
    fn properties(&self) -> PropertiesView {
        let path = self.path.clone();
        (move || path.properties()).into()
    }

    /// The node metadata.
    ///
    /// Returns:
    ///     MetadataView:
    #[getter]
    fn metadata(&self) -> MetadataView {
        let path = self.path.clone();
        (move || path.metadata()).into()
    }

    /// The node in-degrees.
    ///
    /// Returns:
    ///     UsizeIterable:
    fn in_degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.in_degree()).into()
    }

    /// The node out-degrees.
    ///
    /// Returns:
    ///     UsizeIterable:
    fn out_degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.out_degree()).into()
    }

    /// The node degrees.
    ///
    /// Returns:
    ///     UsizeIterable:
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
    pub fn type_filter(&self, node_types: Vec<PyBackedStr>) -> PathFromNode<'static, DynamicGraph> {
        self.path.type_filter(&node_types)
    }

    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyPathFromNode> {
        let r = self.path.select(filter)?;
        Ok(PyPathFromNode::from(r))
    }
}

impl<'graph, G: GraphViewOps<'graph>> Repr for PathFromNode<'graph, G> {
    fn repr(&self) -> String {
        format!("PathFromNode({})", iterator_repr(self.iter()))
    }
}
