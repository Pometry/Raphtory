use crate::{
    client::{remote_node::RemoteNode, ClientError},
    python::client::{
        remote_edges::PyRemoteEdges,
        remote_history::PyRemoteHistory,
        remote_metadata::{PyRemoteMetadata, PyRemoteProperties},
        remote_nodes::PyRemoteNodes,
        remote_path_from_node::PyRemotePathFromNode,
    },
};
use pyo3::{
    exceptions::{PyKeyError, PyValueError},
    pyclass, pymethods, PyResult,
};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use raphtory_api::core::{
    entities::properties::prop::Prop, storage::timeindex::EventTime, utils::time::InputTime,
};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
#[pyclass(name = "RemoteNode", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteNode {
    pub(crate) node: Arc<RemoteNode>,
}

impl PyRemoteNode {
    /// New node.
    ///
    /// Arguments:
    ///   path (str):
    ///   client (RaphtoryClient):
    ///   id (str):
    ///
    /// Returns:
    ///   None:
    pub(crate) fn new(node: RemoteNode) -> Self {
        Self {
            node: Arc::new(node),
        }
    }
}

#[pymethods]
impl PyRemoteNode {
    /// Time-window this node. Lazy — no RPC.
    pub fn window(&self, start: InputTime, end: InputTime) -> PyRemoteNode {
        PyRemoteNode::new(self.node.window(start, end))
    }

    /// Return a filtered view of this node — mirrors the local
    /// `Node.filter(FilterExpr)`. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a node filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteNode: a new filtered node view.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented as a GraphQL
    ///         `NodeFilter` (e.g. references edge fields).
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemoteNode> {
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteNode::new(self.node.filter(tree)?))
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: &str) -> PyRemoteNode {
        PyRemoteNode::new(self.node.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: InputTime) -> PyRemoteNode {
        PyRemoteNode::new(self.node.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: InputTime) -> PyRemoteNode {
        PyRemoteNode::new(self.node.before(time))
    }

    /// Restrict to events strictly after the given time (exclusive). Lazy — no RPC.
    pub fn after(&self, time: InputTime) -> PyRemoteNode {
        PyRemoteNode::new(self.node.after(time))
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.node.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.node.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: InputTime) -> PyRemoteNode {
        PyRemoteNode::new(self.node.snapshot_at(time))
    }

    /// Exclude a specific layer from the view. Lazy — no RPC.
    pub fn exclude_layer(&self, name: &str) -> PyRemoteNode {
        PyRemoteNode::new(self.node.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> PyRemoteNode {
        PyRemoteNode::new(self.node.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: InputTime) -> PyRemoteNode {
        PyRemoteNode::new(self.node.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: InputTime) -> PyRemoteNode {
        PyRemoteNode::new(self.node.shrink_end(end))
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.node.default_layer())
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> PyRemoteNode {
        PyRemoteNode::new(self.node.layers(names))
    }

    /// Exclude the given set of layers from the view. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemoteNode {
        PyRemoteNode::new(self.node.exclude_layers(names))
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> PyRemoteNode {
        PyRemoteNode::new(self.node.valid_layers(names))
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: &str) -> PyRemoteNode {
        PyRemoteNode::new(self.node.exclude_valid_layer(name))
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> PyRemoteNode {
        PyRemoteNode::new(self.node.exclude_valid_layers(names))
    }

    /// Set the type on the node. This only works if the type has not been previously set, otherwise will
    /// throw an error
    ///
    /// Arguments:
    ///   new_type (str): The new type to be set
    ///
    /// Returns:
    ///   None:
    pub fn set_node_type(&self, new_type: &str) -> Result<(), ClientError> {
        let node = Arc::clone(&self.node);
        let new_type = new_type.to_string();

        let task = move || async move { node.set_node_type(new_type).await };
        execute_async_task(task)?;
        Ok(())
    }

    /// Add updates to a node in the remote graph at a specified time.
    /// This function allows for the addition of property updates to a node within the graph. The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Arguments:
    ///   t (int | str | datetime): The timestamp at which the updates should be applied.
    ///   properties (dict[str, PropValue], optional): A dictionary of properties to update.
    ///   event_id (int, optional): Secondary index to disambiguate multiple
    ///       updates at the same timestamp. If omitted, the server auto-increments it.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (t, properties=None, event_id=None))]
    pub fn add_updates(
        &self,
        t: EventTime,
        properties: Option<HashMap<String, Prop>>,
        event_id: Option<usize>,
    ) -> Result<(), ClientError> {
        let node = Arc::clone(&self.node);

        let task = move || async move { node.add_updates(t, properties, event_id).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Add metadata to a node in the remote graph.
    /// This function is used to add properties to a node that do not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Arguments:
    ///   properties (dict[str, PropValue]): A dictionary of properties to be added to the node.
    ///
    /// Returns:
    ///   None:
    pub fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let node = Arc::clone(&self.node);

        let task = move || async move { node.add_metadata(properties).await };
        execute_async_task(task)?;
        Ok(())
    }

    /// Update metadata of a node in the remote graph overwriting existing values.
    /// This function is used to add properties to a node that does not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Arguments:
    ///   properties (dict[str, PropValue]): A dictionary of properties to be added to the node.
    ///
    /// Returns:
    ///   None:
    pub fn update_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let node = Arc::clone(&self.node);

        let task = move || async move { node.update_metadata(properties).await };
        execute_async_task(task)?;
        Ok(())
    }

    /// Returns the degree of the node, evaluated under the current view chain
    /// (e.g. under any `rg.window(...)` applied on the parent graph).
    ///
    /// Fires one RPC to the server.
    ///
    /// Returns:
    ///   int: the node's degree
    pub fn degree(&self) -> Result<i64, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.degree().await })
    }

    /// Returns the in-degree of the node under the current view chain.
    /// Fires one RPC.
    pub fn in_degree(&self) -> Result<i64, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.in_degree().await })
    }

    /// Returns the out-degree of the node under the current view chain.
    /// Fires one RPC.
    pub fn out_degree(&self) -> Result<i64, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.out_degree().await })
    }

    /// The node's name. Property — attribute access fires one RPC.
    #[getter]
    pub fn name(&self) -> Result<String, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.name().await })
    }

    /// Earliest event time on this node under the current view. `None` if the
    /// node has no events. Property — attribute access fires one RPC.
    #[getter]
    pub fn earliest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let node = Arc::clone(&self.node);
        Ok(
            execute_async_task(move || async move { node.earliest_time().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// Latest event time on this node. Property — attribute access fires one RPC.
    #[getter]
    pub fn latest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let node = Arc::clone(&self.node);
        Ok(
            execute_async_task(move || async move { node.latest_time().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// View start bound as seen by this node. Property — fires one RPC.
    #[getter]
    pub fn start(&self) -> Result<Option<EventTime>, ClientError> {
        let node = Arc::clone(&self.node);
        Ok(
            execute_async_task(move || async move { node.start().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// View end bound as seen by this node. Property — fires one RPC.
    #[getter]
    pub fn end(&self) -> Result<Option<EventTime>, ClientError> {
        let node = Arc::clone(&self.node);
        Ok(execute_async_task(move || async move { node.end().await })?
            .and_then(|t| t.to_event_time()))
    }

    /// The node's id (as a string, even if the graph uses integer GIDs).
    /// Property — attribute access fires one RPC.
    #[getter]
    pub fn id(&self) -> Result<String, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.id().await })
    }

    /// The node's type. `None` if not set. Property — fires one RPC.
    #[getter]
    pub fn node_type(&self) -> Result<Option<String>, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.node_type().await })
    }

    /// Whether the node has any events in the current view. Fires one RPC.
    pub fn is_active(&self) -> Result<bool, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.is_active().await })
    }

    /// Check if this view has a layer named `name`. Fires one RPC.
    pub fn has_layer(&self, name: &str) -> Result<bool, ClientError> {
        let node = Arc::clone(&self.node);
        let name = name.to_string();
        execute_async_task(move || async move { node.has_layer(name).await })
    }

    /// The size of the window covered by this view (`end - start`), or `None`
    /// if the view is unbounded. Property — attribute access fires one RPC.
    #[getter]
    pub fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.window_size().await })
    }

    /// Count of temporal edge events on this node. Fires one RPC.
    pub fn edge_history_count(&self) -> Result<i64, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.edge_history_count().await })
    }

    /// First update timestamp on this node under the current view. Returns
    /// `None` if the node has no updates in the view. Fires one RPC.
    pub fn first_update(&self) -> Result<Option<i64>, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.first_update().await })
    }

    /// Last update timestamp on this node under the current view. Returns
    /// `None` if the node has no updates in the view. Fires one RPC.
    pub fn last_update(&self) -> Result<Option<i64>, ClientError> {
        let node = Arc::clone(&self.node);
        execute_async_task(move || async move { node.last_update().await })
    }

    /// This node's neighbours (both directions). Lazy — no RPC. Returns a
    /// `RemotePathFromNode` (not `RemoteNodes`) — see that type for the
    /// available methods; `sorted` and `default_layer` are not available.
    #[getter]
    pub fn neighbours(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.node.neighbours())
    }

    /// This node's in-neighbours. Lazy — no RPC. See `neighbours` for
    /// return-type notes.
    #[getter]
    pub fn in_neighbours(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.node.in_neighbours())
    }

    /// This node's out-neighbours. Lazy — no RPC. See `neighbours` for
    /// return-type notes.
    #[getter]
    pub fn out_neighbours(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.node.out_neighbours())
    }

    /// The in-component of this node — nodes that can reach this node via
    /// incoming edges (ancestors, not including self). Lazy — no RPC.
    #[getter]
    pub fn in_component(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.node.in_component())
    }

    /// The out-component of this node — nodes reachable from this node via
    /// outgoing edges (descendants, not including self). Lazy — no RPC.
    #[getter]
    pub fn out_component(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.node.out_component())
    }

    /// The collection of this node's edges (both directions). Lazy — no RPC.
    #[getter]
    pub fn edges(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.node.edges())
    }

    /// The collection of this node's incoming edges. Lazy — no RPC.
    #[getter]
    pub fn in_edges(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.node.in_edges())
    }

    /// The collection of this node's outgoing edges. Lazy — no RPC.
    #[getter]
    pub fn out_edges(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.node.out_edges())
    }

    /// The event history of this node — a `RemoteHistory` container with
    /// terminals like `count()`, `collect()`, `earliest_time()`, and the
    /// `.t` / `.dt` / `.event_id` / `.intervals` sub-container accessors.
    /// Lazy — no RPC.
    #[getter]
    pub fn history(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.node.history())
    }

    /// The non-temporal metadata container of this node. Lazy — no RPC.
    #[getter]
    pub fn metadata(&self) -> PyRemoteMetadata {
        PyRemoteMetadata::new(self.node.metadata())
    }

    /// The full properties container of this node (temporal + metadata).
    /// Lazy — no RPC.
    #[getter]
    pub fn properties(&self) -> PyRemoteProperties {
        PyRemoteProperties::new(self.node.properties())
    }

    /// `node[key]` — the property value for `key`, or raises `KeyError` if
    /// absent (matches the local `Node.__getitem__`). Fires one RPC.
    fn __getitem__(&self, name: String) -> PyResult<Prop> {
        match self.properties().get(name.clone())? {
            Some(v) => Ok(v),
            None => Err(PyKeyError::new_err(format!("Unknown property {name}"))),
        }
    }
}
