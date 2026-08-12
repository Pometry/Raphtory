use crate::{
    client::{remote_edge::RemoteEdge, ClientError},
    python::client::{
        remote_edges::PyRemoteEdges,
        remote_history::PyRemoteHistory,
        remote_metadata::{PyRemoteMetadata, PyRemoteProperties},
        remote_node::PyRemoteNode,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use raphtory_api::core::{
    entities::properties::prop::Prop, storage::timeindex::EventTime, utils::time::InputTime,
};
use std::{collections::HashMap, sync::Arc};

/// A remote edge reference
///
/// Returned by [RemoteGraph.edge][raphtory.graphql.RemoteGraph.edge],
/// [RemoteGraph.add_edge][raphtory.graphql.RemoteGraph.add_edge],
/// and [RemoteGraph.delete_edge][raphtory.graphql.RemoteGraph.delete_edge].
#[derive(Clone)]
#[pyclass(name = "RemoteEdge", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteEdge {
    pub(crate) edge: Arc<RemoteEdge>,
}

impl PyRemoteEdge {
    pub(crate) fn new(edge: RemoteEdge) -> Self {
        PyRemoteEdge {
            edge: Arc::new(edge),
        }
    }
}

#[pymethods]
impl PyRemoteEdge {
    /// Time-window this edge. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): inclusive start of the window.
    ///     end (TimeInput): exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteEdge: a new view restricted to the window.
    pub fn window(&self, start: InputTime, end: InputTime) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.window(start, end))
    }

    /// Return a filtered view of this edge — the filter propagates to
    /// everything reached through it. Accepts node or edge filter
    /// expressions; mirrors the local `Edge.filter`. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteEdge: a new filtered edge view.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented remotely.
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemoteEdge> {
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteEdge::new(self.edge.filter(tree)?))
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer.
    ///
    /// Returns:
    ///     RemoteEdge: a new view restricted to that layer.
    pub fn layer(&self, name: &str) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemoteEdge: a new view snapshotted at that time.
    pub fn at(&self, time: InputTime) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly before this time are kept.
    ///
    /// Returns:
    ///     RemoteEdge: a new view restricted to events before that time.
    pub fn before(&self, time: InputTime) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.before(time))
    }

    /// Restrict to events strictly after the given time (exclusive). Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly after this time are kept.
    ///
    /// Returns:
    ///     RemoteEdge: a new view restricted to events after that time.
    pub fn after(&self, time: InputTime) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.after(time))
    }

    /// Latest state. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdge: a new view of the latest state.
    pub fn latest(&self) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdge: a new view snapshotted at the latest time.
    pub fn snapshot_latest(&self) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemoteEdge: a new view snapshotted at that time.
    pub fn snapshot_at(&self, time: InputTime) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.snapshot_at(time))
    }

    /// Exclude a specific layer from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to exclude.
    ///
    /// Returns:
    ///     RemoteEdge: a new view with that layer excluded.
    pub fn exclude_layer(&self, name: &str) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteEdge: a new view with both window bounds shrunk.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///
    /// Returns:
    ///     RemoteEdge: a new view with the window start shrunk.
    pub fn shrink_start(&self, start: InputTime) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteEdge: a new view with the window end shrunk.
    pub fn shrink_end(&self, end: InputTime) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.shrink_end(end))
    }

    /// Restrict to the default layer. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdge: a new view restricted to the default layer.
    pub fn default_layer(&self) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.default_layer())
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers.
    ///
    /// Returns:
    ///     RemoteEdge: a new view restricted to those layers.
    pub fn layers(&self, names: Vec<String>) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.layers(names))
    }

    /// Exclude the given set of layers from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers to exclude.
    ///
    /// Returns:
    ///     RemoteEdge: a new view with those layers excluded.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.exclude_layers(names))
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers.
    ///
    /// Returns:
    ///     RemoteEdge: a new view restricted to those valid layers.
    pub fn valid_layers(&self, names: Vec<String>) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.valid_layers(names))
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the valid layer to exclude.
    ///
    /// Returns:
    ///     RemoteEdge: a new view with that valid layer excluded.
    pub fn exclude_valid_layer(&self, name: &str) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.exclude_valid_layer(name))
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers to exclude.
    ///
    /// Returns:
    ///     RemoteEdge: a new view with those valid layers excluded.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> PyRemoteEdge {
        PyRemoteEdge::new(self.edge.exclude_valid_layers(names))
    }

    /// Add updates to an edge in the remote graph at a specified time.
    ///
    /// This function allows for the addition of property updates to an edge within the graph.
    /// The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Arguments:
    ///   t (int | str | datetime): The timestamp at which the updates should be applied.
    ///   properties (dict[str, PropValue], optional): A dictionary of properties to update.
    ///   layer (str, optional): The layer you want the updates to be applied.
    ///   event_id (int, optional): Secondary index to disambiguate multiple
    ///       updates at the same timestamp. If omitted, the server auto-increments it.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (t, properties=None, layer=None, event_id=None))]
    fn add_updates(
        &self,
        t: EventTime,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task =
            move || async move { edge.add_updates(t, properties, layer_str, event_id).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Mark the edge as deleted at the specified time.
    ///
    /// Arguments:
    ///   t (int | str | datetime): The timestamp at which the deletion should be applied.
    ///   layer (str, optional): The layer you want the deletion applied to.
    ///   event_id (int, optional): Secondary index to disambiguate multiple
    ///       updates at the same timestamp. If omitted, the server auto-increments it.
    ///
    /// Returns:
    ///   None:
    ///
    /// Raises:
    ///   GraphError: If the operation fails.
    #[pyo3(signature = (t, layer=None, event_id=None))]
    fn delete(
        &self,
        t: EventTime,
        layer: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move { edge.delete(t, layer_str, event_id).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Add metadata to the edge within the remote graph.
    /// This function is used to add metadata to an edge that does not
    /// change over time. This metadata is fundamental information of the edge.
    ///
    /// Arguments:
    ///   properties (dict[str, PropValue]): A dictionary of properties to be added to the edge.
    ///   layer (str, optional): The layer you want these properties to be added on to.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (properties, layer=None))]
    fn add_metadata(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move { edge.add_metadata(properties, layer_str).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Update metadata of an edge in the remote graph overwriting existing values.
    /// This function is used to add properties to an edge that does not
    /// change over time. These properties are fundamental attributes of the edge.
    ///
    /// Arguments:
    ///   properties (dict[str, PropValue]): A dictionary of properties to be added to the edge.
    ///   layer (str, optional): The layer you want these properties to be added on to.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (properties, layer=None))]
    pub fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move { edge.update_metadata(properties, layer_str).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Navigate to this edge's source node. Lazy — no RPC.
    ///
    /// Returns:
    ///   RemoteNode: a handle to the source node, carrying the accumulated view chain.
    #[getter]
    pub fn src(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.edge.src())
    }

    /// Navigate to this edge's destination node. Property — lazy, no RPC.
    ///
    /// Returns:
    ///   RemoteNode: a handle to the destination node, carrying the accumulated view chain.
    #[getter]
    pub fn dst(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.edge.dst())
    }

    /// Navigate to the "other end" node — destination on out-edges, source
    /// on in-edges. Property — lazy, no RPC.
    ///
    /// Returns:
    ///     RemoteNode: a handle to the other-end node, carrying the accumulated view chain.
    #[getter]
    pub fn nbr(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.edge.nbr())
    }

    /// Earliest event time on this edge under the current view. `None` if the
    /// edge has no events in the view. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     Optional[EventTime]: the earliest event time on the edge, or `None` if it has no
    ///         events in view.
    #[getter]
    pub fn earliest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let edge = Arc::clone(&self.edge);
        Ok(
            execute_async_task(move || async move { edge.earliest_time().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// Latest event time on this edge under the current view. Property — RPC.
    ///
    /// Returns:
    ///     Optional[EventTime]: the latest event time on the edge, or `None` if it has no
    ///         events in view.
    #[getter]
    pub fn latest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let edge = Arc::clone(&self.edge);
        Ok(
            execute_async_task(move || async move { edge.latest_time().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// First update timestamp on this edge under the current view. Fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the first update timestamp, or `None` if the edge has no updates
    ///         in view.
    pub fn first_update(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.first_update().await })
    }

    /// Last update timestamp on this edge under the current view. Fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the last update timestamp, or `None` if the edge has no updates
    ///         in view.
    pub fn last_update(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.last_update().await })
    }

    /// The event time this exploded edge event happened at. Meaningful
    /// primarily on `explode()`'d views. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     Optional[EventTime]: the event time of this exploded edge event, or `None` if
    ///         there is none.
    #[getter]
    pub fn time(&self) -> Result<Option<EventTime>, ClientError> {
        let edge = Arc::clone(&self.edge);
        Ok(
            execute_async_task(move || async move { edge.time().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// View start bound as seen by this edge. Property — fires one RPC.
    ///
    /// Returns:
    ///     Optional[EventTime]: the view start bound, or `None` if unbounded.
    #[getter]
    pub fn start(&self) -> Result<Option<EventTime>, ClientError> {
        let edge = Arc::clone(&self.edge);
        Ok(
            execute_async_task(move || async move { edge.start().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// View end bound as seen by this edge. Property — fires one RPC.
    ///
    /// Returns:
    ///     Optional[EventTime]: the view end bound, or `None` if unbounded.
    #[getter]
    pub fn end(&self) -> Result<Option<EventTime>, ClientError> {
        let edge = Arc::clone(&self.edge);
        Ok(execute_async_task(move || async move { edge.end().await })?
            .and_then(|t| t.to_event_time()))
    }

    /// Edge id as a `(src, dst)` pair of endpoint ids. Property — fires one RPC.
    ///
    /// Returns:
    ///     tuple[str, str]: the `(src, dst)` pair of endpoint ids.
    #[getter]
    pub fn id(&self) -> Result<(String, String), ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.id().await })
    }

    /// Layer names this edge is present in. Property — fires one RPC.
    ///
    /// Returns:
    ///     list[str]: the layer names the edge is present in.
    #[getter]
    pub fn layer_names(&self) -> Result<Vec<String>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.layer_names().await })
    }

    /// Single layer name for a layer-restricted view of this edge. Raises if
    /// the edge isn't scoped to exactly one layer. Property — fires one RPC.
    ///
    /// Returns:
    ///     str: the single layer name of this view.
    #[getter]
    pub fn layer_name(&self) -> Result<String, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.layer_name().await })
    }

    /// Whether the edge has any events in the current view. Fires one RPC.
    ///
    /// Returns:
    ///     bool: True if the edge has events in the current view.
    pub fn is_active(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_active().await })
    }

    /// Whether the edge is valid at the current time. Fires one RPC.
    ///
    /// Returns:
    ///     bool: True if the edge is valid at the current time.
    pub fn is_valid(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_valid().await })
    }

    /// Whether the edge has been deleted at the current time. Fires one RPC.
    ///
    /// Returns:
    ///     bool: True if the edge has been deleted at the current time.
    pub fn is_deleted(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_deleted().await })
    }

    /// Whether the edge is a self-loop (src == dst). Fires one RPC.
    ///
    /// Returns:
    ///     bool: True if the edge is a self-loop.
    pub fn is_self_loop(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_self_loop().await })
    }

    /// Check if this view has a layer named `name`. Fires one RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to check.
    ///
    /// Returns:
    ///     bool: True if the layer is present.
    pub fn has_layer(&self, name: &str) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        let name = name.to_string();
        execute_async_task(move || async move { edge.has_layer(name).await })
    }

    /// The size of the window covered by this view (`end - start`), or `None`
    /// if the view is unbounded. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the size of the window, or `None` if the view is unbounded.
    #[getter]
    pub fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.window_size().await })
    }

    /// The event history of this edge — a `RemoteHistory` container with
    /// terminals like `count()`, `collect()`, `earliest_time()`, and the
    /// `.t` / `.dt` / `.event_id` / `.intervals` sub-container accessors.
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteHistory: the edge's event history.
    #[getter]
    pub fn history(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.edge.history())
    }

    /// The deletion history of this edge — a `RemoteHistory` container
    /// tracking the times at which the edge was marked deleted. Distinct
    /// from `history` which tracks all events. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteHistory: the edge's deletion history.
    #[getter]
    pub fn deletions(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.edge.deletions())
    }

    /// Fan out this edge into one entry per event — returns a `RemoteEdges`
    /// with each member a single-event edge instance. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: one entry per event of this edge.
    pub fn explode(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edge.explode())
    }

    /// Fan out this edge into one entry per layer — returns a `RemoteEdges`
    /// with each member a single-layer edge instance. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: one entry per layer of this edge.
    pub fn explode_layers(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edge.explode_layers())
    }

    /// The non-temporal metadata container of this edge. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteMetadata: the edge's metadata container.
    #[getter]
    pub fn metadata(&self) -> PyRemoteMetadata {
        PyRemoteMetadata::new(self.edge.metadata())
    }

    /// The full properties container of this edge (temporal + metadata).
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteProperties: the edge's properties container.
    #[getter]
    pub fn properties(&self) -> PyRemoteProperties {
        PyRemoteProperties::new(self.edge.properties())
    }

    /// `edge[key]` — the property value for `key`, or `None` if absent
    /// (matches the local `Edge.__getitem__`, which returns `Optional`).
    /// Fires one RPC.
    fn __getitem__(&self, name: String) -> Result<Option<Prop>, ClientError> {
        self.properties().get(name)
    }
}
