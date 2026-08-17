use crate::{
    client::{remote_path_from_node::RemotePathFromNode, ClientError},
    python::client::{
        remote_collection_metadata::{PyRemoteMetadataView, PyRemotePropertiesView},
        remote_edges::PyRemoteEdges,
        remote_history::PyRemoteHistory,
        remote_node::PyRemoteNode,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyRef, PyRefMut, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use raphtory_api::{
    core::{storage::timeindex::EventTime, utils::time::InputTime},
    python::timeindex::PyOptionalEventTime,
};
use std::sync::Arc;

/// A handle to a "path from node" collection.
///
/// Produced by [RemoteNode.neighbours][raphtory.graphql.RemoteNode.neighbours] /
/// [RemoteNode.in_neighbours][raphtory.graphql.RemoteNode.in_neighbours] /
/// [RemoteNode.out_neighbours][raphtory.graphql.RemoteNode.out_neighbours].
///
/// Distinct from `RemoteNodes` because the server type (`GqlPathFromNode`)
/// exposes a strict subset of `GqlNodes`. **`sorted` is not available here.**
#[derive(Clone)]
#[pyclass(
    name = "RemotePathFromNode",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemotePathFromNode {
    pub(crate) path: Arc<RemotePathFromNode>,
}

impl PyRemotePathFromNode {
    pub(crate) fn new(path: RemotePathFromNode) -> Self {
        Self {
            path: Arc::new(path),
        }
    }
}

#[pymethods]
impl PyRemotePathFromNode {
    /// Time-window this collection. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): inclusive start of the window.
    ///     end (TimeInput): exclusive end of the window.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection restricted to the window.
    pub fn window(&self, start: InputTime, end: InputTime) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.window(start, end))
    }

    /// Filter this collection by a node filter. **Propagates** to downstream
    /// traversals from the matching nodes. Mirrors the local
    /// `PathFromNode.filter(FilterExpr)`. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a node filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection with the filter applied.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented as a GraphQL
    ///         `NodeFilter`.
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemotePathFromNode> {
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemotePathFromNode::new(self.path.filter(tree)?))
    }

    /// Narrow this collection's membership by a node filter — applies only at
    /// this step; downstream traversals see the unfiltered graph. Server-only
    /// (no local `PathFromNode.select`). Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a node filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection narrowed to matching nodes.
    pub fn select(&self, filter: PyFilterExpr) -> PyResult<PyRemotePathFromNode> {
        let composite = filter
            .try_as_node_filter()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemotePathFromNode::new(self.path.select(composite)?))
    }

    /// `path[filter]` — sugar for `.select(filter)` (matches the local
    /// `PathFromNode.__getitem__`). Lazy — no RPC.
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyRemotePathFromNode> {
        self.select(filter)
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection restricted to that layer.
    pub fn layer(&self, name: &str) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection snapshotted at that time.
    pub fn at(&self, time: InputTime) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly before this time are kept.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection restricted to events before that time.
    pub fn before(&self, time: InputTime) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.before(time))
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly after this time are kept.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection restricted to events after that time.
    pub fn after(&self, time: InputTime) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.after(time))
    }

    /// Latest state. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection of the latest state.
    pub fn latest(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection snapshotted at the latest time.
    pub fn snapshot_latest(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection snapshotted at that time.
    pub fn snapshot_at(&self, time: InputTime) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.snapshot_at(time))
    }

    /// Exclude a specific layer. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to exclude.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection with that layer excluded.
    pub fn exclude_layer(&self, name: &str) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection with both window bounds shrunk.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection with the window start shrunk.
    pub fn shrink_start(&self, start: InputTime) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection with the window end shrunk.
    pub fn shrink_end(&self, end: InputTime) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.shrink_end(end))
    }

    /// Restrict to the default layer. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection restricted to the default layer.
    pub fn default_layer(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.default_layer())
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection restricted to those layers.
    pub fn layers(&self, names: Vec<String>) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.layers(names))
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers to exclude.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection with those layers excluded.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.exclude_layers(names))
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection restricted to those valid layers.
    pub fn valid_layers(&self, names: Vec<String>) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.valid_layers(names))
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the valid layer to exclude.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection with that valid layer excluded.
    pub fn exclude_valid_layer(&self, name: &str) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.exclude_valid_layer(name))
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers to exclude.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection with those valid layers excluded.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.exclude_valid_layers(names))
    }

    /// Restrict this collection to members whose node type is in the given
    /// list. Lazy — no RPC.
    ///
    /// Arguments:
    ///     node_types (list[str]): the node types to keep.
    ///
    /// Returns:
    ///     RemotePathFromNode: a new collection restricted to those node types.
    pub fn type_filter(&self, node_types: Vec<String>) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.type_filter(node_types))
    }

    /// The neighbours (both directions) reachable one further hop from this
    /// path, as a flat `RemotePathFromNode`. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePathFromNode: the neighbours one further hop from this path.
    #[getter]
    pub fn neighbours(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.neighbours())
    }

    /// The in-neighbours reachable one further hop from this path, as a flat
    /// `RemotePathFromNode`. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePathFromNode: the in-neighbours one further hop from this path.
    #[getter]
    pub fn in_neighbours(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.in_neighbours())
    }

    /// The out-neighbours reachable one further hop from this path, as a flat
    /// `RemotePathFromNode`. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePathFromNode: the out-neighbours one further hop from this path.
    #[getter]
    pub fn out_neighbours(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.out_neighbours())
    }

    /// The incident edges (both directions) of this path, as a flat
    /// `RemoteEdges` collection. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: the incident edges of this path.
    #[getter]
    pub fn edges(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.path.edges())
    }

    /// The incoming edges of this path, as a flat `RemoteEdges` collection.
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: the incoming edges of this path.
    #[getter]
    pub fn in_edges(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.path.in_edges())
    }

    /// The outgoing edges of this path, as a flat `RemoteEdges` collection.
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: the outgoing edges of this path.
    #[getter]
    pub fn out_edges(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.path.out_edges())
    }

    /// The id of each node in this path. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     list[str]: the ids, in path order.
    #[getter]
    pub fn id(&self) -> Result<Vec<String>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.id().await })
    }

    /// The name of each node in this path. Property — attribute access fires
    /// one RPC.
    ///
    /// Returns:
    ///     list[str]: the names, in path order.
    #[getter]
    pub fn name(&self) -> Result<Vec<String>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.name().await })
    }

    /// The type of each node in this path (`None` when unset). Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///     list[Optional[str]]: the node types, in path order.
    #[getter]
    pub fn node_type(&self) -> Result<Vec<Option<String>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.node_type().await })
    }

    /// The earliest event time of each node in this path. Property — attribute
    /// access fires one RPC.
    ///
    /// Returns:
    ///   list[Optional[EventTime]]: the earliest times, in collection order.
    #[getter]
    pub fn earliest_time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(
            execute_async_task(move || async move { path.earliest_time().await })?
                .into_iter()
                .map(|o| o)
                .collect(),
        )
    }

    /// The latest event time of each node in this path. Property — attribute
    /// access fires one RPC.
    ///
    /// Returns:
    ///   list[Optional[EventTime]]: the latest times, in collection order.
    #[getter]
    pub fn latest_time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(
            execute_async_task(move || async move { path.latest_time().await })?
                .into_iter()
                .map(|o| o)
                .collect(),
        )
    }

    /// The non-temporal metadata of this path as a columnar view. Each accessor
    /// returns one value per node. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteMetadataView: the columnar metadata view of this path.
    #[getter]
    pub fn metadata(&self) -> PyRemoteMetadataView {
        PyRemoteMetadataView::new(self.path.metadata())
    }

    /// The properties of this path as a columnar view. Each accessor returns
    /// one value per node. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePropertiesView: the columnar properties view of this path.
    #[getter]
    pub fn properties(&self) -> PyRemotePropertiesView {
        PyRemotePropertiesView::new(self.path.properties())
    }

    /// Returns the number of nodes in this collection. Fires one RPC.
    ///
    /// Returns:
    ///     int: the number of nodes.
    pub fn count(&self) -> Result<i64, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.count().await })
    }

    /// Returns the degree of each node in this path. Fires one RPC.
    ///
    /// Returns:
    ///   list[int]: the per-node degrees, in path order.
    pub fn degree(&self) -> Result<Vec<i64>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.degree().await })
    }

    /// Returns the in-degree of each node in this path. Fires one RPC.
    ///
    /// Returns:
    ///   list[int]: the per-node in-degrees, in path order.
    pub fn in_degree(&self) -> Result<Vec<i64>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.in_degree().await })
    }

    /// Returns the out-degree of each node in this path. Fires one RPC.
    ///
    /// Returns:
    ///   list[int]: the per-node out-degrees, in path order.
    pub fn out_degree(&self) -> Result<Vec<i64>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.out_degree().await })
    }

    /// Returns the number of incident edge updates for each node in this path.
    /// Fires one RPC.
    ///
    /// Returns:
    ///   list[int]: the per-node edge history counts, in path order.
    pub fn edge_history_count(&self) -> Result<Vec<i64>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.edge_history_count().await })
    }

    /// Check if this view has a layer named `name`. Fires one RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to check.
    ///
    /// Returns:
    ///     bool: True if the layer is present.
    pub fn has_layer(&self, name: &str) -> Result<bool, ClientError> {
        let path = Arc::clone(&self.path);
        let name = name.to_string();
        execute_async_task(move || async move { path.has_layer(name).await })
    }

    /// The size of the window covered by this view (`end - start`), or `None`
    /// if the view is unbounded. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the size of the window, or `None` if the view is unbounded.
    #[getter]
    pub fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.window_size().await })
    }

    /// A single combined event history for all nodes reachable from the source
    /// in this view — a `RemoteHistory` container. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteHistory: the combined event history of the nodes in this view.
    pub fn combined_history(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.path.combined_history())
    }

    /// `len(path)` — number of nodes in the collection. Fires one RPC.
    pub fn __len__(&self) -> Result<usize, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(execute_async_task(move || async move { path.count().await })?.max(0) as usize)
    }

    /// `bool(path)` — whether the collection is non-empty. Fires one RPC.
    pub fn __bool__(&self) -> Result<bool, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(execute_async_task(move || async move { path.count().await })? > 0)
    }

    /// View start bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the view start bound, or empty if unbounded.
    #[getter]
    pub fn start(&self) -> Result<PyOptionalEventTime, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(execute_async_task(move || async move { path.start().await })?.into())
    }

    /// View end bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the view end bound, or empty if unbounded.
    #[getter]
    pub fn end(&self) -> Result<PyOptionalEventTime, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(execute_async_task(move || async move { path.end().await })?.into())
    }

    /// Materialize this collection as a list of `RemoteNode` handles. Fires
    /// one RPC. Each returned node is rebased under the same view chain
    /// that produced this collection.
    ///
    /// Returns:
    ///     list[RemoteNode]: one handle per node in the collection.
    pub fn collect(&self) -> Result<Vec<PyRemoteNode>, ClientError> {
        let path = Arc::clone(&self.path);
        let result = execute_async_task(move || async move { path.collect().await })?;
        Ok(result.into_iter().map(PyRemoteNode::new).collect())
    }

    /// Enables `for n in remote_path_from_node:` — fetches all ids in one
    /// RPC, then yields a `RemoteNode` handle for each.
    fn __iter__(&self) -> Result<PyRemotePathFromNodeIter, ClientError> {
        let list = self.collect()?;
        Ok(PyRemotePathFromNodeIter {
            inner: list.into_iter(),
        })
    }
}

#[pyclass(name = "RemotePathFromNodeIter", module = "raphtory.graphql")]
pub struct PyRemotePathFromNodeIter {
    inner: std::vec::IntoIter<PyRemoteNode>,
}

#[pymethods]
impl PyRemotePathFromNodeIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyRemoteNode> {
        slf.inner.next()
    }
}
