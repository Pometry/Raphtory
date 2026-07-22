use crate::{
    client::{remote_path_from_node::RemotePathFromNode, ClientError},
    python::client::{remote_history::PyRemoteEventTime, remote_node::PyRemoteNode},
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyRef, PyRefMut, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use std::sync::Arc;

/// A handle to a "path from node" collection.
///
/// Produced by [RemoteNode.neighbours][raphtory.graphql.RemoteNode.neighbours] /
/// [RemoteNode.in_neighbours][raphtory.graphql.RemoteNode.in_neighbours] /
/// [RemoteNode.out_neighbours][raphtory.graphql.RemoteNode.out_neighbours].
///
/// Distinct from `RemoteNodes` because the server type (`GqlPathFromNode`)
/// exposes a strict subset of `GqlNodes`. **`sorted` and `default_layer`
/// are not available here.**
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
    pub fn window(&self, start: i64, end: i64) -> PyRemotePathFromNode {
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
        let composite = filter
            .try_as_node_filter()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let gql_filter = composite
            .try_into()
            .map_err(|e: raphtory::errors::GraphError| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemotePathFromNode::new(self.path.filter(gql_filter)))
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
        let gql_filter = composite
            .try_into()
            .map_err(|e: raphtory::errors::GraphError| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemotePathFromNode::new(self.path.select(gql_filter)))
    }

    /// `path[filter]` — sugar for `.select(filter)` (matches the local
    /// `PathFromNode.__getitem__`). Lazy — no RPC.
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyRemotePathFromNode> {
        self.select(filter)
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: &str) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.before(time))
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.after(time))
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.snapshot_at(time))
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: &str) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.shrink_end(end))
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.layers(names))
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.exclude_layers(names))
    }

    /// Restrict this collection to members whose node type is in the given
    /// list. Lazy — no RPC.
    pub fn type_filter(&self, node_types: Vec<String>) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.path.type_filter(node_types))
    }

    /// Returns the list of node ids in this collection. Fires one RPC.
    pub fn ids(&self) -> Result<Vec<String>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.ids().await })
    }

    /// Returns the number of nodes in this collection. Fires one RPC.
    pub fn count(&self) -> Result<i64, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.count().await })
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
    #[getter]
    pub fn start(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(
            execute_async_task(move || async move { path.start().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// View end bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    #[getter]
    pub fn end(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(execute_async_task(move || async move { path.end().await })?
            .map(PyRemoteEventTime::from))
    }

    /// Materialize this collection as a list of `RemoteNode` handles. Fires
    /// one RPC. Each returned node is rebased under the same view chain
    /// that produced this collection.
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
