use crate::{
    client::{remote_path_from_graph::RemotePathFromGraph, ClientError},
    python::client::{
        remote_history::{PyRemoteEventTime, PyRemoteHistory},
        remote_nested_edges::PyRemoteNestedEdges,
        remote_node::PyRemoteNode,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyRef, PyRefMut, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use std::sync::Arc;

/// A handle to a "path from graph" collection.
///
/// Produced by [RemoteNodes.neighbours][raphtory.graphql.RemoteNodes.neighbours] /
/// [RemoteNodes.in_neighbours][raphtory.graphql.RemoteNodes.in_neighbours] /
/// [RemoteNodes.out_neighbours][raphtory.graphql.RemoteNodes.out_neighbours].
///
/// Distinct from `RemotePathFromNode` because it is **nested** — the server
/// type (`GqlPathFromGraph`) groups results per source node. `ids()` returns
/// `list[list[str]]`, `collect()` returns `list[list[RemoteNode]]`, and
/// `count()` is the number of source paths.
#[derive(Clone)]
#[pyclass(
    name = "RemotePathFromGraph",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemotePathFromGraph {
    pub(crate) path: Arc<RemotePathFromGraph>,
}

impl PyRemotePathFromGraph {
    pub(crate) fn new(path: RemotePathFromGraph) -> Self {
        Self {
            path: Arc::new(path),
        }
    }
}

#[pymethods]
impl PyRemotePathFromGraph {
    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: i64, end: i64) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.window(start, end))
    }

    /// Filter this collection by a node filter. **Propagates** to downstream
    /// traversals from the matching nodes. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a node filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemotePathFromGraph: a new collection with the filter applied.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented as a GraphQL
    ///         `NodeFilter`.
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemotePathFromGraph> {
        let composite = filter
            .try_as_node_filter()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let gql_filter = composite
            .try_into()
            .map_err(|e: raphtory::errors::GraphError| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemotePathFromGraph::new(self.path.filter(gql_filter)))
    }

    /// Narrow this collection's membership by a node filter — applies only at
    /// this step; downstream traversals see the unfiltered graph. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a node filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemotePathFromGraph: a new collection narrowed to matching nodes.
    pub fn select(&self, filter: PyFilterExpr) -> PyResult<PyRemotePathFromGraph> {
        let composite = filter
            .try_as_node_filter()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let gql_filter = composite
            .try_into()
            .map_err(|e: raphtory::errors::GraphError| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemotePathFromGraph::new(self.path.select(gql_filter)))
    }

    /// `path[filter]` — sugar for `.select(filter)`. Lazy — no RPC.
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyRemotePathFromGraph> {
        self.select(filter)
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: &str) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.before(time))
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.after(time))
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.snapshot_at(time))
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: &str) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.shrink_end(end))
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.layers(names))
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.exclude_layers(names))
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.valid_layers(names))
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: &str) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.exclude_valid_layer(name))
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.exclude_valid_layers(names))
    }

    /// Restrict this collection to members whose node type is in the given
    /// list. Lazy — no RPC.
    pub fn type_filter(&self, node_types: Vec<String>) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.type_filter(node_types))
    }

    /// The neighbours (both directions) reachable one further hop from each
    /// source path, as a nested `RemotePathFromGraph`. Lazy — no RPC.
    #[getter]
    pub fn neighbours(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.neighbours())
    }

    /// The in-neighbours reachable one further hop from each source path, as a
    /// nested `RemotePathFromGraph`. Lazy — no RPC.
    #[getter]
    pub fn in_neighbours(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.in_neighbours())
    }

    /// The out-neighbours reachable one further hop from each source path, as a
    /// nested `RemotePathFromGraph`. Lazy — no RPC.
    #[getter]
    pub fn out_neighbours(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.out_neighbours())
    }

    /// The incident edges (both directions) of each source path, as a nested
    /// `RemoteNestedEdges` collection. Lazy — no RPC.
    #[getter]
    pub fn edges(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.path.edges())
    }

    /// The incoming edges of each source path, as a nested `RemoteNestedEdges`
    /// collection. Lazy — no RPC.
    #[getter]
    pub fn in_edges(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.path.in_edges())
    }

    /// The outgoing edges of each source path, as a nested `RemoteNestedEdges`
    /// collection. Lazy — no RPC.
    #[getter]
    pub fn out_edges(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.path.out_edges())
    }

    /// Returns the nested list of node ids in this collection — one inner list
    /// per source node. Fires one RPC.
    ///
    /// Returns:
    ///   list[list[str]]: the ids grouped per source node.
    pub fn ids(&self) -> Result<Vec<Vec<String>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.ids().await })
    }

    /// Returns the number of source paths in this collection. Fires one RPC.
    pub fn count(&self) -> Result<i64, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.count().await })
    }

    /// Returns the degree of each node, grouped per source node. Fires one RPC.
    ///
    /// Returns:
    ///   list[list[int]]: the per-node degrees grouped per source node.
    pub fn degree(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.degree().await })
    }

    /// Returns the in-degree of each node, grouped per source node. Fires one RPC.
    ///
    /// Returns:
    ///   list[list[int]]: the per-node in-degrees grouped per source node.
    pub fn in_degree(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.in_degree().await })
    }

    /// Returns the out-degree of each node, grouped per source node. Fires one RPC.
    ///
    /// Returns:
    ///   list[list[int]]: the per-node out-degrees grouped per source node.
    pub fn out_degree(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.out_degree().await })
    }

    /// Returns the number of incident edge updates for each node, grouped per
    /// source node. Fires one RPC.
    ///
    /// Returns:
    ///   list[list[int]]: the per-node edge history counts grouped per source node.
    pub fn edge_history_count(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.edge_history_count().await })
    }

    /// Check if this view has a layer named `name`. Fires one RPC.
    pub fn has_layer(&self, name: &str) -> Result<bool, ClientError> {
        let path = Arc::clone(&self.path);
        let name = name.to_string();
        execute_async_task(move || async move { path.has_layer(name).await })
    }

    /// The size of the window covered by this view (`end - start`), or `None`
    /// if the view is unbounded. Property — attribute access fires one RPC.
    #[getter]
    pub fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.window_size().await })
    }

    /// A single combined event history for all nodes in this view — a
    /// `RemoteHistory` container. Lazy — no RPC.
    pub fn combined_history(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.path.combined_history())
    }

    /// `len(path)` — number of source paths in the collection. Fires one RPC.
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

    /// Materialize this collection as a nested list of `RemoteNode` handles —
    /// one inner list per source node. Fires one RPC. Each returned node is
    /// rebased under the same view chain that produced this collection.
    ///
    /// Returns:
    ///   list[list[RemoteNode]]: the neighbours grouped per source node.
    pub fn collect(&self) -> Result<Vec<Vec<PyRemoteNode>>, ClientError> {
        let path = Arc::clone(&self.path);
        let result = execute_async_task(move || async move { path.collect().await })?;
        Ok(result
            .into_iter()
            .map(|row| row.into_iter().map(PyRemoteNode::new).collect())
            .collect())
    }

    /// Enables `for row in remote_path_from_graph:` — fetches everything in one
    /// RPC, then yields each per-source `list[RemoteNode]`.
    fn __iter__(&self) -> Result<PyRemotePathFromGraphIter, ClientError> {
        let list = self.collect()?;
        Ok(PyRemotePathFromGraphIter {
            inner: list.into_iter(),
        })
    }
}

#[pyclass(name = "RemotePathFromGraphIter", module = "raphtory.graphql")]
pub struct PyRemotePathFromGraphIter {
    inner: std::vec::IntoIter<Vec<PyRemoteNode>>,
}

#[pymethods]
impl PyRemotePathFromGraphIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<Vec<PyRemoteNode>> {
        slf.inner.next()
    }
}
