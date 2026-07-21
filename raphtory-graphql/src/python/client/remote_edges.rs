use crate::{
    client::{remote_edges::RemoteEdges, ClientError},
    python::client::{
        remote_edge::PyRemoteEdge, remote_history::PyRemoteEventTime, remote_sorting::PyEdgeSortBy,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyRef, PyRefMut, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use std::sync::Arc;

/// A handle to a remote collection of edges.
///
/// Returned by [RemoteGraph.edges][raphtory.graphql.RemoteGraph.edges] and by
/// [RemoteNode.edges][raphtory.graphql.RemoteNode.edges] /
/// [RemoteNode.in_edges][raphtory.graphql.RemoteNode.in_edges] /
/// [RemoteNode.out_edges][raphtory.graphql.RemoteNode.out_edges].
///
/// Edges are identified by `(src, dst)` pairs — there's no single-string id,
/// so this collection exposes `count()` and `list()` but no `ids()`.
#[derive(Clone)]
#[pyclass(name = "RemoteEdges", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteEdges {
    pub(crate) edges: Arc<RemoteEdges>,
}

impl PyRemoteEdges {
    pub(crate) fn new(edges: RemoteEdges) -> Self {
        Self {
            edges: Arc::new(edges),
        }
    }
}

#[pymethods]
impl PyRemoteEdges {
    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: i64, end: i64) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.window(start, end))
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: &str) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.before(time))
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.after(time))
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.snapshot_at(time))
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: &str) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.shrink_end(end))
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.default_layer())
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.layers(names))
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.exclude_layers(names))
    }

    /// Fan out this collection into one entry per event. Lazy — no RPC.
    pub fn explode(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.explode())
    }

    /// Fan out this collection into one entry per layer per edge. Lazy — no RPC.
    pub fn explode_layers(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.explode_layers())
    }

    /// Reorder this collection by an ordered list of sort keys. Multi-key
    /// sort is lexicographic (ties on key 1 break to key 2). Lazy — no RPC.
    ///
    /// Arguments:
    ///     sort_bys (list[EdgeSortBy]): the ordered sort keys.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection in the sorted order.
    pub fn sorted(&self, sort_bys: Vec<PyEdgeSortBy>) -> PyRemoteEdges {
        let inner: Vec<_> = sort_bys.into_iter().map(|s| s.inner).collect();
        PyRemoteEdges::new(self.edges.sorted(inner))
    }

    /// Filter this collection by a filter expression. **The filter
    /// propagates**: it applies to the current collection's membership *and*
    /// to downstream traversals from the matching edges. For a
    /// narrow-here-only variant, use `.select(...)`. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with the filter applied.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented as a GraphQL
    ///         `EdgeFilter` (e.g. uses an unsupported operator like
    ///         `FuzzySearch`).
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemoteEdges> {
        let composite = filter
            .try_as_edge_filter()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let gql_filter = composite
            .try_into()
            .map_err(|e: raphtory::errors::GraphError| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteEdges::new(self.edges.filter(gql_filter)))
    }

    /// Narrow this collection's membership by a filter expression. Unlike
    /// `.filter()`, the filter applies **only at this step** — downstream
    /// traversals from the matching edges see the unfiltered graph. Use
    /// `.filter()` for the propagating variant. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection narrowed to matching edges.
    pub fn select(&self, filter: PyFilterExpr) -> PyResult<PyRemoteEdges> {
        let composite = filter
            .try_as_edge_filter()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let gql_filter = composite
            .try_into()
            .map_err(|e: raphtory::errors::GraphError| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteEdges::new(self.edges.select(gql_filter)))
    }

    /// Returns the number of edges in this collection. Fires one RPC.
    ///
    /// Returns:
    ///   int: the number of edges.
    pub fn count(&self) -> Result<i64, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.count().await })
    }

    /// View start bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    #[getter]
    pub fn start(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(
            execute_async_task(move || async move { edges.start().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// View end bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    #[getter]
    pub fn end(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(
            execute_async_task(move || async move { edges.end().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// Materialize this collection as a list of `RemoteEdge` handles.
    ///
    /// Fires one RPC (to fetch each edge's `(src, dst)` pair); each returned
    /// edge is rebased under the view chain that produced this collection.
    ///
    /// Returns:
    ///   list[RemoteEdge]: one handle per edge in the collection.
    pub fn list(&self) -> Result<Vec<PyRemoteEdge>, ClientError> {
        let edges = Arc::clone(&self.edges);
        let result = execute_async_task(move || async move { edges.list().await })?;
        Ok(result.into_iter().map(PyRemoteEdge::new).collect())
    }

    /// Enables `for e in remote_edges:` — fetches all `(src, dst)` pairs in
    /// one RPC, then yields a `RemoteEdge` handle for each. No per-edge RPC
    /// batching yet; each terminal on a yielded edge fires its own RPC.
    fn __iter__(&self) -> Result<PyRemoteEdgesIter, ClientError> {
        let list = self.list()?;
        Ok(PyRemoteEdgesIter {
            inner: list.into_iter(),
        })
    }
}

/// Opaque iterator returned by `PyRemoteEdges::__iter__`.
///
/// Not intended to be constructed directly — Python creates it via
/// `iter(remote_edges)` (or under the hood in a `for` loop).
#[pyclass(name = "RemoteEdgesIter", module = "raphtory.graphql")]
pub struct PyRemoteEdgesIter {
    inner: std::vec::IntoIter<PyRemoteEdge>,
}

#[pymethods]
impl PyRemoteEdgesIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyRemoteEdge> {
        slf.inner.next()
    }
}
