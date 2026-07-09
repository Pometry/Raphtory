use crate::{
    client::{remote_edges::RemoteEdges, ClientError},
    python::client::remote_edge::PyRemoteEdge,
};
use pyo3::{pyclass, pymethods, PyRef, PyRefMut};
use raphtory::python::utils::execute_async_task;
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
    /// Returns the number of edges in this collection. Fires one RPC.
    ///
    /// Returns:
    ///   int: the number of edges.
    pub fn count(&self) -> Result<i64, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.count().await })
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
