use crate::{
    client::{remote_nodes::RemoteNodes, ClientError},
    python::client::remote_node::PyRemoteNode,
};
use pyo3::{pyclass, pymethods, PyRef, PyRefMut};
use raphtory::python::utils::execute_async_task;
use std::sync::Arc;

/// A handle to a remote collection of nodes.
///
/// Returned by [RemoteGraph.nodes][raphtory.graphql.RemoteGraph.nodes] and by
/// [RemoteNode.neighbours][raphtory.graphql.RemoteNode.neighbours] /
/// [RemoteNode.in_neighbours][raphtory.graphql.RemoteNode.in_neighbours] /
/// [RemoteNode.out_neighbours][raphtory.graphql.RemoteNode.out_neighbours].
#[derive(Clone)]
#[pyclass(name = "RemoteNodes", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteNodes {
    pub(crate) nodes: Arc<RemoteNodes>,
}

impl PyRemoteNodes {
    pub(crate) fn new(nodes: RemoteNodes) -> Self {
        Self {
            nodes: Arc::new(nodes),
        }
    }
}

#[pymethods]
impl PyRemoteNodes {
    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: i64, end: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.window(start, end))
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: &str) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.before(time))
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.after(time))
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.snapshot_at(time))
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: &str) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.shrink_end(end))
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.default_layer())
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.layers(names))
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.exclude_layers(names))
    }

    /// Returns the list of node ids in this collection.
    ///
    /// Fires one RPC.
    ///
    /// Returns:
    ///   list[str]: the ids of the nodes.
    pub fn ids(&self) -> Result<Vec<String>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.ids().await })
    }

    /// Returns the number of nodes in this collection. Fires one RPC.
    ///
    /// Returns:
    ///   int: the number of nodes.
    pub fn count(&self) -> Result<i64, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.count().await })
    }

    /// View start bound for this collection — `None` if unbounded. Fires one RPC.
    pub fn start(&self) -> Result<Option<i64>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.start().await })
    }

    /// View end bound for this collection — `None` if unbounded. Fires one RPC.
    pub fn end(&self) -> Result<Option<i64>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.end().await })
    }

    /// Materialize this collection as a list of `RemoteNode` handles.
    ///
    /// Fires one RPC (to fetch the ids); each returned node wraps its id in a
    /// fresh read expression rooted at the graph. Note: the view chain that
    /// produced this collection is *not* propagated to the returned nodes —
    /// see the module docstring for details.
    ///
    /// Returns:
    ///   list[RemoteNode]: one handle per node in the collection.
    pub fn list(&self) -> Result<Vec<PyRemoteNode>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        let result = execute_async_task(move || async move { nodes.list().await })?;
        Ok(result.into_iter().map(PyRemoteNode::new).collect())
    }

    /// Enables `for n in remote_nodes:` — fetches all ids in one RPC, then
    /// yields a `RemoteNode` handle for each. No per-node RPC batching yet
    /// (planned as a follow-up); each terminal on a yielded node fires its
    /// own RPC.
    fn __iter__(&self) -> Result<PyRemoteNodesIter, ClientError> {
        let list = self.list()?;
        Ok(PyRemoteNodesIter {
            inner: list.into_iter(),
        })
    }
}

/// Opaque iterator returned by `PyRemoteNodes::__iter__`.
///
/// Not intended to be constructed directly — Python creates it via
/// `iter(remote_nodes)` (or under the hood in a `for` loop).
#[pyclass(name = "RemoteNodesIter", module = "raphtory.graphql")]
pub struct PyRemoteNodesIter {
    inner: std::vec::IntoIter<PyRemoteNode>,
}

#[pymethods]
impl PyRemoteNodesIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyRemoteNode> {
        slf.inner.next()
    }
}
