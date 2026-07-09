use crate::{
    client::{remote_nodes::RemoteNodes, ClientError},
    python::client::remote_node::PyRemoteNode,
};
use pyo3::{pyclass, pymethods};
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
}
