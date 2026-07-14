use crate::client::{remote_history::RemoteHistory, ClientError};
use pyo3::{pyclass, pymethods};
use raphtory::python::utils::execute_async_task;
use std::sync::Arc;

/// A handle to the event history of a remote node or edge.
///
/// Returned by [RemoteNode.history][raphtory.graphql.RemoteNode.history] and
/// by [RemoteEdge.history][raphtory.graphql.RemoteEdge.history] /
/// [RemoteEdge.deletions][raphtory.graphql.RemoteEdge.deletions].
///
/// Mirrors the shape of the local Python API's `History` type. This batch
/// exposes scalar terminals (`count`, `is_empty`, `earliest_time`,
/// `latest_time`); list/page terminals and sub-container accessors ship in
/// follow-up batches.
#[derive(Clone)]
#[pyclass(name = "RemoteHistory", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteHistory {
    pub(crate) history: Arc<RemoteHistory>,
}

impl PyRemoteHistory {
    pub(crate) fn new(history: RemoteHistory) -> Self {
        Self {
            history: Arc::new(history),
        }
    }
}

#[pymethods]
impl PyRemoteHistory {
    /// Number of events in this history. Fires one RPC.
    ///
    /// Returns:
    ///   int: the number of events.
    pub fn count(&self) -> Result<i64, ClientError> {
        let history = Arc::clone(&self.history);
        execute_async_task(move || async move { history.count().await })
    }

    /// Whether this history has no events. Fires one RPC.
    ///
    /// Returns:
    ///   bool: True if empty.
    pub fn is_empty(&self) -> Result<bool, ClientError> {
        let history = Arc::clone(&self.history);
        execute_async_task(move || async move { history.is_empty().await })
    }

    /// Earliest event time in this history — `None` if empty. Fires one RPC.
    ///
    /// Returns:
    ///   Optional[int]: the earliest event timestamp, or None.
    pub fn earliest_time(&self) -> Result<Option<i64>, ClientError> {
        let history = Arc::clone(&self.history);
        execute_async_task(move || async move { history.earliest_time().await })
    }

    /// Latest event time in this history — `None` if empty. Fires one RPC.
    ///
    /// Returns:
    ///   Optional[int]: the latest event timestamp, or None.
    pub fn latest_time(&self) -> Result<Option<i64>, ClientError> {
        let history = Arc::clone(&self.history);
        execute_async_task(move || async move { history.latest_time().await })
    }
}
