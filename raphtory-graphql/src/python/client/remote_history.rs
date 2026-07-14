use crate::client::{
    remote_history::{RemoteEventTime, RemoteHistory},
    ClientError,
};
use pyo3::{pyclass, pymethods, PyRef, PyRefMut};
use raphtory::python::utils::execute_async_task;
use std::sync::Arc;

/// A single event on a node/edge's history. All three fields are optional
/// because the server can return null for any of them.
///
/// `dt` is an RFC 3339 datetime string (e.g. `"1970-01-01T00:00:00.003+00:00"`);
/// parse it to `datetime.datetime` client-side if you need a typed object.
#[derive(Clone)]
#[pyclass(name = "RemoteEventTime", module = "raphtory.graphql", get_all)]
pub struct PyRemoteEventTime {
    /// The event's timestamp in the graph's native time unit.
    pub timestamp: Option<i64>,
    /// RFC 3339 datetime string for the event.
    pub dt: Option<String>,
    /// The event's internal id.
    pub event_id: Option<i64>,
}

impl From<RemoteEventTime> for PyRemoteEventTime {
    fn from(t: RemoteEventTime) -> Self {
        Self {
            timestamp: t.timestamp,
            dt: t.dt,
            event_id: t.event_id,
        }
    }
}

#[pymethods]
impl PyRemoteEventTime {
    fn __repr__(&self) -> String {
        format!(
            "RemoteEventTime(timestamp={:?}, dt={:?}, event_id={:?})",
            self.timestamp, self.dt, self.event_id
        )
    }
}

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

    /// All events in this history in ascending time order. Fires one RPC.
    ///
    /// Returns:
    ///   list[RemoteEventTime]: one event per entry.
    pub fn list(&self) -> Result<Vec<PyRemoteEventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        let result = execute_async_task(move || async move { history.list().await })?;
        Ok(result.into_iter().map(Into::into).collect())
    }

    /// All events in this history in descending time order. Fires one RPC.
    ///
    /// Returns:
    ///   list[RemoteEventTime]: one event per entry.
    pub fn list_rev(&self) -> Result<Vec<PyRemoteEventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        let result = execute_async_task(move || async move { history.list_rev().await })?;
        Ok(result.into_iter().map(Into::into).collect())
    }

    /// Enables `for t in remote_history:` — fetches all events in one RPC
    /// via `.list()`, then yields each `RemoteEventTime` locally.
    fn __iter__(&self) -> Result<PyRemoteHistoryIter, ClientError> {
        let list = self.list()?;
        Ok(PyRemoteHistoryIter {
            inner: list.into_iter(),
        })
    }
}

/// Opaque iterator returned by `PyRemoteHistory::__iter__`.
#[pyclass(name = "RemoteHistoryIter", module = "raphtory.graphql")]
pub struct PyRemoteHistoryIter {
    inner: std::vec::IntoIter<PyRemoteEventTime>,
}

#[pymethods]
impl PyRemoteHistoryIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyRemoteEventTime> {
        slf.inner.next()
    }
}
