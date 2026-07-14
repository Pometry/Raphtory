use crate::client::{
    remote_history::{
        RemoteEventTime, RemoteHistory, RemoteHistoryDateTimes, RemoteHistoryEventIds,
        RemoteHistoryTimestamps, RemoteIntervals,
    },
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

    /// A page of events in ascending time order — at most `limit` items,
    /// starting `page_index * limit + offset` items in. Both `offset` and
    /// `page_index` default to 0. Fires one RPC.
    ///
    /// Arguments:
    ///   limit (int): maximum number of events on this page.
    ///   offset (int, optional): additional items to skip. Defaults to 0.
    ///   page_index (int, optional): 0-based page number. Defaults to 0.
    ///
    /// Returns:
    ///   list[RemoteEventTime]: at most `limit` events.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<PyRemoteEventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        let result =
            execute_async_task(
                move || async move { history.page(limit, offset, page_index).await },
            )?;
        Ok(result.into_iter().map(Into::into).collect())
    }

    /// A page of events in descending time order. Same args as `page()`.
    /// Fires one RPC.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<PyRemoteEventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        let result = execute_async_task(move || async move {
            history.page_rev(limit, offset, page_index).await
        })?;
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

    /// Sub-container: timestamps view of this history (plain int timestamps).
    /// Lazy — no RPC.
    #[getter]
    pub fn timestamps(&self) -> PyRemoteHistoryTimestamps {
        PyRemoteHistoryTimestamps {
            inner: Arc::new(self.history.timestamps()),
        }
    }

    /// Sub-container: event-id view of this history. Lazy — no RPC.
    #[getter]
    pub fn event_id(&self) -> PyRemoteHistoryEventIds {
        PyRemoteHistoryEventIds {
            inner: Arc::new(self.history.event_id()),
        }
    }

    /// Sub-container: datetime view of this history (RFC 3339 strings).
    /// Lazy — no RPC.
    #[getter]
    pub fn datetimes(&self) -> PyRemoteHistoryDateTimes {
        PyRemoteHistoryDateTimes {
            inner: Arc::new(self.history.datetimes()),
        }
    }

    /// Sub-container: inter-event intervals view of this history. Adds
    /// stats terminals (mean/median/max/min). Lazy — no RPC.
    #[getter]
    pub fn intervals(&self) -> PyRemoteIntervals {
        PyRemoteIntervals {
            inner: Arc::new(self.history.intervals()),
        }
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

// ============ Sub-container Python types ============

/// Timestamps view of a `RemoteHistory`. Lists / pages return `list[int]`.
#[derive(Clone)]
#[pyclass(name = "RemoteHistoryTimestamps", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteHistoryTimestamps {
    pub(crate) inner: Arc<RemoteHistoryTimestamps>,
}

#[pymethods]
impl PyRemoteHistoryTimestamps {
    /// Fires one RPC.
    pub fn list(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.list().await })
    }

    /// Fires one RPC.
    pub fn list_rev(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.list_rev().await })
    }

    /// Fires one RPC.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.page(limit, offset, page_index).await })
    }

    /// Fires one RPC.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move {
            inner.page_rev(limit, offset, page_index).await
        })
    }
}

/// Event-id view of a `RemoteHistory`. Lists / pages return `list[int]`.
#[derive(Clone)]
#[pyclass(name = "RemoteHistoryEventIds", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteHistoryEventIds {
    pub(crate) inner: Arc<RemoteHistoryEventIds>,
}

#[pymethods]
impl PyRemoteHistoryEventIds {
    /// Fires one RPC.
    pub fn list(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.list().await })
    }

    /// Fires one RPC.
    pub fn list_rev(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.list_rev().await })
    }

    /// Fires one RPC.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.page(limit, offset, page_index).await })
    }

    /// Fires one RPC.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move {
            inner.page_rev(limit, offset, page_index).await
        })
    }
}

/// Datetime view of a `RemoteHistory`. Lists / pages return `list[str]`
/// (RFC 3339 formatted).
#[derive(Clone)]
#[pyclass(name = "RemoteHistoryDateTimes", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteHistoryDateTimes {
    pub(crate) inner: Arc<RemoteHistoryDateTimes>,
}

#[pymethods]
impl PyRemoteHistoryDateTimes {
    /// Fires one RPC.
    pub fn list(&self) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.list().await })
    }

    /// Fires one RPC.
    pub fn list_rev(&self) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.list_rev().await })
    }

    /// Fires one RPC.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.page(limit, offset, page_index).await })
    }

    /// Fires one RPC.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move {
            inner.page_rev(limit, offset, page_index).await
        })
    }
}

/// Intervals view of a `RemoteHistory` — inter-event gaps plus summary
/// stats (`mean`, `median`, `max`, `min`).
#[derive(Clone)]
#[pyclass(name = "RemoteIntervals", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteIntervals {
    pub(crate) inner: Arc<RemoteIntervals>,
}

#[pymethods]
impl PyRemoteIntervals {
    /// Fires one RPC.
    pub fn list(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.list().await })
    }

    /// Fires one RPC.
    pub fn list_rev(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.list_rev().await })
    }

    /// Fires one RPC.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.page(limit, offset, page_index).await })
    }

    /// Fires one RPC.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move {
            inner.page_rev(limit, offset, page_index).await
        })
    }

    /// Mean interval between consecutive events. `None` if fewer than 2 events.
    /// Fires one RPC.
    pub fn mean(&self) -> Result<Option<f64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.mean().await })
    }

    /// Median interval between consecutive events. `None` if fewer than 2 events.
    /// Fires one RPC.
    pub fn median(&self) -> Result<Option<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.median().await })
    }

    /// Max interval between consecutive events. `None` if fewer than 2 events.
    /// Fires one RPC.
    pub fn max(&self) -> Result<Option<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.max().await })
    }

    /// Min interval between consecutive events. `None` if fewer than 2 events.
    /// Fires one RPC.
    pub fn min(&self) -> Result<Option<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.min().await })
    }
}
