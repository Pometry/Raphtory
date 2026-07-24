use crate::client::{
    remote_history::{
        RemoteEventTime, RemoteHistory, RemoteHistoryDateTimes, RemoteHistoryEventIds,
        RemoteHistoryTimestamps, RemoteIntervals,
    },
    ClientError,
};
use pyo3::{
    basic::CompareOp,
    exceptions::{PyIndexError, PyValueError},
    pyclass, pymethods,
    types::{PyAnyMethods, PyList},
    Bound, Py, PyAny, PyRef, PyRefMut, PyResult, Python,
};
use raphtory::python::utils::execute_async_task;
use std::sync::Arc;

/// A single event time — mirrors the local `EventTime`. Exposes `timestamp`,
/// `dt` (a real `datetime.datetime`), `event_id`, and `as_tuple`; comparable to
/// ints (by timestamp) and to other event times (by `(timestamp, event_id)`).
///
/// Fields are optional because the server can return null for any of them.
#[derive(Clone)]
#[pyclass(name = "RemoteEventTime", module = "raphtory.graphql")]
pub struct PyRemoteEventTime {
    /// The event's timestamp in the graph's native time unit.
    pub timestamp: Option<i64>,
    /// RFC 3339 datetime string for the event (the on-wire form; the `dt`
    /// getter parses this into a Python `datetime`).
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

    /// The event's timestamp in the graph's native time unit (`None` if absent).
    #[getter]
    fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// The event's timestamp in the graph's native time unit — alias of
    /// `timestamp`, mirroring the local `EventTime.t`. (`None` if absent.)
    #[getter]
    fn t(&self) -> Option<i64> {
        self.timestamp
    }

    /// The event's internal id used to order events within a timestamp
    /// (`None` if absent).
    #[getter]
    fn event_id(&self) -> Option<i64> {
        self.event_id
    }

    /// The UTC `datetime` for this event (`None` if the event has no
    /// datetime). Mirrors the local `EventTime.dt`.
    #[getter]
    fn dt(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        use pyo3::IntoPyObject;
        match &self.dt {
            Some(s) => {
                let parsed = chrono::DateTime::parse_from_rfc3339(s)
                    .map_err(|e| PyValueError::new_err(format!("invalid datetime {s:?}: {e}")))?;
                Ok(Some(parsed.into_pyobject(py)?.into_any().unbind()))
            }
            None => Ok(None),
        }
    }

    /// `(timestamp, event_id)` — mirrors the local `EventTime.as_tuple`.
    /// `None` if either component is absent.
    #[getter]
    fn as_tuple(&self) -> Option<(i64, i64)> {
        match (self.timestamp, self.event_id) {
            (Some(t), Some(e)) => Some((t, e)),
            _ => None,
        }
    }

    /// The timestamp as an int (drops `event_id`) — mirrors the local
    /// `EventTime.__int__`, so `int(node.earliest_time)` works.
    fn __int__(&self) -> PyResult<i64> {
        self.timestamp
            .ok_or_else(|| PyValueError::new_err("EventTime has no timestamp"))
    }

    fn __hash__(&self) -> u64 {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };
        let mut h = DefaultHasher::new();
        self.timestamp.hash(&mut h);
        self.event_id.hash(&mut h);
        h.finish()
    }

    /// Compare like the local `EventTime`: against another event time by
    /// `(timestamp, event_id)`, or against a bare `int` by timestamp (so
    /// `node.earliest_time == 5` works). Returns `NotImplemented` for other
    /// types, letting Python fall back / raise as usual.
    fn __richcmp__(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
    ) -> PyResult<Py<PyAny>> {
        use pyo3::IntoPyObject;
        let ordering = if let Ok(o) = other.extract::<PyRef<'_, PyRemoteEventTime>>() {
            (self.timestamp, self.event_id).cmp(&(o.timestamp, o.event_id))
        } else if let Ok(o) = other.extract::<i64>() {
            match self.timestamp {
                Some(t) => t.cmp(&o),
                None => return Ok(py.NotImplemented()),
            }
        } else {
            return Ok(py.NotImplemented());
        };
        Ok(op
            .matches(ordering)
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind())
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
    ///   Optional[RemoteEventTime]: the earliest event time, or None.
    pub fn earliest_time(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        Ok(
            execute_async_task(move || async move { history.earliest_time().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// Latest event time in this history — `None` if empty. Fires one RPC.
    ///
    /// Returns:
    ///   Optional[RemoteEventTime]: the latest event time, or None.
    pub fn latest_time(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        Ok(
            execute_async_task(move || async move { history.latest_time().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// All events in this history in ascending time order. Fires one RPC.
    ///
    /// Returns:
    ///   list[RemoteEventTime]: one event per entry.
    pub fn collect(&self) -> Result<Vec<PyRemoteEventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        let result = execute_async_task(move || async move { history.collect().await })?;
        Ok(result.into_iter().map(Into::into).collect())
    }

    /// All events in this history in descending time order. Fires one RPC.
    ///
    /// Returns:
    ///   list[RemoteEventTime]: one event per entry.
    pub fn collect_rev(&self) -> Result<Vec<PyRemoteEventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        let result = execute_async_task(move || async move { history.collect_rev().await })?;
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
    /// via `.collect()`, then yields each `RemoteEventTime` locally.
    fn __iter__(&self) -> Result<PyRemoteHistoryIter, ClientError> {
        let list = self.collect()?;
        Ok(PyRemoteHistoryIter {
            inner: list.into_iter(),
        })
    }

    /// `len(history)` — number of events. Fires one RPC (`count()`).
    fn __len__(&self) -> Result<usize, ClientError> {
        Ok(self.count()? as usize)
    }

    /// `history[i]` — the i-th event in ascending time order. Supports
    /// negative indices. Raises `IndexError` if out of range. Fires one RPC
    /// (`collect()`).
    fn __getitem__(&self, index: isize) -> PyResult<PyRemoteEventTime> {
        let events = self.collect()?;
        let len = events.len() as isize;
        let idx = if index < 0 { index + len } else { index };
        if idx < 0 || idx >= len {
            return Err(PyIndexError::new_err(format!(
                "Index {index} out of bounds"
            )));
        }
        Ok(events[idx as usize].clone())
    }

    /// `item in history` — whether an event equal to `item` is present.
    /// `item` may be a `RemoteEventTime` (compared by `(timestamp, event_id)`)
    /// or a bare `int` (compared by timestamp), mirroring the local
    /// `History.__contains__`. Fires one RPC (`collect()`).
    fn __contains__(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> PyResult<bool> {
        let events = self.collect()?;
        for e in events {
            let obj = Py::new(py, e)?;
            if obj.bind(py).eq(item)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// `reversed(history)` — iterate events in descending time order.
    /// Fires one RPC (`collect_rev()`), then yields each locally.
    fn __reversed__(&self) -> Result<PyRemoteHistoryIter, ClientError> {
        let list = self.collect_rev()?;
        Ok(PyRemoteHistoryIter {
            inner: list.into_iter(),
        })
    }

    /// A new history with the iteration order of its entries reversed.
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///   RemoteHistory: the reversed history.
    pub fn reverse(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.history.reverse())
    }

    /// Sub-container: timestamps view of this history (plain int timestamps).
    /// Lazy — no RPC.
    #[getter]
    pub fn timestamps(&self) -> PyRemoteHistoryTimestamps {
        PyRemoteHistoryTimestamps {
            inner: Arc::new(self.history.timestamps()),
        }
    }

    /// Timestamps view of this history — alias of `timestamps`, mirroring the
    /// local `History.t`. Lazy — no RPC.
    #[getter]
    pub fn t(&self) -> PyRemoteHistoryTimestamps {
        self.timestamps()
    }

    /// Datetime view of this history — alias of `datetimes`, mirroring the
    /// local `History.dt`. Lazy — no RPC.
    #[getter]
    pub fn dt(&self) -> PyRemoteHistoryDateTimes {
        self.datetimes()
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
#[pyclass(
    name = "RemoteHistoryTimestamps",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemoteHistoryTimestamps {
    pub(crate) inner: Arc<RemoteHistoryTimestamps>,
}

#[pymethods]
impl PyRemoteHistoryTimestamps {
    /// Fires one RPC.
    pub fn collect(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect().await })
    }

    /// Fires one RPC.
    pub fn collect_rev(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect_rev().await })
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
        execute_async_task(move || async move { inner.page_rev(limit, offset, page_index).await })
    }

    /// All timestamps as a `list[int]` — alias of `collect()`, mirroring the
    /// local `HistoryTimestamp.to_list`. Fires one RPC.
    pub fn to_list(&self) -> Result<Vec<i64>, ClientError> {
        self.collect()
    }

    /// All timestamps as a `list[int]` in reverse order — alias of
    /// `collect_rev()`, mirroring the local `HistoryTimestamp.to_list_rev`.
    /// Fires one RPC.
    pub fn to_list_rev(&self) -> Result<Vec<i64>, ClientError> {
        self.collect_rev()
    }

    /// `len(...)` — number of timestamps. Fires one RPC (`collect()`).
    fn __len__(&self) -> Result<usize, ClientError> {
        Ok(self.collect()?.len())
    }

    /// `x[i]` — the i-th timestamp. Supports negative indices; raises
    /// `IndexError` if out of range. Fires one RPC (`collect()`).
    fn __getitem__(&self, index: isize) -> PyResult<i64> {
        let items = self.collect()?;
        index_i64(&items, index)
    }

    /// `for x in ...` — iterate timestamps. Fires one RPC (`collect()`).
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }

    /// `item in ...` — membership test. Fires one RPC (`collect()`).
    fn __contains__(&self, item: i64) -> Result<bool, ClientError> {
        Ok(self.collect()?.contains(&item))
    }

    /// `reversed(...)` — iterate timestamps in reverse. Fires one RPC
    /// (`collect_rev()`).
    fn __reversed__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect_rev()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }
}

/// Event-id view of a `RemoteHistory`. Lists / pages return `list[int]`.
#[derive(Clone)]
#[pyclass(
    name = "RemoteHistoryEventIds",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemoteHistoryEventIds {
    pub(crate) inner: Arc<RemoteHistoryEventIds>,
}

#[pymethods]
impl PyRemoteHistoryEventIds {
    /// Fires one RPC.
    pub fn collect(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect().await })
    }

    /// Fires one RPC.
    pub fn collect_rev(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect_rev().await })
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
        execute_async_task(move || async move { inner.page_rev(limit, offset, page_index).await })
    }

    /// All event ids as a `list[int]` — alias of `collect()`, mirroring the
    /// local `HistoryEventId.to_list`. Fires one RPC.
    pub fn to_list(&self) -> Result<Vec<i64>, ClientError> {
        self.collect()
    }

    /// All event ids as a `list[int]` in reverse order — alias of
    /// `collect_rev()`, mirroring the local `HistoryEventId.to_list_rev`.
    /// Fires one RPC.
    pub fn to_list_rev(&self) -> Result<Vec<i64>, ClientError> {
        self.collect_rev()
    }

    /// `len(...)` — number of event ids. Fires one RPC (`collect()`).
    fn __len__(&self) -> Result<usize, ClientError> {
        Ok(self.collect()?.len())
    }

    /// `x[i]` — the i-th event id. Supports negative indices; raises
    /// `IndexError` if out of range. Fires one RPC (`collect()`).
    fn __getitem__(&self, index: isize) -> PyResult<i64> {
        let items = self.collect()?;
        index_i64(&items, index)
    }

    /// `for x in ...` — iterate event ids. Fires one RPC (`collect()`).
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }

    /// `item in ...` — membership test. Fires one RPC (`collect()`).
    fn __contains__(&self, item: i64) -> Result<bool, ClientError> {
        Ok(self.collect()?.contains(&item))
    }

    /// `reversed(...)` — iterate event ids in reverse. Fires one RPC
    /// (`collect_rev()`).
    fn __reversed__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect_rev()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }
}

/// Datetime view of a `RemoteHistory`. Lists / pages return `list[str]`
/// (RFC 3339 formatted).
#[derive(Clone)]
#[pyclass(
    name = "RemoteHistoryDateTimes",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemoteHistoryDateTimes {
    pub(crate) inner: Arc<RemoteHistoryDateTimes>,
}

#[pymethods]
impl PyRemoteHistoryDateTimes {
    /// Fires one RPC.
    pub fn collect(&self) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect().await })
    }

    /// Fires one RPC.
    pub fn collect_rev(&self) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect_rev().await })
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
        execute_async_task(move || async move { inner.page_rev(limit, offset, page_index).await })
    }

    /// `len(...)` — number of datetimes. Fires one RPC (`collect()`).
    fn __len__(&self) -> Result<usize, ClientError> {
        Ok(self.collect()?.len())
    }

    /// `x[i]` — the i-th datetime (RFC 3339 string). Supports negative
    /// indices; raises `IndexError` if out of range. Fires one RPC
    /// (`collect()`).
    fn __getitem__(&self, index: isize) -> PyResult<String> {
        let items = self.collect()?;
        let len = items.len() as isize;
        let idx = if index < 0 { index + len } else { index };
        if idx < 0 || idx >= len {
            return Err(PyIndexError::new_err(format!(
                "Index {index} out of bounds"
            )));
        }
        Ok(items[idx as usize].clone())
    }

    /// `for x in ...` — iterate datetimes. Fires one RPC (`collect()`).
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }

    /// `item in ...` — membership test (against the RFC 3339 string form).
    /// Fires one RPC (`collect()`).
    fn __contains__(&self, item: String) -> Result<bool, ClientError> {
        Ok(self.collect()?.contains(&item))
    }

    /// `reversed(...)` — iterate datetimes in reverse. Fires one RPC
    /// (`collect_rev()`).
    fn __reversed__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect_rev()?)?
            .try_iter()?
            .into_any()
            .unbind())
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
    pub fn collect(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect().await })
    }

    /// Fires one RPC.
    pub fn collect_rev(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect_rev().await })
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
        execute_async_task(move || async move { inner.page_rev(limit, offset, page_index).await })
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

    /// All intervals as a `list[int]` — alias of `collect()`, mirroring the
    /// local `Intervals.to_list`. Fires one RPC.
    pub fn to_list(&self) -> Result<Vec<i64>, ClientError> {
        self.collect()
    }

    /// All intervals as a `list[int]` in reverse order — alias of
    /// `collect_rev()`, mirroring the local `Intervals.to_list_rev`. Fires
    /// one RPC.
    pub fn to_list_rev(&self) -> Result<Vec<i64>, ClientError> {
        self.collect_rev()
    }

    /// `len(...)` — number of intervals. Fires one RPC (`collect()`).
    fn __len__(&self) -> Result<usize, ClientError> {
        Ok(self.collect()?.len())
    }

    /// `x[i]` — the i-th interval. Supports negative indices; raises
    /// `IndexError` if out of range. Fires one RPC (`collect()`).
    fn __getitem__(&self, index: isize) -> PyResult<i64> {
        let items = self.collect()?;
        index_i64(&items, index)
    }

    /// `for x in ...` — iterate intervals. Fires one RPC (`collect()`).
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }

    /// `item in ...` — membership test. Fires one RPC (`collect()`).
    fn __contains__(&self, item: i64) -> Result<bool, ClientError> {
        Ok(self.collect()?.contains(&item))
    }

    /// `reversed(...)` — iterate intervals in reverse. Fires one RPC
    /// (`collect_rev()`).
    fn __reversed__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect_rev()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }
}

/// Shared helper for `__getitem__` on the int-valued sub-collections:
/// resolves a (possibly negative) index into `items`, raising `IndexError`
/// when out of range.
fn index_i64(items: &[i64], index: isize) -> PyResult<i64> {
    let len = items.len() as isize;
    let idx = if index < 0 { index + len } else { index };
    if idx < 0 || idx >= len {
        return Err(PyIndexError::new_err(format!(
            "Index {index} out of bounds"
        )));
    }
    Ok(items[idx as usize])
}
