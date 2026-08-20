use crate::client::{
    remote_history::{
        RemoteHistory, RemoteHistoryDateTimes, RemoteHistoryEventIds, RemoteHistoryTimestamps,
        RemoteIntervals,
    },
    ClientError,
};
use chrono::{DateTime, Utc};
use pyo3::{
    exceptions::PyIndexError,
    pyclass, pymethods,
    types::{PyAnyMethods, PyList},
    Bound, Py, PyAny, PyRef, PyRefMut, PyResult, Python,
};
use raphtory::python::utils::execute_async_task;
use raphtory_api::{
    core::storage::timeindex::{AsTime, EventTime},
    python::timeindex::PyOptionalEventTime,
};
use std::sync::Arc;

/// A handle to the event history of a remote node or edge.
///
/// Returned by [RemoteNode.history][raphtory.graphql.RemoteNode.history] and
/// by [RemoteEdge.history][raphtory.graphql.RemoteEdge.history] /
/// [RemoteEdge.deletions][raphtory.graphql.RemoteEdge.deletions].
///
/// Mirrors the shape of the local Python API's `History` type. Exposes scalar
/// terminals (`count`, `is_empty`, `earliest_time`, `latest_time`), the
/// `collect` / `collect_rev` / `page` / `page_rev` list terminals, and the
/// `.t` / `.dt` / `.event_id` / `.intervals` sub-container accessors.
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
    ///   OptionalEventTime: the earliest event time, or empty.
    pub fn earliest_time(&self) -> Result<PyOptionalEventTime, ClientError> {
        let history = Arc::clone(&self.history);
        Ok(execute_async_task(move || async move { history.earliest_time().await })?.into())
    }

    /// Latest event time in this history — `None` if empty. Fires one RPC.
    ///
    /// Returns:
    ///   OptionalEventTime: the latest event time, or empty.
    pub fn latest_time(&self) -> Result<PyOptionalEventTime, ClientError> {
        let history = Arc::clone(&self.history);
        Ok(execute_async_task(move || async move { history.latest_time().await })?.into())
    }

    /// All events in this history in ascending time order. Fires one RPC.
    ///
    /// Returns:
    ///   list[EventTime]: one event per entry.
    pub fn collect(&self) -> Result<Vec<EventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        let result = execute_async_task(move || async move { history.collect().await })?;
        Ok(result)
    }

    /// All events in this history in descending time order. Fires one RPC.
    ///
    /// Returns:
    ///   list[EventTime]: one event per entry.
    pub fn collect_rev(&self) -> Result<Vec<EventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        let result = execute_async_task(move || async move { history.collect_rev().await })?;
        Ok(result)
    }

    /// A page of events in ascending time order — at most `limit` items,
    /// starting `page_index * limit + offset` items in. Both `offset` and
    /// `page_index` default to 0. Fires one RPC.
    ///
    /// Arguments:
    ///   limit (int): maximum number of events on this page.
    ///   offset (int, optional): additional items to skip.
    ///   page_index (int, optional): 0-based page number.
    ///
    /// Returns:
    ///   list[EventTime]: at most `limit` events.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<EventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        let result =
            execute_async_task(
                move || async move { history.page(limit, offset, page_index).await },
            )?;
        Ok(result)
    }

    /// A page of events in descending time order. Same args as `page()`.
    /// Fires one RPC.
    ///
    /// Arguments:
    ///     limit (int): maximum number of items on this page.
    ///     offset (int, optional): additional items to skip.
    ///     page_index (int, optional): 0-based page number.
    ///
    /// Returns:
    ///     list[EventTime]: at most `limit` events.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<EventTime>, ClientError> {
        let history = Arc::clone(&self.history);
        let result = execute_async_task(move || async move {
            history.page_rev(limit, offset, page_index).await
        })?;
        Ok(result)
    }

    /// Enables `for t in remote_history:` — fetches all events in one RPC
    /// via `.collect()`, then yields each `EventTime` locally.
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
    /// negative indices. Raises `IndexError` if out of range. Fires ONE small
    /// RPC (`page`/`page_rev` with `limit=1`) — fetches just the requested
    /// element, not the whole history.
    fn __getitem__(&self, index: isize) -> PyResult<EventTime> {
        let (rev, offset) = page_offset(index);
        let page = if rev {
            self.page_rev(1, Some(offset), None)?
        } else {
            self.page(1, Some(offset), None)?
        };
        single_from_page(page, index)
    }

    /// `item in history` — whether an event equal to `item` is present,
    /// compared by `(timestamp, event_id)` exactly as locally: anything that
    /// converts to an `EventTime` is accepted (a bare `int` becomes
    /// `(t, event_id=0)`), anything else raises. Fires one small RPC — the
    /// membership check runs server-side; no events travel.
    fn __contains__(&self, item: EventTime) -> PyResult<bool> {
        // The same signature as the local `History.__contains__`: the
        // `EventTime` extraction defines what an item may be (an `EventTime`,
        // or an int/float/str/datetime that converts to one — a bare int
        // becomes `(t, event_id=0)`) and what fails with a `TypeError`.
        let history = Arc::clone(&self.history);
        let (timestamp, event_id) = (item.t(), item.i());
        Ok(execute_async_task(move || async move {
            history.contains(timestamp, Some(event_id)).await
        })?)
    }

    /// `reversed(history)` — iterate events in descending time order.
    /// Routed through the lazy `reverse()` handle, so it stays a single RPC
    /// today and picks up any future optimised iterator automatically.
    fn __reversed__(&self) -> Result<PyRemoteHistoryIter, ClientError> {
        self.reverse().__iter__()
    }

    /// A new history with the iteration order of its entries reversed.
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///   RemoteHistory: the reversed history.
    pub fn reverse(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.history.reverse())
    }

    /// Timestamps view of this history (plain int timestamps), mirroring the
    /// local `History.t`. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteHistoryTimestamps: the timestamps view of this history.
    #[getter]
    pub fn t(&self) -> PyRemoteHistoryTimestamps {
        PyRemoteHistoryTimestamps {
            inner: Arc::new(self.history.timestamps()),
        }
    }

    /// Datetime view of this history (RFC 3339 strings), mirroring the
    /// local `History.dt`. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteHistoryDateTimes: the datetimes view of this history.
    #[getter]
    pub fn dt(&self) -> PyRemoteHistoryDateTimes {
        PyRemoteHistoryDateTimes {
            inner: Arc::new(self.history.datetimes()),
        }
    }

    /// Sub-container: event-id view of this history. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteHistoryEventIds: the event-ids view of this history.
    #[getter]
    pub fn event_id(&self) -> PyRemoteHistoryEventIds {
        PyRemoteHistoryEventIds {
            inner: Arc::new(self.history.event_id()),
        }
    }

    /// Sub-container: inter-event intervals view of this history. Adds
    /// stats terminals (mean/median/max/min). Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteIntervals: the intervals view of this history.
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
    inner: std::vec::IntoIter<EventTime>,
}

#[pymethods]
impl PyRemoteHistoryIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<EventTime> {
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
    ///
    /// Returns:
    ///     list[int]: all timestamps in ascending time order.
    pub fn collect(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect().await })
    }

    /// Fires one RPC.
    ///
    /// Returns:
    ///     list[int]: all timestamps in descending time order.
    pub fn collect_rev(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect_rev().await })
    }

    /// Fires one RPC.
    ///
    /// Arguments:
    ///     limit (int): maximum number of items on this page.
    ///     offset (int, optional): additional items to skip.
    ///     page_index (int, optional): 0-based page number.
    ///
    /// Returns:
    ///     list[int]: at most `limit` timestamps, in ascending time order.
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
    ///
    /// Arguments:
    ///     limit (int): maximum number of items on this page.
    ///     offset (int, optional): additional items to skip.
    ///     page_index (int, optional): 0-based page number.
    ///
    /// Returns:
    ///     list[int]: at most `limit` timestamps, in descending time order.
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

    /// `len(...)` — number of timestamps. Fires one RPC — the count comes
    /// back alone, not the values.
    fn __len__(&self) -> Result<usize, ClientError> {
        let inner = Arc::clone(&self.inner);
        Ok(execute_async_task(move || async move { inner.count().await })? as usize)
    }

    /// `x[i]` — the i-th timestamp. Supports negative indices; raises
    /// `IndexError` if out of range. Fires ONE small RPC (`page`/`page_rev`
    /// with `limit=1`) — fetches just the requested element.
    fn __getitem__(&self, index: isize) -> PyResult<i64> {
        let (rev, offset) = page_offset(index);
        let page = if rev {
            self.page_rev(1, Some(offset), None)?
        } else {
            self.page(1, Some(offset), None)?
        };
        single_from_page(page, index)
    }

    /// `for x in ...` — iterate timestamps. Fires one RPC (`collect()`).
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }

    /// `item in ...` — membership test. Fires one RPC — the check runs
    /// server-side; no values travel.
    fn __contains__(&self, item: i64) -> Result<bool, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.contains(item).await })
    }

    /// `reversed(...)` — iterate timestamps in reverse. Routed through the
    /// lazy `reverse()` view, so it stays one RPC and composes with any
    /// future optimised iterator.
    fn __reversed__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reversed = Self {
            inner: Arc::new(self.inner.reverse()),
        };
        reversed.__iter__(py)
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
    ///
    /// Returns:
    ///     list[int]: all event ids in ascending time order.
    pub fn collect(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect().await })
    }

    /// Fires one RPC.
    ///
    /// Returns:
    ///     list[int]: all event ids in descending time order.
    pub fn collect_rev(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect_rev().await })
    }

    /// Fires one RPC.
    ///
    /// Arguments:
    ///     limit (int): maximum number of items on this page.
    ///     offset (int, optional): additional items to skip.
    ///     page_index (int, optional): 0-based page number.
    ///
    /// Returns:
    ///     list[int]: at most `limit` event ids, in ascending time order.
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
    ///
    /// Arguments:
    ///     limit (int): maximum number of items on this page.
    ///     offset (int, optional): additional items to skip.
    ///     page_index (int, optional): 0-based page number.
    ///
    /// Returns:
    ///     list[int]: at most `limit` event ids, in descending time order.
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

    /// `len(...)` — number of event ids. Fires one RPC — the count comes
    /// back alone, not the values.
    fn __len__(&self) -> Result<usize, ClientError> {
        let inner = Arc::clone(&self.inner);
        Ok(execute_async_task(move || async move { inner.count().await })? as usize)
    }

    /// `x[i]` — the i-th event id. Supports negative indices; raises
    /// `IndexError` if out of range. Fires ONE small RPC (`page`/`page_rev`
    /// with `limit=1`) — fetches just the requested element.
    fn __getitem__(&self, index: isize) -> PyResult<i64> {
        let (rev, offset) = page_offset(index);
        let page = if rev {
            self.page_rev(1, Some(offset), None)?
        } else {
            self.page(1, Some(offset), None)?
        };
        single_from_page(page, index)
    }

    /// `for x in ...` — iterate event ids. Fires one RPC (`collect()`).
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }

    /// `item in ...` — membership test. Fires one RPC — the check runs
    /// server-side; no values travel.
    fn __contains__(&self, item: i64) -> Result<bool, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.contains(item).await })
    }

    /// `reversed(...)` — iterate event ids in reverse. Routed through the
    /// lazy `reverse()` view, so it stays one RPC and composes with any
    /// future optimised iterator.
    fn __reversed__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reversed = Self {
            inner: Arc::new(self.inner.reverse()),
        };
        reversed.__iter__(py)
    }
}

/// Datetime view of a `RemoteHistory`. Lists / pages return `list[datetime]`
/// (UTC), mirroring the local `History.dt`.
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
    ///
    /// Returns:
    ///     list[datetime]: all datetimes (UTC) in ascending time order.
    pub fn collect(&self) -> Result<Vec<DateTime<Utc>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect().await })
    }

    /// Fires one RPC.
    ///
    /// Returns:
    ///     list[datetime]: all datetimes (UTC) in descending time order.
    pub fn collect_rev(&self) -> Result<Vec<DateTime<Utc>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect_rev().await })
    }

    /// Fires one RPC.
    ///
    /// Arguments:
    ///     limit (int): maximum number of items on this page.
    ///     offset (int, optional): additional items to skip.
    ///     page_index (int, optional): 0-based page number.
    ///
    /// Returns:
    ///     list[datetime]: at most `limit` datetimes (UTC), in ascending time order.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<DateTime<Utc>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.page(limit, offset, page_index).await })
    }

    /// Fires one RPC.
    ///
    /// Arguments:
    ///     limit (int): maximum number of items on this page.
    ///     offset (int, optional): additional items to skip.
    ///     page_index (int, optional): 0-based page number.
    ///
    /// Returns:
    ///     list[datetime]: at most `limit` datetimes (UTC), in descending time order.
    #[pyo3(signature = (limit, offset = None, page_index = None))]
    pub fn page_rev(
        &self,
        limit: usize,
        offset: Option<usize>,
        page_index: Option<usize>,
    ) -> Result<Vec<DateTime<Utc>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.page_rev(limit, offset, page_index).await })
    }

    /// `len(...)` — number of datetimes. Fires one RPC — the count comes
    /// back alone, not the values.
    fn __len__(&self) -> Result<usize, ClientError> {
        let inner = Arc::clone(&self.inner);
        Ok(execute_async_task(move || async move { inner.count().await })? as usize)
    }

    /// `x[i]` — the i-th datetime. Supports negative indices; raises
    /// `IndexError` if out of range. Fires ONE small RPC (`page`/`page_rev`
    /// with `limit=1`) — fetches just the requested element.
    fn __getitem__(&self, index: isize) -> PyResult<DateTime<Utc>> {
        let (rev, offset) = page_offset(index);
        let page = if rev {
            self.page_rev(1, Some(offset), None)?
        } else {
            self.page(1, Some(offset), None)?
        };
        single_from_page(page, index)
    }

    /// `for x in ...` — iterate datetimes. Fires one RPC (`collect()`).
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }

    /// `item in ...` — membership test against the datetimes. Fires one RPC —
    /// the check runs server-side on the underlying timestamp. Anything that
    /// isn't a UTC-convertible datetime is simply not a member (returns
    /// `False`), matching Python's `in` — rather than raising on a naive
    /// datetime or a string.
    fn __contains__(&self, item: &Bound<'_, PyAny>) -> Result<bool, ClientError> {
        match item.extract::<DateTime<Utc>>() {
            Ok(dt) => {
                let inner = Arc::clone(&self.inner);
                let ms = dt.timestamp_millis();
                execute_async_task(move || async move { inner.contains_ms(ms).await })
            }
            Err(_) => Ok(false),
        }
    }

    /// `reversed(...)` — iterate datetimes in reverse. Routed through the
    /// lazy `reverse()` view, so it stays one RPC and composes with any
    /// future optimised iterator.
    fn __reversed__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reversed = Self {
            inner: Arc::new(self.inner.reverse()),
        };
        reversed.__iter__(py)
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
    ///
    /// Returns:
    ///     list[int]: all intervals in ascending time order.
    pub fn collect(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect().await })
    }

    /// Fires one RPC.
    ///
    /// Returns:
    ///     list[int]: all intervals in descending time order.
    pub fn collect_rev(&self) -> Result<Vec<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.collect_rev().await })
    }

    /// Fires one RPC.
    ///
    /// Arguments:
    ///     limit (int): maximum number of items on this page.
    ///     offset (int, optional): additional items to skip.
    ///     page_index (int, optional): 0-based page number.
    ///
    /// Returns:
    ///     list[int]: at most `limit` intervals, in ascending time order.
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
    ///
    /// Arguments:
    ///     limit (int): maximum number of items on this page.
    ///     offset (int, optional): additional items to skip.
    ///     page_index (int, optional): 0-based page number.
    ///
    /// Returns:
    ///     list[int]: at most `limit` intervals, in descending time order.
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
    ///
    /// Returns:
    ///     Optional[float]: the mean interval, or `None` if fewer than 2 events.
    pub fn mean(&self) -> Result<Option<f64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.mean().await })
    }

    /// Median interval between consecutive events. `None` if fewer than 2 events.
    /// Fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the median interval, or `None` if fewer than 2 events.
    pub fn median(&self) -> Result<Option<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.median().await })
    }

    /// Max interval between consecutive events. `None` if fewer than 2 events.
    /// Fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the largest interval, or `None` if fewer than 2 events.
    pub fn max(&self) -> Result<Option<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.max().await })
    }

    /// Min interval between consecutive events. `None` if fewer than 2 events.
    /// Fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the smallest interval, or `None` if fewer than 2 events.
    pub fn min(&self) -> Result<Option<i64>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.min().await })
    }

    /// `len(...)` — number of intervals. Fires one RPC — the count comes
    /// back alone, not the values.
    fn __len__(&self) -> Result<usize, ClientError> {
        let inner = Arc::clone(&self.inner);
        Ok(execute_async_task(move || async move { inner.count().await })? as usize)
    }

    /// `x[i]` — the i-th interval. Supports negative indices; raises
    /// `IndexError` if out of range. Fires ONE small RPC (`page`/`page_rev`
    /// with `limit=1`) — fetches just the requested element.
    fn __getitem__(&self, index: isize) -> PyResult<i64> {
        let (rev, offset) = page_offset(index);
        let page = if rev {
            self.page_rev(1, Some(offset), None)?
        } else {
            self.page(1, Some(offset), None)?
        };
        single_from_page(page, index)
    }

    /// `for x in ...` — iterate intervals. Fires one RPC (`collect()`).
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.collect()?)?
            .try_iter()?
            .into_any()
            .unbind())
    }

    /// `item in ...` — membership test. Fires one RPC — the check runs
    /// server-side; no values travel.
    fn __contains__(&self, item: i64) -> Result<bool, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.contains(item).await })
    }

    /// `reversed(...)` — iterate intervals in reverse. Routed through the
    /// lazy `reverse()` view, so it stays one RPC and composes with any
    /// future optimised iterator.
    fn __reversed__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reversed = Self {
            inner: Arc::new(self.inner.reverse()),
        };
        reversed.__iter__(py)
    }
}

/// Shared helper for `__getitem__` on the int-valued sub-collections:
/// resolves a (possibly negative) index into `items`, raising `IndexError`
/// when out of range.
/// Map a Python index to a single-element page fetch: `(use_page_rev, offset)`.
/// A non-negative index is the `offset`-th item from the front (`page`); a
/// negative index is the `offset`-th from the end (`page_rev`, so `[-1]` costs
/// one RPC with no `count()`).
fn page_offset(index: isize) -> (bool, usize) {
    if index >= 0 {
        (false, index as usize)
    } else {
        // unsigned_abs (not negation) so isize::MIN can't overflow; the -1
        // can't underflow since index < 0 guarantees unsigned_abs() >= 1.
        (true, index.unsigned_abs() - 1)
    }
}

/// Unwrap the single element of a `page(1, …)` result, mapping an empty page
/// (index past the end) to `IndexError` — matching the old `collect()`-based
/// bounds check.
fn single_from_page<T>(page: Vec<T>, index: isize) -> PyResult<T> {
    page.into_iter()
        .next()
        .ok_or_else(|| PyIndexError::new_err(format!("Index {index} out of bounds")))
}
