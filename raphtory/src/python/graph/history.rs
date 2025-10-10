use crate::{
    db::api::view::history::*,
    python::{
        types::{
            iterable::{FromIterable, Iterable},
            repr::{iterator_repr, Repr},
            wrappers::iterators::PyBorrowingIterator,
        },
        utils::{PyGenericIterator, PyNestedGenericIterator},
    },
};
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use numpy::{IntoPyArray, Ix1, PyArray};
use pyo3::prelude::*;
use raphtory_api::{
    core::storage::timeindex::{AsTime, EventTime, TimeError},
    iter::{BoxedIter, BoxedLIter, IntoDynBoxed},
    python::timeindex::EventTimeComponent,
};
use raphtory_core::utils::iter::GenLockedIter;
use std::sync::Arc;

impl Repr for EventTime {
    fn repr(&self) -> String {
        self.to_string()
    }
}

/// History of updates for an object. Provides access to time entries and derived views such as timestamps, datetimes, event ids, and intervals.
#[pyclass(name = "History", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyHistory {
    history: History<'static, Arc<dyn InternalHistoryOps>>,
}

impl<'a, T: InternalHistoryOps> Repr for History<'a, T> {
    fn repr(&self) -> String {
        format!("History({})", iterator_repr(self.iter()))
    }
}

impl PyHistory {
    pub fn new(history: History<'static, Arc<dyn InternalHistoryOps>>) -> PyHistory {
        PyHistory { history }
    }
}

#[pymethods]
impl PyHistory {
    /// Compose multiple History objects into a single History by fusing their time entries in chronological order.
    ///
    /// Arguments:
    ///     objects (Iterable[History]): History objects to compose.
    ///
    /// Returns:
    ///     History: Composed History object containing entries from all inputs.
    #[staticmethod]
    pub fn compose_histories(objects: FromIterable<PyHistory>) -> Self {
        // the only way to get History objects from python is if they are already Arc<...> because that's what PyHistory's inner field holds to make sure it's Send + Sync
        let underlying_objects: Vec<Arc<dyn InternalHistoryOps>> = objects
            .into_iter()
            .map(|obj| obj.history.0.clone())
            .collect();
        Self {
            history: History::new(Arc::new(CompositeHistory::new(underlying_objects))),
        }
    }

    // TODO: Ideally we want one of compose_histories/merge. We want to see where the performance benefits shift from one to the other and automatically use that.
    /// Merge this History with another by interleaving entries in time order.
    ///
    /// Arguments:
    ///     other (History): Right-hand history to merge.
    ///
    /// Returns:
    ///     History: Merged history containing entries from both inputs.
    pub fn merge(&self, other: &Self) -> Self {
        // Clones the Arcs, we end up with Arc<Arc<dyn InternalHistoryOps>>, 1 level of indirection. Cloning the underlying InternalHistoryOps objects introduces lifetime issues.
        Self {
            history: History::new(Arc::new(MergedHistory::new(
                self.history.0.clone(),
                other.history.0.clone(),
            ))),
        }
    }

    /// Return a History where iteration order is reversed.
    ///
    /// Returns:
    ///     History: History that yields items in reverse chronological order.
    pub fn reverse(&self) -> Self {
        // Clones the Arcs, we end up with Arc<Arc<dyn InternalHistoryOps>>, 1 level of indirection. Cloning the underlying InternalHistoryOps objects introduces lifetime issues.
        PyHistory {
            history: History::new(Arc::new(ReversedHistoryOps::new(self.history.0.clone()))),
        }
    }

    /// Access history events as timestamps (milliseconds since Unix the epoch).
    ///
    /// Returns:
    ///     HistoryTimestamp: Timestamp (as int) view of this history.
    #[getter]
    pub fn t(&self) -> PyHistoryTimestamp {
        PyHistoryTimestamp {
            history_t: HistoryTimestamp::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }

    /// Access history events as UTC datetimes.
    ///
    /// Returns:
    ///     HistoryDateTime: Datetime view of this history.
    #[getter]
    pub fn dt(&self) -> PyHistoryDateTime {
        PyHistoryDateTime {
            history_dt: HistoryDateTime::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }

    /// Access the unique event id of each time entry.
    ///
    /// Returns:
    ///     HistoryEventId: Event id view of this history.
    #[getter]
    pub fn event_id(&self) -> PyHistoryEventId {
        PyHistoryEventId {
            history_s: HistoryEventId::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }

    /// Access the intervals between consecutive timestamps in milliseconds.
    ///
    /// Returns:
    ///     Intervals: Intervals view of this history.
    #[getter]
    pub fn intervals(&self) -> PyIntervals {
        PyIntervals {
            intervals: Intervals::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }

    /// Get the earliest time entry.
    ///
    /// Returns:
    ///     Optional[EventTime]: Earliest time entry, or None if empty.
    pub fn earliest_time(&self) -> Option<EventTime> {
        self.history.earliest_time()
    }

    /// Get the latest time entry.
    ///
    /// Returns:
    ///     Optional[EventTime]: Latest time entry, or None if empty.
    pub fn latest_time(&self) -> Option<EventTime> {
        self.history.latest_time()
    }

    /// Collect all time entries in chronological order.
    ///
    /// Returns:
    ///     List[EventTime]: Collected time entries.
    pub fn collect(&self) -> Vec<EventTime> {
        self.history.collect()
    }

    /// Collect all time entries in reverse chronological order.
    ///
    /// Returns:
    ///     List[EventTime]: Collected time entries in reverse order.
    pub fn collect_rev(&self) -> Vec<EventTime> {
        self.history.collect_rev()
    }

    /// Iterate over all time entries in chronological order.
    ///
    /// Returns:
    ///     Iterator[EventTime]: Iterator over time entries.
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history.clone(),
            History<'static, Arc<dyn InternalHistoryOps>>,
            |history| history.iter()
        )
    }

    /// Iterate over all time entries in reverse chronological order.
    ///
    /// Returns:
    ///     Iterator[EventTime]: Iterator over time entries in reverse order.
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history.clone(),
            History<'static, Arc<dyn InternalHistoryOps>>,
            |history| history.iter_rev()
        )
    }

    /// Return the string representation.
    ///
    /// Returns:
    ///     str: String representation.
    pub fn __repr__(&self) -> String {
        self.history.repr()
    }

    /// Check if this History object contains a time entry.
    ///
    /// Arguments:
    ///     item (EventTime): Time entry to check.
    ///
    /// Returns:
    ///     bool: True if present, otherwise False.
    fn __contains__(&self, item: EventTime) -> bool {
        self.history.iter().any(|x| x == item)
    }

    /// Compare equality with another History or a list of EventTime.
    ///
    /// Arguments:
    ///     other (History | List[EventTime]): The item to compare equality with.
    ///
    /// Returns:
    ///     bool: True if equal, otherwise False.
    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        if let Ok(py_hist) = other.downcast::<PyHistory>() {
            return self.history.eq(&py_hist.get().history);
        }
        // compare timestamps only
        if let Ok(list) = other.extract::<Vec<EventTimeComponent>>() {
            return self
                .history
                .iter()
                .map(|t| t.t())
                .eq(list.into_iter().map(|c| c.t()));
        }
        if let Ok(list) = other.extract::<Vec<EventTime>>() {
            return self.history.iter().eq(list.into_iter());
        }
        false
    }

    /// Compare inequality with another History or a list of EventTime.
    ///
    /// Arguments:
    ///     other (History | List[EventTime]): The item to compare inequality with.
    ///
    /// Returns:
    ///     bool: True if not equal, otherwise False.
    fn __ne__(&self, other: &Bound<PyAny>) -> bool {
        !self.__eq__(other)
    }

    /// Check whether the history has no entries.
    ///
    /// Returns:
    ///     bool: True if empty, otherwise False.
    pub fn is_empty(&self) -> bool {
        self.history.is_empty()
    }

    /// Return the number of time entries.
    ///
    /// Returns:
    ///     int: Number of entries.
    pub fn __len__(&self) -> usize {
        self.history.len()
    }
}

impl<T: IntoArcDynHistoryOps> From<History<'_, T>> for PyHistory {
    fn from(history: History<T>) -> Self {
        let arc_ops: Arc<dyn InternalHistoryOps> = history.0.into_arc_dyn();
        Self {
            history: History::new(arc_ops),
        }
    }
}

impl<'py, T: IntoArcDynHistoryOps> IntoPyObject<'py> for History<'_, T> {
    type Target = PyHistory;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyHistory::from(self).into_pyobject(py)
    }
}

impl<'py> FromPyObject<'py> for History<'static, Arc<dyn InternalHistoryOps>> {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let py_history = ob.downcast::<PyHistory>()?;
        Ok(py_history.get().history.clone())
    }
}

/// History view that exposes timestamps in milliseconds since the Unix epoch.
#[pyclass(name = "HistoryTimestamp", module = "raphtory", frozen)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyHistoryTimestamp {
    history_t: HistoryTimestamp<Arc<dyn InternalHistoryOps>>,
}

#[pymethods]
impl PyHistoryTimestamp {
    /// Collect all timestamps into a numpy ndarray.
    ///
    /// Returns:
    ///     NDArray[np.int64]: Timestamps in milliseconds since the Unix epoch.
    pub fn collect<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let t = self.history_t.collect();
        t.into_pyarray(py)
    }

    /// Collect all timestamps into a numpy ndarray in reverse order.
    ///
    /// Returns:
    ///     NDArray[np.int64]: Timestamps in milliseconds since the Unix epoch in reverse order.
    pub fn collect_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let t = self.history_t.collect_rev();
        t.into_pyarray(py)
    }

    /// Iterate over all timestamps.
    ///
    /// Returns:
    ///     Iterator[int]: Iterator over timestamps in milliseconds since the Unix epoch.
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_t.clone(),
            HistoryTimestamp<Arc<dyn InternalHistoryOps>>,
            |history_t| history_t.iter()
        )
    }

    /// Iterate over all timestamps in reverse order.
    ///
    /// Returns:
    ///     Iterator[int]: Iterator over timestamps (milliseconds since the Unix epoch) in reverse order.
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_t.clone(),
            HistoryTimestamp<Arc<dyn InternalHistoryOps>>,
            |history_t| history_t.iter_rev()
        )
    }

    /// Check if this HistoryTimestamp object contains a timestamp.
    ///
    /// Arguments:
    ///     item (int): Timestamp in milliseconds since the Unix epoch.
    ///
    /// Returns:
    ///     bool: True if present, otherwise False.
    fn __contains__(&self, item: i64) -> bool {
        self.history_t.iter().any(|x| x == item)
    }

    /// Compare equality with another HistoryTimestamp or with a list of integers.
    ///
    /// Arguments:
    ///     other (HistoryTimestamp | List[int]): The item to compare equality with.
    ///
    /// Returns:
    ///     bool: True if equal, otherwise False.
    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        if let Ok(py_hist) = other.downcast::<PyHistoryTimestamp>() {
            return self.history_t.iter().eq(py_hist.get().history_t.iter());
        }
        if let Ok(list) = other.extract::<Vec<i64>>() {
            return self.history_t.iter().eq(list.into_iter());
        }
        false
    }

    /// Compare inequality with another HistoryTimestamp or with a list of integers.
    ///
    /// Arguments:
    ///     other (HistoryTimestamp | List[int]): The item to compare inequality with.
    ///
    /// Returns:
    ///     bool: True if not equal, otherwise False.
    fn __ne__(&self, other: &Bound<PyAny>) -> bool {
        !self.__eq__(other)
    }

    /// Return the string representation.
    ///
    /// Returns:
    ///     str: String representation.
    pub fn __repr__(&self) -> String {
        self.history_t.repr()
    }
}

impl IntoIterator for PyHistoryTimestamp {
    type Item = i64;
    type IntoIter = BoxedIter<i64>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self.history_t, |item| item.iter()).into_dyn_boxed()
    }
}

impl<T: InternalHistoryOps> Repr for HistoryTimestamp<T> {
    fn repr(&self) -> String {
        format!("HistoryTimestamp({})", iterator_repr(self.iter()))
    }
}

impl Repr for PyHistoryTimestamp {
    fn repr(&self) -> String {
        self.history_t.repr()
    }
}

impl<T: InternalHistoryOps + 'static> From<HistoryTimestamp<T>> for PyHistoryTimestamp {
    fn from(value: HistoryTimestamp<T>) -> Self {
        PyHistoryTimestamp {
            history_t: HistoryTimestamp::new(Arc::new(value.0)),
        }
    }
}

impl<'py, T: InternalHistoryOps + 'static> IntoPyObject<'py> for HistoryTimestamp<T> {
    type Target = PyHistoryTimestamp;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyHistoryTimestamp::from(self).into_pyobject(py)
    }
}

/// History view that exposes UTC datetimes.
#[pyclass(name = "HistoryDateTime", module = "raphtory", frozen)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyHistoryDateTime {
    history_dt: HistoryDateTime<Arc<dyn InternalHistoryOps>>,
}

#[pymethods]
impl PyHistoryDateTime {
    /// Collect all datetimes.
    ///
    /// Returns:
    ///     List[datetime]: Collected UTC datetimes.
    ///
    /// Raises:
    ///     TimeError: If a timestamp cannot be converted to a datetime.
    pub fn collect(&self) -> PyResult<Vec<DateTime<Utc>>> {
        self.history_dt.collect().map_err(PyErr::from)
    }

    /// Collect all datetimes in reverse order.
    ///
    /// Returns:
    ///     List[datetime]: Collected UTC datetimes in reverse order.
    ///
    /// Raises:
    ///     TimeError: If a timestamp cannot be converted to a datetime.
    pub fn collect_rev(&self) -> PyResult<Vec<DateTime<Utc>>> {
        self.history_dt.collect_rev().map_err(PyErr::from)
    }

    /// Iterate over all datetimes.
    ///
    /// Returns:
    ///     Iterator[datetime]: Iterator over UTC datetimes.
    ///
    /// Raises:
    ///     TimeError: May be raised during iteration if a timestamp cannot be converted.
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter_result!(
            self.history_dt.clone(),
            HistoryDateTime<Arc<dyn InternalHistoryOps>>,
            |history_dt| history_dt.iter()
        )
    }

    /// Iterate over all datetimes in reverse order.
    ///
    /// Returns:
    ///     Iterator[datetime]: Iterator over UTC datetimes in reverse order.
    ///
    /// Raises:
    ///     TimeError: May be raised during iteration if a timestamp cannot be converted.
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter_result!(
            self.history_dt.clone(),
            HistoryDateTime<Arc<dyn InternalHistoryOps>>,
            |history_dt| history_dt.iter_rev()
        )
    }

    /// Check if this HistoryDateTime object contains a datetime.
    ///
    /// Arguments:
    ///     item (datetime): Datetime to check. Naive datetimes are treated as UTC; aware datetimes are converted to UTC.
    ///
    /// Returns:
    ///     bool: True if present, otherwise False.
    fn __contains__(&self, item: &Bound<PyAny>) -> bool {
        let dt_opt: Option<DateTime<Utc>> = {
            if let Ok(dt) = item.extract::<DateTime<FixedOffset>>() {
                Some(dt.with_timezone(&Utc));
            }
            if let Ok(ndt) = item.extract::<NaiveDateTime>() {
                Some(ndt.and_utc());
            }
            None
        };
        if let Some(target) = dt_opt {
            return self
                .history_dt
                .iter()
                .any(|res| res.map(|dt| dt == target).unwrap_or(false));
        }
        false
    }

    /// Compare equality with another HistoryDateTime or a list of datetimes.
    ///
    /// Arguments:
    ///     other (HistoryDateTime | List[datetime]): The other item to compare equality with.
    ///
    /// Returns:
    ///     bool: True if equal, otherwise False.
    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        let dt_iter_opt: Option<BoxedLIter<DateTime<Utc>>> = {
            if let Ok(list) = other.extract::<Vec<DateTime<FixedOffset>>>() {
                Some(
                    list.into_iter()
                        .map(|d| d.with_timezone(&Utc))
                        .into_dyn_boxed(),
                );
            }
            if let Ok(list) = other.extract::<Vec<NaiveDateTime>>() {
                Some(list.into_iter().map(|d| d.and_utc()).into_dyn_boxed());
            }
            None
        };
        if let Ok(py_hist) = other.downcast::<PyHistoryDateTime>() {
            return self.history_dt.iter().eq(py_hist.get().history_dt.iter());
        }
        if let Some(iterator) = dt_iter_opt {
            return self.history_dt.iter().eq(iterator.map(|dt| Ok(dt)));
        }
        false
    }

    /// Compare inequality with another HistoryDateTime or a list of datetimes.
    ///
    /// Arguments:
    ///     other (HistoryDateTime | List[datetime]): The other item to compare inequality with.
    ///
    /// Returns:
    ///     bool: True if not equal, otherwise False.
    fn __ne__(&self, other: &Bound<PyAny>) -> bool {
        !self.__eq__(other)
    }

    /// Return the string representation.
    ///
    /// Returns:
    ///     str: String representation.
    pub fn __repr__(&self) -> String {
        self.history_dt.repr()
    }
}

impl IntoIterator for PyHistoryDateTime {
    type Item = Result<DateTime<Utc>, TimeError>;
    type IntoIter = BoxedIter<Result<DateTime<Utc>, TimeError>>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self.history_dt, |item| item.iter()).into_dyn_boxed()
    }
}

impl<T: InternalHistoryOps> Repr for HistoryDateTime<T> {
    fn repr(&self) -> String {
        format!("HistoryDateTime({})", iterator_repr(self.iter()))
    }
}

impl Repr for PyHistoryDateTime {
    fn repr(&self) -> String {
        self.history_dt.repr()
    }
}

impl<T: InternalHistoryOps + 'static> From<HistoryDateTime<T>> for PyHistoryDateTime {
    fn from(value: HistoryDateTime<T>) -> Self {
        PyHistoryDateTime {
            history_dt: HistoryDateTime::new(Arc::new(value.0)),
        }
    }
}

impl<'py, T: InternalHistoryOps + 'static> IntoPyObject<'py> for HistoryDateTime<T> {
    type Target = PyHistoryDateTime;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyHistoryDateTime::from(self).into_pyobject(py)
    }
}

/// History view that exposes event ids of time entries. They are used for ordering within the same timestamp.
#[pyclass(name = "HistoryEventId", module = "raphtory", frozen)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyHistoryEventId {
    history_s: HistoryEventId<Arc<dyn InternalHistoryOps>>,
}

#[pymethods]
impl PyHistoryEventId {
    /// Collect all event ids.
    ///
    /// Returns:
    ///     NDArray[np.uintp]: Event ids.
    pub fn collect<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<usize, Ix1>> {
        let u = self.history_s.collect();
        u.into_pyarray(py)
    }

    /// Collect all event ids in reverse order.
    ///
    /// Returns:
    ///     NDArray[np.uintp]: Event ids in reverse order.
    pub fn collect_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<usize, Ix1>> {
        let u = self.history_s.collect_rev();
        u.into_pyarray(py)
    }

    /// Iterate over all event ids.
    ///
    /// Returns:
    ///     Iterator[int]: Iterator over event ids.
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_s.clone(),
            HistoryEventId<Arc<dyn InternalHistoryOps>>,
            |history_s| history_s.iter()
        )
    }

    /// Iterate over all event ids in reverse order.
    ///
    /// Returns:
    ///     Iterator[int]: Iterator over event ids in reverse order.
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_s.clone(),
            HistoryEventId<Arc<dyn InternalHistoryOps>>,
            |history_s| history_s.iter_rev()
        )
    }

    /// Check if this HistoryEventId object contains an event id.
    ///
    /// Arguments:
    ///     item (int): Event id to check.
    ///
    /// Returns:
    ///     bool: True if present, otherwise False.
    fn __contains__(&self, item: usize) -> bool {
        self.history_s.iter().any(|x| x == item)
    }

    /// Compare equality with another HistoryEventId or a list of integers.
    ///
    /// Arguments:
    ///     other (HistoryEventId | List[int]): The other item to compare equality with.
    ///
    /// Returns:
    ///     bool: True if equal, otherwise False.
    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        if let Ok(py_hist) = other.downcast::<PyHistoryEventId>() {
            return self.history_s.iter().eq(py_hist.get().history_s.iter());
        }
        if let Ok(list) = other.extract::<Vec<usize>>() {
            return self.history_s.iter().eq(list.into_iter());
        }
        false
    }

    /// Compare inequality with another HistoryEventId or a list of integers.
    ///
    /// Arguments:
    ///     other (HistoryEventId | List[int]): The other item to compare inequality with.
    ///
    /// Returns:
    ///     bool: True if not equal, otherwise False.
    fn __ne__(&self, other: &Bound<PyAny>) -> bool {
        !self.__eq__(other)
    }

    /// Return the string representation.
    ///
    /// Returns:
    ///     str: String representation.
    pub fn __repr__(&self) -> String {
        self.history_s.repr()
    }
}

impl IntoIterator for PyHistoryEventId {
    type Item = usize;
    type IntoIter = BoxedIter<usize>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self.history_s, |item| item.iter()).into_dyn_boxed()
    }
}

impl<T: InternalHistoryOps> Repr for HistoryEventId<T> {
    fn repr(&self) -> String {
        format!("HistoryEventId({})", iterator_repr(self.iter()))
    }
}

impl Repr for PyHistoryEventId {
    fn repr(&self) -> String {
        self.history_s.repr()
    }
}

impl<T: InternalHistoryOps + 'static> From<HistoryEventId<T>> for PyHistoryEventId {
    fn from(value: HistoryEventId<T>) -> Self {
        PyHistoryEventId {
            history_s: HistoryEventId::new(Arc::new(value.0)),
        }
    }
}

impl<'py, T: InternalHistoryOps + 'static> IntoPyObject<'py> for HistoryEventId<T> {
    type Target = PyHistoryEventId;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyHistoryEventId::from(self).into_pyobject(py)
    }
}

/// View over the intervals between consecutive timestamps, expressed in milliseconds.
#[pyclass(name = "Intervals", module = "raphtory", frozen)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyIntervals {
    intervals: Intervals<Arc<dyn InternalHistoryOps>>,
}

#[pymethods]
impl PyIntervals {
    /// Collect all interval values in milliseconds.
    ///
    /// Returns:
    ///     NDArray[np.int64]: Intervals in milliseconds.
    pub fn collect<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let i = self.intervals.collect();
        i.into_pyarray(py)
    }

    /// Collect all interval values in reverse order.
    ///
    /// Returns:
    ///     NDArray[np.int64]: Intervals in reverse order.
    pub fn collect_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let i = self.intervals.collect_rev();
        i.into_pyarray(py)
    }

    /// Iterate over all intervals.
    ///
    /// Returns:
    ///     Iterator[int]: Iterator over intervals in milliseconds.
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.intervals.clone(),
            Intervals<Arc<dyn InternalHistoryOps>>,
            |intervals| intervals.iter()
        )
    }

    /// Iterate over all intervals in reverse order.
    ///
    /// Returns:
    ///     Iterator[int]: Iterator over intervals in reverse order.
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.intervals.clone(),
            Intervals<Arc<dyn InternalHistoryOps>>,
            |intervals| intervals.iter_rev()
        )
    }

    /// Check if the Intervals object contains an interval value.
    ///
    /// Arguments:
    ///     item (int): Interval to check, in milliseconds.
    ///
    /// Returns:
    ///     bool: True if present, otherwise False.
    fn __contains__(&self, item: i64) -> bool {
        self.intervals.iter().any(|x| x == item)
    }

    /// Compare equality with another Intervals or a list of integers.
    ///
    /// Arguments:
    ///     other (Intervals | List[int]): The other item to compare equality with.
    ///
    /// Returns:
    ///     bool: True if equal, otherwise False.
    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        if let Ok(py_hist) = other.downcast::<PyIntervals>() {
            return self.intervals.iter().eq(py_hist.get().intervals.iter());
        }
        if let Ok(list) = other.extract::<Vec<i64>>() {
            return self.intervals.iter().eq(list.into_iter());
        }
        false
    }

    /// Compare inequality with another Intervals or a list of integers.
    ///
    /// Arguments:
    ///     other (Intervals | List[int]): The other item to compare inequality with.
    ///
    /// Returns:
    ///     bool: True if not equal, otherwise False.
    fn __ne__(&self, other: &Bound<PyAny>) -> bool {
        !self.__eq__(other)
    }

    /// Return the string representation.
    ///
    /// Returns:
    ///     str: String representation.
    pub fn __repr__(&self) -> String {
        self.intervals.repr()
    }

    /// Calculate the mean interval in milliseconds.
    ///
    /// Returns:
    ///     Optional[float]: Mean interval, or None if fewer than 1 interval.
    pub fn mean(&self) -> Option<f64> {
        self.intervals.mean()
    }

    /// Calculate the median interval in milliseconds.
    ///
    /// Returns:
    ///     Optional[int]: Median interval, or None if fewer than 1 interval.
    pub fn median(&self) -> Option<i64> {
        self.intervals.median()
    }

    /// Calculate the maximum interval in milliseconds.
    ///
    /// Returns:
    ///     Optional[int]: Maximum interval, or None if fewer than 1 interval.
    pub fn max(&self) -> Option<i64> {
        self.intervals.max()
    }

    /// Calculate the minimum interval in milliseconds.
    ///
    /// Returns:
    ///     Optional[int]: Minimum interval, or None if fewer than 1 interval.
    pub fn min(&self) -> Option<i64> {
        self.intervals.min()
    }
}

impl IntoIterator for PyIntervals {
    type Item = i64;
    type IntoIter = BoxedIter<i64>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self.intervals, |item| item.iter()).into_dyn_boxed()
    }
}

impl<T: InternalHistoryOps> Repr for Intervals<T> {
    fn repr(&self) -> String {
        format!("Intervals({})", iterator_repr(self.iter()))
    }
}

impl Repr for PyIntervals {
    fn repr(&self) -> String {
        self.intervals.repr()
    }
}

impl<T: InternalHistoryOps + 'static> From<Intervals<T>> for PyIntervals {
    fn from(value: Intervals<T>) -> Self {
        PyIntervals {
            intervals: Intervals::new(Arc::new(value.0)),
        }
    }
}

impl<'py, T: InternalHistoryOps + 'static> IntoPyObject<'py> for Intervals<T> {
    type Target = PyIntervals;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyIntervals::from(self).into_pyobject(py)
    }
}

// Iterable types used by Edges, temporal props, ...
py_iterable_base!(
    HistoryIterable,
    History<'static, Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(HistoryIterable, PyGenericIterator);

#[pymethods]
impl HistoryIterable {
    /// Access history items as timestamps (milliseconds since the Unix epoch).
    ///
    /// Returns:
    ///     HistoryTimestampIterable: Iterable of HistoryTimestamp objects, one for each item.
    #[getter]
    pub fn t(&self) -> HistoryTimestampIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|h| h.t())).into()
    }

    /// Access history items as UTC datetimes.
    ///
    /// Returns:
    ///     HistoryDateTimeIterable: Iterable of HistoryDateTime objects, one for each item.
    #[getter]
    pub fn dt(&self) -> HistoryDateTimeIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|h| h.dt())).into()
    }

    /// Access event ids of history items.
    ///
    /// Returns:
    ///     HistoryEventIdIterable: Iterable of HistoryEventId objects, one for each item.
    #[getter]
    pub fn event_id(&self) -> HistoryEventIdIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|h| h.event_id())).into()
    }

    /// Access intervals between consecutive timestamps in milliseconds.
    ///
    /// Returns:
    ///     IntervalsIterable: Iterable of Intervals objects, one for each item.
    #[getter]
    pub fn intervals(&self) -> IntervalsIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|h| h.intervals())).into()
    }

    /// Collect time entries from each history in the iterable.
    ///
    /// Returns:
    ///     List[List[EventTime]]: Collected entries per history.
    pub fn collect(&self) -> Vec<Vec<EventTime>> {
        self.iter().map(|h| h.collect()).collect()
    }
}

py_nested_iterable_base!(
    NestedHistoryIterable,
    History<'static, Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(NestedHistoryIterable, PyNestedGenericIterator);

#[pymethods]
impl NestedHistoryIterable {
    /// Access nested histories as timestamp views.
    ///
    /// Returns:
    ///     NestedHistoryTimestampIterable: Iterable of iterables of HistoryTimestamp objects.
    #[getter]
    pub fn t(&self) -> NestedHistoryTimestampIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|it| it.map(|h| h.t()))).into()
    }

    /// Access nested histories as datetime views.
    ///
    /// Returns:
    ///     NestedHistoryDateTimeIterable: Iterable of iterables of HistoryDateTime objects.
    #[getter]
    pub fn dt(&self) -> NestedHistoryDateTimeIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|it| it.map(|h| h.dt()))).into()
    }

    /// Access nested histories as event id views.
    ///
    /// Returns:
    ///     NestedHistoryEventIdIterable: Iterable of iterables of HistoryEventId objects.
    #[getter]
    pub fn event_id(&self) -> NestedHistoryEventIdIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|it| it.map(|h| h.event_id()))).into()
    }

    /// Access nested histories as intervals views.
    ///
    /// Returns:
    ///     NestedIntervalsIterable: Iterable of iterables of Intervals objects.
    #[getter]
    pub fn intervals(&self) -> NestedIntervalsIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|it| it.map(|h| h.intervals()))).into()
    }

    /// Collect time entries from each history within each nested iterable.
    ///
    /// Returns:
    ///     List[List[List[EventTime]]]: Collected entries per nested history.
    pub fn collect(&self) -> Vec<Vec<Vec<EventTime>>> {
        self.iter()
            .map(|h| h.map(|h| h.collect()).collect())
            .collect()
    }
}

py_iterable_base!(
    HistoryTimestampIterable,
    HistoryTimestamp<Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(HistoryTimestampIterable, PyGenericIterator);

#[pymethods]
impl HistoryTimestampIterable {
    /// Collect timestamps for each history.
    ///
    /// Returns:
    ///     List[NDArray[np.int64]]: Timestamps in milliseconds per history.
    pub fn collect<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyArray<i64, Ix1>>> {
        self.iter().map(|h| h.collect().into_pyarray(py)).collect()
    }
}

py_nested_iterable_base!(
    NestedHistoryTimestampIterable,
    HistoryTimestamp<Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(NestedHistoryTimestampIterable, PyNestedGenericIterator);

#[pymethods]
impl NestedHistoryTimestampIterable {
    /// Collect timestamps for each history in each nested iterable.
    ///
    /// Returns:
    ///     List[List[NDArray[np.int64]]]: Timestamps in milliseconds per nested history.
    pub fn collect<'py>(&self, py: Python<'py>) -> Vec<Vec<Bound<'py, PyArray<i64, Ix1>>>> {
        self.iter()
            .map(|h| h.map(|h| h.collect().into_pyarray(py)).collect())
            .collect()
    }
}

py_iterable_base!(
    HistoryDateTimeIterable,
    HistoryDateTime<Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(HistoryDateTimeIterable, PyGenericIterator);

#[pymethods]
impl HistoryDateTimeIterable {
    /// Collect datetimes for each history.
    ///
    /// Returns:
    ///     List[List[datetime]]: UTC datetimes per history.
    ///
    /// Raises:
    ///     TimeError: If a timestamp cannot be converted to a datetime.
    pub fn collect(&self) -> Result<Vec<Vec<DateTime<Utc>>>, TimeError> {
        self.iter().map(|h| h.collect()).collect()
    }
}

py_nested_iterable_base!(
    NestedHistoryDateTimeIterable,
    HistoryDateTime<Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(NestedHistoryDateTimeIterable, PyNestedGenericIterator);

#[pymethods]
impl NestedHistoryDateTimeIterable {
    /// Collect datetimes for each history in each nested iterable.
    ///
    /// Returns:
    ///     List[List[List[datetime]]]: UTC datetimes per nested history.
    ///
    /// Raises:
    ///     TimeError: If a timestamp cannot be converted to a datetime.
    pub fn collect(&self) -> Result<Vec<Vec<Vec<DateTime<Utc>>>>, TimeError> {
        self.iter()
            .map(|h| h.map(|h| h.collect()).collect())
            .collect()
    }
}

py_iterable_base!(
    HistoryEventIdIterable,
    HistoryEventId<Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(HistoryEventIdIterable, PyGenericIterator);

#[pymethods]
impl HistoryEventIdIterable {
    /// Collect event ids for each history.
    ///
    /// Returns:
    ///     List[List[int]]: Event ids per history.
    pub fn collect(&self) -> Vec<Vec<usize>> {
        Iterable::iter(self).map(|h| h.collect()).collect()
    }
}

py_nested_iterable_base!(
    NestedHistoryEventIdIterable,
    HistoryEventId<Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(NestedHistoryEventIdIterable, PyNestedGenericIterator);

#[pymethods]
impl NestedHistoryEventIdIterable {
    /// Collect event ids for each history in each nested iterable.
    ///
    /// Returns:
    ///     List[List[List[int]]]: Event ids per nested history.
    pub fn collect(&self) -> Vec<Vec<Vec<usize>>> {
        self.iter()
            .map(|h| h.map(|h| h.collect()).collect())
            .collect()
    }
}

py_iterable_base!(IntervalsIterable, Intervals<Arc<dyn InternalHistoryOps>>);
py_iterable_base_methods!(IntervalsIterable, PyGenericIterator);

#[pymethods]
impl IntervalsIterable {
    /// Collect intervals for each history in milliseconds.
    ///
    /// Returns:
    ///     List[List[int]]: Intervals per history.
    pub fn collect(&self) -> Vec<Vec<i64>> {
        self.iter().map(|h| h.collect()).collect()
    }
}

py_nested_iterable_base!(
    NestedIntervalsIterable,
    Intervals<Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(NestedIntervalsIterable, PyNestedGenericIterator);

#[pymethods]
impl NestedIntervalsIterable {
    /// Collect intervals for each history in each nested iterable, in milliseconds.
    ///
    /// Returns:
    ///     List[List[List[int]]]: Intervals per nested history.
    pub fn collect(&self) -> Vec<Vec<Vec<i64>>> {
        self.iter()
            .map(|h| h.map(|h| h.collect()).collect())
            .collect()
    }
}
