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
    core::storage::timeindex::{TimeError, TimeIndexEntry},
    iter::{BoxedIter, BoxedLIter, IntoDynBoxed},
};
use raphtory_core::utils::iter::GenLockedIter;
use std::sync::Arc;

impl Repr for TimeIndexEntry {
    fn repr(&self) -> String {
        self.to_string()
    }
}

/// History of updates for an object. Provides access to time entries and derived views such as timestamps, datetimes, secondary indices, and intervals.
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

    /// Access history events as timestamps (milliseconds since Unix epoch).
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

    /// Access the unique secondary index of each time entry.
    ///
    /// Returns:
    ///     HistorySecondaryIndex: Secondary index view of this history.
    #[getter]
    pub fn secondary_index(&self) -> PyHistorySecondaryIndex {
        PyHistorySecondaryIndex {
            history_s: HistorySecondaryIndex::new(self.history.0.clone()), // clone the Arc, not the underlying object
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
    ///     Optional[TimeIndexEntry]: Earliest time entry, or None if empty.
    pub fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.history.earliest_time()
    }

    /// Get the latest time entry.
    ///
    /// Returns:
    ///     Optional[TimeIndexEntry]: Latest time entry, or None if empty.
    pub fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.history.latest_time()
    }

    /// Collect all time entries in chronological order.
    ///
    /// Returns:
    ///     List[TimeIndexEntry]: Collected time entries.
    pub fn collect(&self) -> Vec<TimeIndexEntry> {
        self.history.collect()
    }

    /// Collect all time entries in reverse chronological order.
    ///
    /// Returns:
    ///     List[TimeIndexEntry]: Collected time entries in reverse order.
    pub fn collect_rev(&self) -> Vec<TimeIndexEntry> {
        self.history.collect_rev()
    }

    /// Iterate over all time entries in chronological order.
    ///
    /// Returns:
    ///     Iterator[TimeIndexEntry]: Iterator over time entries.
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
    ///     Iterator[TimeIndexEntry]: Iterator over time entries in reverse order.
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
    ///     item (TimeIndexEntry): Time entry to check.
    ///
    /// Returns:
    ///     bool: True if present, otherwise False.
    fn __contains__(&self, item: TimeIndexEntry) -> bool {
        self.history.iter().any(|x| x == item)
    }

    /// Compare equality with another History or a list of TimeIndexEntry.
    ///
    /// Arguments:
    ///     other (History | List[TimeIndexEntry]): The item to compare equality with.
    ///
    /// Returns:
    ///     bool: True if equal, otherwise False.
    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        if let Ok(py_hist) = other.downcast::<PyHistory>() {
            return self.history.eq(&py_hist.get().history);
        }
        if let Ok(list) = other.extract::<Vec<TimeIndexEntry>>() {
            return self.history.iter().eq(list.into_iter());
        }
        false
    }

    /// Compare inequality with another History or a list of TimeIndexEntry.
    ///
    /// Arguments:
    ///     other (History | List[TimeIndexEntry]): The item to compare inequality with.
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

/// History view that exposes timestamps in milliseconds since Unix epoch.
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
    ///     NDArray[np.int64]: Timestamps in milliseconds since Unix epoch.
    pub fn collect<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let t = self.history_t.collect();
        t.into_pyarray(py)
    }

    /// Collect all timestamps into a numpy ndarray in reverse order.
    ///
    /// Returns:
    ///     NDArray[np.int64]: Timestamps in milliseconds since Unix epoch in reverse order.
    pub fn collect_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let t = self.history_t.collect_rev();
        t.into_pyarray(py)
    }

    /// Iterate over all timestamps.
    ///
    /// Returns:
    ///     Iterator[int]: Iterator over timestamps in milliseconds since Unix epoch.
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
    ///     Iterator[int]: Iterator over timestamps (milliseconds since Unix epoch) in reverse order.
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
    ///     item (int): Timestamp in milliseconds since Unix epoch.
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

/// History view that exposes secondary indices of time entries. They are used for ordering within the same timestamp.
#[pyclass(name = "HistorySecondaryIndex", module = "raphtory", frozen)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyHistorySecondaryIndex {
    history_s: HistorySecondaryIndex<Arc<dyn InternalHistoryOps>>,
}

#[pymethods]
impl PyHistorySecondaryIndex {
    /// Collect all secondary indices.
    ///
    /// Returns:
    ///     NDArray[np.uintp]: Secondary indices.
    pub fn collect<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<usize, Ix1>> {
        let u = self.history_s.collect();
        u.into_pyarray(py)
    }

    /// Collect all secondary indices in reverse order.
    ///
    /// Returns:
    ///     NDArray[np.uintp]: Secondary indices in reverse order.
    pub fn collect_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<usize, Ix1>> {
        let u = self.history_s.collect_rev();
        u.into_pyarray(py)
    }

    /// Iterate over all secondary indices.
    ///
    /// Returns:
    ///     Iterator[int]: Iterator over secondary indices.
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_s.clone(),
            HistorySecondaryIndex<Arc<dyn InternalHistoryOps>>,
            |history_s| history_s.iter()
        )
    }

    /// Iterate over all secondary indices in reverse order.
    ///
    /// Returns:
    ///     Iterator[int]: Iterator over secondary indices in reverse order.
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_s.clone(),
            HistorySecondaryIndex<Arc<dyn InternalHistoryOps>>,
            |history_s| history_s.iter_rev()
        )
    }

    /// Check if this HistorySecondaryIndex object contains a secondary index.
    ///
    /// Arguments:
    ///     item (int): Secondary index to check.
    ///
    /// Returns:
    ///     bool: True if present, otherwise False.
    fn __contains__(&self, item: usize) -> bool {
        self.history_s.iter().any(|x| x == item)
    }

    /// Compare equality with another HistorySecondaryIndex or a list of integers.
    ///
    /// Arguments:
    ///     other (HistorySecondaryIndex | List[int]): The other item to compare equality with.
    ///
    /// Returns:
    ///     bool: True if equal, otherwise False.
    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        if let Ok(py_hist) = other.downcast::<PyHistorySecondaryIndex>() {
            return self.history_s.iter().eq(py_hist.get().history_s.iter());
        }
        if let Ok(list) = other.extract::<Vec<usize>>() {
            return self.history_s.iter().eq(list.into_iter());
        }
        false
    }

    /// Compare inequality with another HistorySecondaryIndex or a list of integers.
    ///
    /// Arguments:
    ///     other (HistorySecondaryIndex | List[int]): The other item to compare inequality with.
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

impl IntoIterator for PyHistorySecondaryIndex {
    type Item = usize;
    type IntoIter = BoxedIter<usize>;

    fn into_iter(self) -> Self::IntoIter {
        GenLockedIter::from(self.history_s, |item| item.iter()).into_dyn_boxed()
    }
}

impl<T: InternalHistoryOps> Repr for HistorySecondaryIndex<T> {
    fn repr(&self) -> String {
        format!("HistorySecondaryIndex({})", iterator_repr(self.iter()))
    }
}

impl Repr for PyHistorySecondaryIndex {
    fn repr(&self) -> String {
        self.history_s.repr()
    }
}

impl<T: InternalHistoryOps + 'static> From<HistorySecondaryIndex<T>> for PyHistorySecondaryIndex {
    fn from(value: HistorySecondaryIndex<T>) -> Self {
        PyHistorySecondaryIndex {
            history_s: HistorySecondaryIndex::new(Arc::new(value.0)),
        }
    }
}

impl<'py, T: InternalHistoryOps + 'static> IntoPyObject<'py> for HistorySecondaryIndex<T> {
    type Target = PyHistorySecondaryIndex;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyHistorySecondaryIndex::from(self).into_pyobject(py)
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
    /// Access history items as timestamps (milliseconds since Unix epoch).
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

    /// Access secondary indices of history items.
    ///
    /// Returns:
    ///     HistorySecondaryIndexIterable: Iterable of HistorySecondaryIndex objects, one for each item.
    #[getter]
    pub fn secondary_index(&self) -> HistorySecondaryIndexIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|h| h.secondary_index())).into()
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
    ///     List[List[TimeIndexEntry]]: Collected entries per history.
    pub fn collect(&self) -> Vec<Vec<TimeIndexEntry>> {
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

    /// Access nested histories as secondary index views.
    ///
    /// Returns:
    ///     NestedHistorySecondaryIndexIterable: Iterable of iterables of HistorySecondaryIndex objects.
    #[getter]
    pub fn secondary_index(&self) -> NestedHistorySecondaryIndexIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|it| it.map(|h| h.secondary_index()))).into()
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
    ///     List[List[List[TimeIndexEntry]]]: Collected entries per nested history.
    pub fn collect(&self) -> Vec<Vec<Vec<TimeIndexEntry>>> {
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
    HistorySecondaryIndexIterable,
    HistorySecondaryIndex<Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(HistorySecondaryIndexIterable, PyGenericIterator);

#[pymethods]
impl HistorySecondaryIndexIterable {
    /// Collect secondary indices for each history.
    ///
    /// Returns:
    ///     List[List[int]]: Secondary indices per history.
    pub fn collect(&self) -> Vec<Vec<usize>> {
        Iterable::iter(self).map(|h| h.collect()).collect()
    }
}

py_nested_iterable_base!(
    NestedHistorySecondaryIndexIterable,
    HistorySecondaryIndex<Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(NestedHistorySecondaryIndexIterable, PyNestedGenericIterator);

#[pymethods]
impl NestedHistorySecondaryIndexIterable {
    /// Collect secondary indices for each history in each nested iterable.
    ///
    /// Returns:
    ///     List[List[List[int]]]: Secondary indices per nested history.
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
