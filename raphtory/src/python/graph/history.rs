use crate::{
    db::api::view::history::*,
    python::{
        graph::{edge::PyEdge, node::PyNode},
        types::{
            iterable::{FromIterable, Iterable},
            repr::{iterator_repr, Repr},
            wrappers::iterators::PyBorrowingIterator,
        },
        utils::{PyGenericIterator, PyNestedGenericIterator},
    },
};
use chrono::{DateTime, Utc};
use numpy::{IntoPyArray, Ix1, PyArray};
use pyo3::prelude::*;
use raphtory_api::core::storage::timeindex::{TimeError, TimeIndexEntry};
use std::{
    any::Any,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

impl Repr for TimeIndexEntry {
    fn repr(&self) -> String {
        self.to_string()
    }
}

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
    #[staticmethod]
    pub fn compose_histories(objects: FromIterable<PyHistory>) -> Self {
        // the only way to get History objects from python is if they are already Arc<...>
        let underlying_objects: Vec<Arc<dyn InternalHistoryOps>> = objects
            .into_iter()
            .map(|obj| Arc::clone(&obj.history.0))
            .collect();
        Self {
            history: History::new(Arc::new(CompositeHistory::new(underlying_objects))),
        }
    }

    // Clones the Arcs, we end up with Arc<Arc<dyn InternalHistoryOps>>, 1 level of indirection. Cloning the underlying InternalHistoryOps objects introduces lifetime issues.
    // TODO: Ideally we want one of compose_histories/merge. We want to see where the performance benefits shift from one to the other and automatically use that.
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            history: History::new(Arc::new(MergedHistory::new(
                self.history.0.clone(),
                other.history.0.clone(),
            ))),
        }
    }

    // Clones the Arcs, we end up with Arc<Arc<dyn InternalHistoryOps>>, 1 level of indirection. Cloning the underlying InternalHistoryOps objects introduces lifetime issues.
    pub fn reverse(&self) -> Self {
        PyHistory {
            history: History::new(Arc::new(ReversedHistoryOps::new(self.history.0.clone()))),
        }
    }

    /// Access history events as i64 timestamps
    #[getter]
    pub fn t(&self) -> PyHistoryTimestamp {
        PyHistoryTimestamp {
            history_t: HistoryTimestamp::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }

    /// Access history events as DateTime items
    #[getter]
    pub fn dt(&self) -> PyHistoryDateTime {
        PyHistoryDateTime {
            history_dt: HistoryDateTime::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }

    /// Access secondary index (unique index) of history events
    #[getter]
    pub fn secondary_index(&self) -> PyHistorySecondaryIndex {
        PyHistorySecondaryIndex {
            history_s: HistorySecondaryIndex::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }

    #[getter]
    pub fn intervals(&self) -> PyIntervals {
        PyIntervals {
            intervals: Intervals::new(self.history.0.clone()), // clone the Arc, not the underlying object
        }
    }

    /// Get the earliest time in the history
    pub fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.history.earliest_time()
    }

    /// Get the latest time in the history
    pub fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.history.latest_time()
    }

    /// Collect all time events
    pub fn collect(&self) -> Vec<TimeIndexEntry> {
        self.history.collect()
    }

    /// Collect all time events in reverse
    pub fn collect_rev(&self) -> Vec<TimeIndexEntry> {
        self.history.collect_rev()
    }

    /// Iterate over all time events
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history.clone(),
            History<'static, Arc<dyn InternalHistoryOps>>,
            |history| history.iter()
        )
    }

    /// Iterate over all time events in reverse
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history.clone(),
            History<'static, Arc<dyn InternalHistoryOps>>,
            |history| history.iter_rev()
        )
    }

    pub fn __repr__(&self) -> String {
        self.history.repr()
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.history.hash(&mut hasher);
        hasher.finish()
    }

    // implement contains function. might need to downcast general python object, can use enum to downcast to handle all AsTime objects.

    // Might need to downcast to handle all AsTime objects.
    fn __eq__(&self, other: &PyHistory) -> bool {
        self.history.eq(&other.history)
    }

    fn __ne__(&self, other: &PyHistory) -> bool {
        self.history.ne(&other.history)
    }

    pub fn is_empty(&self) -> bool {
        self.history.is_empty()
    }

    pub fn __len__(&self) -> usize {
        self.history.len()
    }
}

impl<T: InternalHistoryOps + 'static> From<History<'_, T>> for PyHistory {
    fn from(history: History<T>) -> Self {
        let arc_ops: Arc<dyn InternalHistoryOps> = {
            // Check if T is already Arc<dyn InternalHistoryOps> to avoid nesting them
            let any_ref: &dyn Any = &history.0;
            if let Some(arc_obj) = any_ref.downcast_ref::<Arc<dyn InternalHistoryOps>>() {
                Arc::clone(arc_obj)
            } else {
                Arc::new(history.0)
            }
        };
        Self {
            history: History::new(arc_ops),
        }
    }
}

impl<'py, T: InternalHistoryOps + 'static> IntoPyObject<'py> for History<'_, T> {
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

#[pyclass(name = "HistoryTimestamp", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyHistoryTimestamp {
    history_t: HistoryTimestamp<Arc<dyn InternalHistoryOps>>,
}

#[pymethods]
impl PyHistoryTimestamp {
    /// Collect all time events
    pub fn collect<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let t = self.history_t.collect();
        t.into_pyarray(py)
    }

    /// Collect all time events in reverse
    pub fn collect_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let t = self.history_t.collect_rev();
        t.into_pyarray(py)
    }

    /// Iterate over all time events
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_t.clone(),
            HistoryTimestamp<Arc<dyn InternalHistoryOps>>,
            |history_t| history_t.iter()
        )
    }

    /// Iterate over all time events in reverse
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_t.clone(),
            HistoryTimestamp<Arc<dyn InternalHistoryOps>>,
            |history_t| history_t.iter_rev()
        )
    }

    pub fn __repr__(&self) -> String {
        self.history_t.repr()
    }
}

impl<T: InternalHistoryOps> Repr for HistoryTimestamp<T> {
    fn repr(&self) -> String {
        format!("HistoryTimestamp({})", iterator_repr(self.iter()))
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

#[pyclass(name = "HistoryDateTime", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyHistoryDateTime {
    history_dt: HistoryDateTime<Arc<dyn InternalHistoryOps>>,
}

#[pymethods]
impl PyHistoryDateTime {
    /// Collect all time events
    pub fn collect(&self) -> PyResult<Vec<DateTime<Utc>>> {
        self.history_dt.collect().map_err(PyErr::from)
    }

    /// Collect all time events in reverse
    pub fn collect_rev(&self) -> PyResult<Vec<DateTime<Utc>>> {
        self.history_dt.collect_rev().map_err(PyErr::from)
    }

    /// Iterate over all time events
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter_result!(
            self.history_dt.clone(),
            HistoryDateTime<Arc<dyn InternalHistoryOps>>,
            |history_dt| history_dt.iter()
        )
    }

    /// Iterate over all time events in reverse
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter_result!(
            self.history_dt.clone(),
            HistoryDateTime<Arc<dyn InternalHistoryOps>>,
            |history_dt| history_dt.iter_rev()
        )
    }

    pub fn __repr__(&self) -> String {
        self.history_dt.repr()
    }
}

impl<T: InternalHistoryOps> Repr for HistoryDateTime<T> {
    fn repr(&self) -> String {
        format!("HistoryDateTime({})", iterator_repr(self.iter()))
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

#[pyclass(name = "HistorySecondaryIndex", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyHistorySecondaryIndex {
    history_s: HistorySecondaryIndex<Arc<dyn InternalHistoryOps>>,
}

#[pymethods]
impl PyHistorySecondaryIndex {
    /// Collect all time events
    pub fn collect<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<usize, Ix1>> {
        let u = self.history_s.collect();
        u.into_pyarray(py)
    }

    /// Collect all time events in reverse
    pub fn collect_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<usize, Ix1>> {
        let u = self.history_s.collect_rev();
        u.into_pyarray(py)
    }

    /// Iterate over all time events
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_s.clone(),
            HistorySecondaryIndex<Arc<dyn InternalHistoryOps>>,
            |history_s| history_s.iter()
        )
    }

    /// Iterate over all time events in reverse
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_s.clone(),
            HistorySecondaryIndex<Arc<dyn InternalHistoryOps>>,
            |history_s| history_s.iter_rev()
        )
    }

    pub fn __repr__(&self) -> String {
        self.history_s.repr()
    }
}

impl<T: InternalHistoryOps> Repr for HistorySecondaryIndex<T> {
    fn repr(&self) -> String {
        format!("HistorySecondaryIndex({})", iterator_repr(self.iter()))
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

#[pyclass(name = "Intervals", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyIntervals {
    intervals: Intervals<Arc<dyn InternalHistoryOps>>,
}

#[pymethods]
impl PyIntervals {
    /// Collect all interval values
    pub fn collect<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let i = self.intervals.collect();
        i.into_pyarray(py)
    }

    /// Collect all interval values in reverse
    pub fn collect_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let i = self.intervals.collect_rev();
        i.into_pyarray(py)
    }

    /// Iterate over all interval values
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.intervals.clone(),
            Intervals<Arc<dyn InternalHistoryOps>>,
            |intervals| intervals.iter()
        )
    }

    /// Iterate over all interval values in reverse
    pub fn __reversed__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.intervals.clone(),
            Intervals<Arc<dyn InternalHistoryOps>>,
            |intervals| intervals.iter_rev()
        )
    }

    pub fn __repr__(&self) -> String {
        self.intervals.repr()
    }

    /// Calculate the mean interval between values
    pub fn mean(&self) -> Option<f64> {
        self.intervals.mean()
    }

    /// Calculate the median interval between values
    pub fn median(&self) -> Option<f64> {
        self.intervals.median()
    }

    /// Calculate the maximum interval between values
    pub fn max(&self) -> Option<i64> {
        self.intervals.max()
    }

    /// Calculate the minimum interval between values
    pub fn min(&self) -> Option<i64> {
        self.intervals.min()
    }
}

impl<T: InternalHistoryOps> Repr for Intervals<T> {
    fn repr(&self) -> String {
        format!("Intervals({})", iterator_repr(self.iter()))
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

// Iterable types used by Edges
py_iterable_base!(
    HistoryIterable,
    History<'static, Arc<dyn InternalHistoryOps>>
);
py_iterable_base_methods!(HistoryIterable, PyGenericIterator);

#[pymethods]
impl HistoryIterable {
    #[getter]
    pub fn t(&self) -> HistoryTimestampIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|h| h.t())).into()
    }

    #[getter]
    pub fn dt(&self) -> HistoryDateTimeIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|h| h.dt())).into()
    }

    #[getter]
    pub fn secondary_index(&self) -> HistorySecondaryIndexIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|h| h.secondary_index())).into()
    }

    #[getter]
    pub fn intervals(&self) -> IntervalsIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|h| h.intervals())).into()
    }

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
    #[getter]
    pub fn t(&self) -> NestedHistoryTimestampIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|it| it.map(|h| h.t()))).into()
    }

    #[getter]
    pub fn dt(&self) -> NestedHistoryDateTimeIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|it| it.map(|h| h.dt()))).into()
    }

    #[getter]
    pub fn secondary_index(&self) -> NestedHistorySecondaryIndexIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|it| it.map(|h| h.secondary_index()))).into()
    }

    #[getter]
    pub fn intervals(&self) -> NestedIntervalsIterable {
        let builder = self.0.builder.clone();
        (move || builder().map(|it| it.map(|h| h.intervals()))).into()
    }

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
    pub fn collect<'py>(&self, py: Python<'py>) -> Result<Vec<Vec<Vec<DateTime<Utc>>>>, TimeError> {
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
    pub fn collect<'py>(&self) -> Vec<Vec<Vec<usize>>> {
        self.iter()
            .map(|h| h.map(|h| h.collect()).collect())
            .collect()
    }
}

py_iterable_base!(IntervalsIterable, Intervals<Arc<dyn InternalHistoryOps>>);
py_iterable_base_methods!(IntervalsIterable, PyGenericIterator);

#[pymethods]
impl IntervalsIterable {
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
    pub fn collect<'py>(&self) -> Vec<Vec<Vec<i64>>> {
        self.iter()
            .map(|h| h.map(|h| h.collect()).collect())
            .collect()
    }
}
