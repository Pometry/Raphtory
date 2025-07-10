use crate::{
    db::api::view::history::*,
    python::{
        graph::{edge::PyEdge, node::PyNode, properties::PyTemporalProp},
        types::{iterable::FromIterable, repr::Repr, wrappers::iterators::PyBorrowingIterator},
    },
};
use chrono::{DateTime, Utc};
use numpy::{IntoPyArray, Ix1, PyArray};
use pyo3::prelude::*;
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

#[pyclass(name = "History", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyHistory {
    history: History<'static, Arc<dyn InternalHistoryOps>>,
}

impl PyHistory {
    pub fn new(history: History<'static, Arc<dyn InternalHistoryOps>>) -> PyHistory {
        PyHistory { history }
    }
}

// TODO: Implement __lt__, __gt__, ...?
#[pymethods]
impl PyHistory {
    #[staticmethod]
    pub fn from_node(node: &PyNode) -> Self {
        Self {
            history: History::new(Arc::new(node.node.clone())),
        }
    }

    #[staticmethod]
    pub fn from_edge(edge: &PyEdge) -> Self {
        Self {
            history: History::new(Arc::new(edge.edge.clone())),
        }
    }

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

    pub fn reverse(&self) -> Self {
        PyHistory {
            history: History::new(Arc::new(ReversedHistoryOps::new(self.history.0.clone()))),
        }
    }

    /// Access history events as i64 timestamps
    pub fn t(&self) -> PyHistoryTimestamp {
        PyHistoryTimestamp {
            history_t: HistoryTimestamp::new(Arc::new(self.history.0.clone())),
        }
    }

    /// Access history events as DateTime items
    pub fn dt(&self) -> PyHistoryDateTime {
        PyHistoryDateTime {
            history_dt: HistoryDateTime::new(Arc::new(self.history.0.clone())),
        }
    }

    /// Access secondary index (unique index) of history events
    pub fn secondary_index(&self) -> PyHistorySecondaryIndex {
        PyHistorySecondaryIndex {
            history_s: HistorySecondaryIndex::new(Arc::new(self.history.0.clone())),
        }
    }

    pub fn intervals(&self) -> PyIntervals {
        PyIntervals {
            intervals: Intervals::new(Arc::new(self.history.0.clone())),
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
    pub fn __list__(&self) -> Vec<TimeIndexEntry> {
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
    pub fn iter_rev(&self) -> PyBorrowingIterator {
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

    // TODO: Can we use &PyHistory here or does it have to be generic PyAny/PyObject
    fn __eq__(&self, other: &PyHistory) -> bool {
        self.history.eq(&other.history)
    }

    fn __ne__(&self, other: &PyHistory) -> bool {
        self.history.ne(&other.history)
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
    pub fn __list__<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
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
    pub fn iter_rev(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_t.clone(),
            HistoryTimestamp<Arc<dyn InternalHistoryOps>>,
            |history_t| history_t.iter_rev()
        )
    }
}

#[pyclass(name = "HistoryDateTime", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyHistoryDateTime {
    history_dt: HistoryDateTime<Arc<dyn InternalHistoryOps>>,
}

impl PyHistoryDateTime {
    pub fn new(history_dt: HistoryDateTime<Arc<dyn InternalHistoryOps>>) -> Self {
        Self { history_dt }
    }
}

#[pymethods]
impl PyHistoryDateTime {
    /// Collect all time events
    pub fn __list__(&self) -> PyResult<Vec<DateTime<Utc>>> {
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
    pub fn iter_rev(&self) -> PyBorrowingIterator {
        py_borrowing_iter_result!(
            self.history_dt.clone(),
            HistoryDateTime<Arc<dyn InternalHistoryOps>>,
            |history_dt| history_dt.iter_rev()
        )
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
    pub fn __list__<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<usize, Ix1>> {
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
    pub fn iter_rev(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.history_s.clone(),
            HistorySecondaryIndex<Arc<dyn InternalHistoryOps>>,
            |history_s| history_s.iter_rev()
        )
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
    pub fn __list__<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let i = self.intervals.collect();
        i.into_pyarray(py)
    }

    /// Collect all interval values in reverse
    pub fn collect_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let i = self.intervals.collect_rev();
        i.into_pyarray(py)
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

#[pymethods]
impl PyTemporalProp {
    pub fn get_history(&self) -> PyHistory {
        PyHistory {
            history: History::new(Arc::new(self.prop.clone())),
        }
    }
}

impl<T: InternalHistoryOps + 'static> From<History<'_, T>> for PyHistory {
    fn from(history: History<T>) -> Self {
        Self {
            history: History::new(Arc::new(history.0)),
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

impl Repr for TimeIndexEntry {
    fn repr(&self) -> String {
        self.to_string()
    }
}
