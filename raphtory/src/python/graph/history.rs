use pyo3::{
    prelude::*,
    types::{PyDateTime},
};
use chrono::{Datelike, Timelike};
use crate::{
    db::api::view::history::*,
};
use std::sync::Arc;

#[pyclass(name = "History", module = "raphtory", frozen)]
pub struct PyHistory {
    history: Arc<HistoryImplemented<Box<dyn InternalHistoryOps + Send + Sync>>>,
}

#[pymethods]
impl PyHistory {
    #[new]
    pub fn new(history: Arc<HistoryImplemented<Box<dyn InternalHistoryOps + Send + Sync>>>) -> Self {
        Self { history }
    }

    /// Get the earliest time in the history
    pub fn earliest_time(&self) -> Option<PyRaphtoryTime> {
        self.history.earliest_time().map(PyRaphtoryTime::new)
    }

    /// Get the latest time in the history
    pub fn latest_time(&self) -> Option<PyRaphtoryTime> {
        self.history.latest_time().map(PyRaphtoryTime::new)
    }

    /// Get an iterator over all times in the history
    pub fn iter(&self) -> PyResult<Vec<PyRaphtoryTime>> {
        Ok(self.history
            .iter()
            .map(|t| PyRaphtoryTime::new(RaphtoryTime::apply(t.clone())))
            .collect())
    }

    /// Get a reverse iterator over all times in the history
    pub fn iter_rev(&self) -> PyResult<Vec<PyRaphtoryTime>> {
        Ok(self.history
            .iter_rev()
            .map(|t| PyRaphtoryTime::new(RaphtoryTime::apply(t.clone())))
            .collect())
    }

    pub fn __repr__(&self) -> String {
        format!("History(earliest={:?}, latest={:?})", 
            self.earliest_time().map(|t| t.epoch()),
            self.latest_time().map(|t| t.epoch()))
    }
}

#[pyclass(name = "RaphtoryTime", module = "raphtory", frozen)]
pub struct PyRaphtoryTime {
    time: RaphtoryTime,
}

#[pymethods]
impl PyRaphtoryTime {
    
    pub fn new(time: RaphtoryTime) -> Self {
        Self { time }
    }

    /// Get the datetime representation of the time
    pub fn dt(&self) -> Option<PyObject> {
        self.time.dt().map(|dt| {
            Python::with_gil(|py| {
                let datetime = PyDateTime::new(
                    py,
                    dt.year(),
                    dt.month() as u8,
                    dt.day() as u8,
                    dt.hour() as u8,
                    dt.minute() as u8,
                    dt.second() as u8,
                    dt.nanosecond() / 1000000, // convert to microseconds, that's what PyDateTime::new takes
                    None,
                ).unwrap();
                datetime.into_py(py)
            })
        })
    }

    /// Get the epoch timestamp of the time
    pub fn epoch(&self) -> i64 {
        self.time.epoch()
    }

    pub fn __repr__(&self) -> String {
        format!("RaphtoryTime(epoch={})", self.epoch())
    }
}

impl From<HistoryImplemented<Box<dyn InternalHistoryOps + Send + Sync>>> for PyHistory {
    fn from(history: HistoryImplemented<Box<dyn InternalHistoryOps + Send + Sync>>) -> Self {
        Self {
            history: Arc::new(history)
        }
    }
}
