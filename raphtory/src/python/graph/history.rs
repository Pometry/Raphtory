use pyo3::{
    prelude::*,
    types::{PyDateTime},
};
use chrono::{Datelike, Timelike};
use crate::{
    db::api::view::history::*,
};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use raphtory_api::iter::{BoxedIter};
use crate::python::graph::edge::PyEdge;
use crate::python::graph::node::PyNode;

#[pyclass(name = "History", module = "raphtory", frozen)]
pub struct PyHistory {
    history: History<Box<dyn InternalHistoryOps>>
}

// TODO: Implement __eq__, __ne__, __lt__, ...
#[pymethods]
impl PyHistory {
    #[staticmethod]
    pub fn from_node(node: &PyNode) -> Self {
        Self {
            history: History::new(Box::new(node.node.clone())),
        }
    }

    #[staticmethod]
    pub fn from_edge(edge: &PyEdge) -> Self {
        Self {
            history: History::new(Box::new(edge.edge.clone()))
        }
    }

    /// Get the earliest time in the history
    /// Implement RaphtoryTime into PyRaphtoryTime
    pub fn earliest_time(&self) -> Option<PyRaphtoryTime> {
        self.history.earliest_time().map(|time| time.into())
    }

    /// Get the latest time in the history
    pub fn latest_time(&self) -> Option<PyRaphtoryTime> {
        self.history.latest_time().map(|time| time.into())
    }
    
    pub fn __list__(&self) -> PyResult<Vec<PyRaphtoryTime>> {
        Ok(self.history
            .iter()
            .map(|t| t.clone().into())
            .collect())
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyResult<Py<PyRaphtoryTimeIterator>> {
        // Convert to a Vec first to collect any borrowed data, then make a static iterator. Need to do this so it works with lifetimes
        // FIXME: Inefficient but I don't know what else to do
        let times: Vec<RaphtoryTime> = slf.history.iter().map(|t| t.clone().into()).collect();
        let iter = PyRaphtoryTimeIterator {
            iter: Box::new(times.into_iter())
        };
        Py::new(slf.py(), iter)
    }
    
    pub fn __repr__(&self) -> String {
        format!("History(earliest={:?}, latest={:?})", 
            self.earliest_time().map(|t| t.epoch()),
            self.latest_time().map(|t| t.epoch()))
    }
}

#[pymethods]
impl PyNode {
    fn get_history(&self) -> PyHistory {
        PyHistory {
            history: History::new(Box::new(self.node.clone()))
        }
    }
}

#[pymethods]
impl PyEdge {
    fn get_history(&self) -> PyHistory {
        PyHistory {
            history: History::new(Box::new(self.edge.clone()))
        }
    }
}

#[pyclass(name = "RaphtoryTime", module = "raphtory", frozen)]
pub struct PyRaphtoryTime {
    time: RaphtoryTime,
}

#[pymethods]
impl PyRaphtoryTime {
    /// Get the datetime representation of the time
    /// TODO: Use PyTime internal converter
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
                    dt.nanosecond() / 1000, // convert to microseconds, that's what PyDateTime::new takes
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

impl PyRaphtoryTime {
    pub fn new(time_entry: TimeIndexEntry) -> Self {
        Self { time: RaphtoryTime::new(time_entry) }
    }
}

#[pyclass(name = "RaphtoryTimeIterator", module = "raphtory")]
pub struct PyRaphtoryTimeIterator {
    iter: BoxedIter<RaphtoryTime>
}

#[pymethods]
impl PyRaphtoryTimeIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyRaphtoryTime> {
        slf.iter.next().map(|time| time.into())
    }
}

impl From<History<Box<dyn InternalHistoryOps>>> for PyHistory {
    fn from(history: History<Box<dyn InternalHistoryOps>>) -> Self {
        Self { history }
    }
}

impl From<RaphtoryTime> for PyRaphtoryTime {
    fn from(time: RaphtoryTime) -> Self {
        Self { time }
    }
}

impl From<TimeIndexEntry> for PyRaphtoryTime {
    fn from(entry: TimeIndexEntry) -> Self {
        Self { time: RaphtoryTime::new(entry) }
    }
}

impl From<BoxedIter<RaphtoryTime>> for PyRaphtoryTimeIterator {
    fn from(iter: BoxedIter<RaphtoryTime>) -> Self {
        Self { iter }
    }
}

