use std::sync::Arc;
use pyo3::{
    prelude::*,
    types::PyType,
    ToPyObject,
};
use pyo3::types::PyBool;
use crate::{
    db::api::view::history::*,
};
use raphtory_api::core::storage::timeindex::{AsTime, TimeIndexEntry};
use crate::python::graph::edge::PyEdge;
use crate::python::graph::node::PyNode;
use crate::python::types::wrappers::iterators::PyBorrowingIterator;

#[pyclass(name = "History", module = "raphtory", frozen)]
pub struct PyHistory {
    history: History<Arc<dyn InternalHistoryOps>>
}

// TODO: Implement __eq__, __ne__, __lt__, ...
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
            history: History::new(Arc::new(edge.edge.clone()))
        }
    }

    /// Get the earliest time in the history
    /// Implement RaphtoryTime into PyRaphtoryTime
    pub fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.history.earliest_time()
    }

    /// Get the latest time in the history
    pub fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.history.latest_time()
    }
    
    pub fn __list__(&self) -> Vec<TimeIndexEntry> {
        self.history.iter().collect()
    }

    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(self.history.clone(), History<Arc<dyn InternalHistoryOps>>, |history| history.iter())
    }
    
    pub fn __repr__(&self) -> String {
        format!("History(earliest={:?}, latest={:?})", 
            self.earliest_time().map(|t| t.t()),
            self.latest_time().map(|t| t.t()))
    }

    // FIXME: Fix use of deprecated to_object function. Other functions return PyBool or Borrow<PyBool> which aren't PyObject
    // PyObject is needed if we want to have NotImplemented support
    fn __eq__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            if let Ok(other_history) = other.extract::<PyRef<PyHistory>>(py) {
                let equals = self.history.eq(&other_history.history);
                Ok(equals.to_object(py))
            } else {
                // Return NotImplemented for mismatched types
                Ok(py.NotImplemented())
            }
        })
    }

    fn __ne__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            if let Ok(other_history) = other.extract::<PyRef<PyHistory>>(py) {
                let not_equals = self.history.ne(&other_history.history);
                Ok(not_equals.to_object(py))
            } else {
                // Return NotImplemented for mismatched types
                Ok(py.NotImplemented())
            }
        })
    }
}

#[pymethods]
impl PyNode {
    fn get_history(&self) -> PyHistory {
        PyHistory {
            history: History::new(Arc::new(self.node.clone()))
        }
    }
}

#[pymethods]
impl PyEdge {
    fn get_history(&self) -> PyHistory {
        PyHistory {
            history: History::new(Arc::new(self.edge.clone()))
        }
    }
}

// TODO: get rid of RaphtoryTime on the rust side and implement python class for TimeIndexEntry


impl<T: InternalHistoryOps + 'static> From<History<T>> for PyHistory {
    fn from(history: History<T>) -> Self {
        Self { history: History::new(Arc::new(history.0)) }
    }
}

impl<'py, T: InternalHistoryOps + 'static> IntoPyObject<'py> for History<T> {
    type Target = PyHistory;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyHistory::from(self).into_pyobject(py)
    }
}

