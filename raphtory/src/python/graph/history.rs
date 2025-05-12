use std::cell::RefCell;
use std::sync::Arc;
use pyo3::{
    prelude::*,
};
use crate::{
    db::api::view::history::*,
};
use raphtory_api::core::storage::timeindex::{TimeIndexEntry};
use crate::python::graph::edge::PyEdge;
use crate::python::graph::node::PyNode;
use crate::python::types::repr::{iterator_repr};
use crate::python::types::wrappers::iterators::PyBorrowingIterator;

// TODO: Do we need "frozen" here? I might need to mutate the object in my merge and compose functions
#[pyclass(name = "History", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyHistory {
    history: History<Arc<dyn InternalHistoryOps>>
}

// TODO: Implement __eq__, __ne__, __lt__, ...
#[pymethods]
impl PyHistory {
    #[staticmethod]
    pub fn from_node(node: &PyNode) -> Self {
        Self {
            history: History::new(Arc::new(node.node.clone()))
        }
    }

    #[staticmethod]
    pub fn from_edge(edge: &PyEdge) -> Self {
        Self {
            history: History::new(Arc::new(edge.edge.clone()))
        }
    }

    // FIXME: How do we wanna deal with merge and composite functions? RefCell, make the pyclass not frozen, ...
    // pub fn merge(&mut self, history: PyHistory) {
    // 
    // }

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
        format!("History({})", iterator_repr(self.history.iter()))
    }

    fn __eq__(&self, other: &PyHistory) -> bool {
        self.history.eq(&other.history)
    }

    fn __ne__(&self, other: &PyHistory) -> bool {
        self.history.ne(&other.history)
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

