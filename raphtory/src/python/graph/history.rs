use std::cell::RefCell;
use std::sync::Arc;
use pyo3::{
    prelude::*,
};
use crate::{
    db::api::view::history::*,
};
use raphtory_api::core::storage::timeindex::{TimeIndexEntry};
use crate::prelude::EdgeViewOps;
use crate::python::graph::edge::PyEdge;
use crate::python::graph::node::PyNode;
use crate::python::types::repr::{iterator_repr};
use crate::python::types::wrappers::iterators::PyBorrowingIterator;

#[pyclass(name = "History", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyHistory {
    history: History<Arc<dyn InternalHistoryOps>>
}

// TODO: Implement __lt__, __gt__, ...?
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

    // FIXME: potentially implement this using dynamic type checking.
    // FIXME: if all items in vec are "dyn InternalHistoryOps", create object, or else return error/message
    // #[staticmethod]
    // pub fn compose_from_items(objects: Vec<Bound<'_, PyAny>>) -> PyResult<Self> {
    //     let mut items: Vec<Arc<dyn InternalHistoryOps>> = Vec::new();
    //
    //     for obj in objects {
    //         let item = obj.get();
    //
    //         // Try to extract as PyNode
    //         if let Ok(node) = item.extract::<PyRef<PyNode>>() {
    //             items.push(Arc::new(node.node.clone()));
    //             continue;
    //         }
    //
    //         // Try to extract as PyEdge
    //         if let Ok(edge) = item.extract::<PyRef<PyEdge>>() {
    //             items.push(Arc::new(edge.edge.clone()));
    //             continue;
    //         }
    //
    //         // Try to extract as PyHistory
    //         if let Ok(history) = item.extract::<PyRef<PyHistory>>() {
    //             // Clone the Arc to avoid unsafe code
    //             let hist_clone = Arc::clone(&history.history.0);
    //             items.push(Arc::new(hist_clone));
    //             continue;
    //         }
    //
    //         // If we got here, the item isn't supported
    //         return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
    //             format!("Expected Node, Edge, or History, got: {}", item.get_type().name()?)
    //         ));
    //     }
    //
    //     Ok(Self {
    //         history: History::new(Arc::new(CompositeHistory::new(items)))
    //     })
    // }

    #[staticmethod]
    pub fn compose_from_histories(objects: Vec<PyHistory>) -> Self {
        // the only way to get History objects from python is if they are already Arc<...>
        let underlying_objects: Vec<Arc<dyn InternalHistoryOps>> =
            objects
                .into_iter()
                .map(|obj| Arc::clone(&obj.history.0))
                .collect();
        Self {
            history: History::new(Arc::new(CompositeHistory::new(underlying_objects)))
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

