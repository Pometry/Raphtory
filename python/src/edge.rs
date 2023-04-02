use crate::dynamic::DynamicGraph;
use crate::vertex::PyVertex;
use crate::wrappers::Prop;
use docbrown_db::edge::EdgeView;
use itertools::Itertools;
use pyo3::{pyclass, pymethods, PyRef, PyRefMut};

#[pyclass(name = "Edge")]
pub struct PyEdge {
    pub(crate) edge: EdgeView<DynamicGraph>,
}

impl From<EdgeView<DynamicGraph>> for PyEdge {
    fn from(value: EdgeView<DynamicGraph>) -> Self {
        Self { edge: value }
    }
}

#[pymethods]
impl PyEdge {
    pub fn __getitem__(&self, name: String) -> Vec<(i64, Prop)> {
        self.prop(name)
    }

    pub fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.edge
            .prop(name)
            .into_iter()
            .map(|(t, p)| (t, p.into()))
            .collect_vec()
    }

    pub fn id(&self) -> usize {
        self.edge.id()
    }

    fn src(&self) -> PyVertex {
        self.edge.src().into()
    }

    fn dst(&self) -> PyVertex {
        self.edge.dst().into()
    }
}

#[pyclass(name = "EdgeIterator")]
pub struct PyEdgeIter {
    iter: Box<dyn Iterator<Item = PyEdge> + Send>,
}

#[pymethods]
impl PyEdgeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyEdge> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = PyEdge> + Send>> for PyEdgeIter {
    fn from(value: Box<dyn Iterator<Item = PyEdge> + Send>) -> Self {
        Self { iter: value }
    }
}

impl From<Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send>> for PyEdgeIter {
    fn from(value: Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|e| e.into())),
        }
    }
}
