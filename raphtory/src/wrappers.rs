use itertools::Itertools;
use pyo3::prelude::*;
use std::borrow::{Borrow, BorrowMut};

use db_c::tgraph_shard;
use docbrown_core as db_c;
use docbrown_db as db_db;
use docbrown_db::graph_window;

use crate::graph_window::{WindowedEdge, WindowedGraph, WindowedVertex};

#[pyclass]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

impl From<Direction> for db_c::Direction {
    fn from(d: Direction) -> db_c::Direction {
        match d {
            Direction::OUT => db_c::Direction::OUT,
            Direction::IN => db_c::Direction::IN,
            Direction::BOTH => db_c::Direction::BOTH,
        }
    }
}

#[derive(Copy, Clone)]
pub(crate) enum Operations {
    OutNeighbours,
    InNeighbours,
    Neighbours,
}

#[derive(FromPyObject, Debug, Clone)]
pub enum Prop {
    Str(String),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Bool(bool),
}

impl IntoPy<PyObject> for Prop {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            Prop::Str(s) => s.into_py(py),
            Prop::I32(i32) => i32.into_py(py),
            Prop::I64(i64) => i64.into_py(py),
            Prop::U32(u32) => u32.into_py(py),
            Prop::U64(u64) => u64.into_py(py),
            Prop::F32(f32) => f32.into_py(py),
            Prop::F64(f64) => f64.into_py(py),
            Prop::Bool(bool) => bool.into_py(py),
        }
    }
}

impl From<Prop> for db_c::Prop {
    fn from(prop: Prop) -> db_c::Prop {
        match prop {
            Prop::Str(string) => db_c::Prop::Str(string.clone()),
            Prop::I32(i32) => db_c::Prop::I32(i32),
            Prop::I64(i64) => db_c::Prop::I64(i64),
            Prop::U32(u32) => db_c::Prop::U32(u32),
            Prop::U64(u64) => db_c::Prop::U64(u64),
            Prop::F32(f32) => db_c::Prop::F32(f32),
            Prop::F64(f64) => db_c::Prop::F64(f64),
            Prop::Bool(bool) => db_c::Prop::Bool(bool),
        }
    }
}

impl From<db_c::Prop> for Prop {
    fn from(prop: db_c::Prop) -> Prop {
        match prop {
            db_c::Prop::Str(string) => Prop::Str(string.clone()),
            db_c::Prop::I32(i32) => Prop::I32(i32),
            db_c::Prop::I64(i64) => Prop::I64(i64),
            db_c::Prop::U32(u32) => Prop::U32(u32),
            db_c::Prop::U64(u64) => Prop::U64(u64),
            db_c::Prop::F32(f32) => Prop::F32(f32),
            db_c::Prop::F64(f64) => Prop::F64(f64),
            db_c::Prop::Bool(bool) => Prop::Bool(bool),
        }
    }
}

#[pyclass]
pub struct VertexIdsIterator {
    pub(crate) iter: Box<dyn Iterator<Item = u64> + Send>,
}

#[pymethods]
impl VertexIdsIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<u64> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct WindowedVertices {
    pub(crate) graph: Py<WindowedGraph>,
}

#[pymethods]
impl WindowedVertices {
    fn __iter__(&self, py: Python) -> WindowedVertexIterator {
        let g = self.graph.borrow(py);
        let g_py = self.graph.clone_ref(py);
        WindowedVertexIterator {
            iter: Box::new(
                g.graph_w
                    .vertices()
                    .map(move |v| WindowedVertex::new(g_py.clone(), v)),
            ),
        }
    }

    fn out_neighbours(mut slf: PyRefMut<'_, Self>) -> WindowedVerticesPath {
        WindowedVerticesPath {
            graph: slf.graph.clone(),
            operations: vec![Operations::OutNeighbours],
        }
    }

    fn in_neighbours(mut slf: PyRefMut<'_, Self>) -> WindowedVerticesPath {
        WindowedVerticesPath {
            graph: slf.graph.clone(),
            operations: vec![Operations::InNeighbours],
        }
    }

    fn neighbours(mut slf: PyRefMut<'_, Self>) -> WindowedVerticesPath {
        WindowedVerticesPath {
            graph: slf.graph.clone(),
            operations: vec![Operations::Neighbours],
        }
    }

    fn in_degree(slf: PyRef<'_, Self>, py: Python) -> PyResult<DegreeIterable> {
        let vertex_iter = Py::new(
            py,
            WindowedVertexIterable {
                graph: slf.graph.clone(),
                operations: vec![],
                start_at: None,
            },
        )?;
        Ok(DegreeIterable {
            vertex_iter,
            operation: Direction::IN,
        })
    }

    fn out_degree(slf: PyRef<'_, Self>, py: Python) -> PyResult<DegreeIterable> {
        let vertex_iter = Py::new(
            py,
            WindowedVertexIterable {
                graph: slf.graph.clone(),
                operations: vec![],
                start_at: None,
            },
        )?;
        Ok(DegreeIterable {
            vertex_iter,
            operation: Direction::OUT,
        })
    }

    fn degree(slf: PyRef<'_, Self>, py: Python) -> PyResult<DegreeIterable> {
        let vertex_iter = Py::new(
            py,
            WindowedVertexIterable {
                graph: slf.graph.clone(),
                operations: vec![],
                start_at: None,
            },
        )?;
        Ok(DegreeIterable {
            vertex_iter,
            operation: Direction::BOTH,
        })
    }

    fn __repr__(&self, py: Python) -> String {
        let values = self
            .__iter__(py)
            .iter
            .take(11)
            .map(|v| v.__repr__())
            .collect_vec();
        if values.len() < 11 {
            "WindowedVertices(".to_string() + &values.join(", ") + ")"
        } else {
            "WindowedVertices(".to_string() + &values.join(", ") + " ... )"
        }
    }
}

#[pyclass]
pub struct WindowedVerticesPath {
    pub(crate) graph: Py<WindowedGraph>,
    pub(crate) operations: Vec<Operations>,
}

impl WindowedVerticesPath {
    fn build_iterator(
        &self,
        py: Python,
    ) -> Box<dyn Iterator<Item = WindowedVertexIterable> + Send> {
        let g = self.graph.borrow(py);
        let g_py = self.graph.clone_ref(py);
        let ops = self.operations.clone();
        // don't capture self inside the closure!
        Box::new(g.graph_w.vertices().map(move |v| WindowedVertexIterable {
            graph: g_py.clone(),
            operations: ops.clone(),
            start_at: Some(v.g_id),
        }))
    }
}

#[pymethods]
impl WindowedVerticesPath {
    fn __iter__(&self, py: Python) -> NestedVertexIterator {
        let iter = self.build_iterator(py);
        NestedVertexIterator {
            iter: Box::new(iter),
        }
    }

    fn out_neighbours(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.operations.push(Operations::OutNeighbours);
        slf
    }

    fn in_neighbours(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.operations.push(Operations::InNeighbours);
        slf
    }

    fn neighbours(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.operations.push(Operations::Neighbours);
        slf
    }

    fn in_degree(slf: PyRef<'_, Self>, py: Python) -> NestedDegreeIterable {
        NestedDegreeIterable {
            vertex_iter: slf.into(),
            operation: Direction::IN,
        }
    }

    fn out_degree(slf: PyRef<'_, Self>, py: Python) -> NestedDegreeIterable {
        NestedDegreeIterable {
            vertex_iter: slf.into(),
            operation: Direction::OUT,
        }
    }

    fn degree(slf: PyRef<'_, Self>, py: Python) -> NestedDegreeIterable {
        NestedDegreeIterable {
            vertex_iter: slf.into(),
            operation: Direction::BOTH,
        }
    }

    fn __repr__(&self, py: Python) -> String {
        let values = self
            .__iter__(py)
            .iter
            .take(11)
            .map(|v| v.__repr__(py))
            .collect_vec();
        if values.len() < 11 {
            "WindowedVerticesPath(".to_string() + &values.join(", ") + ")"
        } else {
            "WindowedVerticesPath(".to_string() + &values.join(", ") + " ... )"
        }
    }
}

#[pyclass]
pub struct WindowedVertexIterable {
    pub(crate) graph: Py<WindowedGraph>,
    pub(crate) operations: Vec<Operations>,
    pub(crate) start_at: Option<u64>,
}

impl WindowedVertexIterable {
    fn build_iterator(
        &self,
        py: Python,
    ) -> Box<dyn Iterator<Item = graph_window::WindowedVertex> + Send> {
        let g = self.graph.borrow(py);
        let mut ops_iter = self.operations.iter();
        let mut iter = match self.start_at {
            None => g.graph_w.vertices(),
            Some(g_id) => {
                let vertex = g.graph_w.vertex(g_id).expect("should exist");
                let op0 = ops_iter
                    .next()
                    .expect("need to have an operation to get here");
                match op0 {
                    Operations::OutNeighbours => vertex.out_neighbours(),
                    Operations::InNeighbours => vertex.in_neighbours(),
                    Operations::Neighbours => vertex.neighbours(),
                }
            }
        };

        for op in ops_iter {
            iter = match op {
                Operations::OutNeighbours => Box::new(iter.flat_map(|v| v.out_neighbours())),
                Operations::InNeighbours => Box::new(iter.flat_map(|v| v.in_neighbours())),
                Operations::Neighbours => Box::new(iter.flat_map(|v| v.neighbours())),
            }
        }
        iter
    }
}

#[pymethods]
impl WindowedVertexIterable {
    fn __iter__(&self, py: Python) -> WindowedVertexIterator {
        let iter = self.build_iterator(py);
        let g = self.graph.clone_ref(py);
        WindowedVertexIterator {
            iter: Box::new(iter.map(move |v| WindowedVertex::new(g.clone(), v))),
        }
    }

    fn out_neighbours(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.operations.push(Operations::OutNeighbours);
        slf
    }

    fn in_neighbours(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.operations.push(Operations::InNeighbours);
        slf
    }

    fn neighbours(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.operations.push(Operations::Neighbours);
        slf
    }

    fn in_degree(slf: PyRef<'_, Self>) -> DegreeIterable {
        let vertex_iter = slf.into();
        DegreeIterable {
            vertex_iter,
            operation: Direction::IN,
        }
    }

    fn out_degree(slf: PyRef<'_, Self>) -> DegreeIterable {
        let vertex_iter = slf.into();
        DegreeIterable {
            vertex_iter,
            operation: Direction::OUT,
        }
    }

    fn degree(slf: PyRef<'_, Self>) -> DegreeIterable {
        let vertex_iter = slf.into();
        DegreeIterable {
            vertex_iter,
            operation: Direction::BOTH,
        }
    }

    fn __repr__(&self, py: Python) -> String {
        let values = self
            .__iter__(py)
            .iter
            .take(11)
            .map(|v| v.__repr__())
            .collect_vec();
        if values.len() < 11 {
            "WindowedVertexIterable(".to_string() + &values.join(", ") + ")"
        } else {
            "WindowedVertexIterable(".to_string() + &values.join(", ") + " ... )"
        }
    }
}

#[pyclass]
pub struct DegreeIterable {
    vertex_iter: Py<WindowedVertexIterable>,
    operation: Direction,
}

impl DegreeIterable {
    fn build_iterator(&self, py: Python) -> Box<dyn Iterator<Item = usize> + Send> {
        let inner = self.vertex_iter.borrow(py);
        let iter = inner.build_iterator(py);
        match self.operation {
            Direction::OUT => Box::new(iter.map(|v| v.out_degree())),
            Direction::IN => Box::new(iter.map(|v| v.in_degree())),
            Direction::BOTH => Box::new(iter.map(|v| v.degree())),
        }
    }
}

#[pymethods]
impl DegreeIterable {
    fn __iter__(&self, py: Python) -> USizeIter {
        USizeIter {
            iter: self.build_iterator(py),
        }
    }
}

#[pyclass]
pub struct NestedDegreeIterable {
    vertex_iter: Py<WindowedVerticesPath>,
    operation: Direction,
}

#[pymethods]
impl NestedDegreeIterable {
    fn __iter__(&self, py: Python) -> NestedUsizeIter {
        let inner = self.vertex_iter.borrow(py);
        let iter = inner.build_iterator(py);
        let op = self.operation.clone();
        NestedUsizeIter {
            iter: Box::new(iter.map(move |iterable| {
                let v_it = Python::with_gil(|py| Py::new(py, iterable)).unwrap();
                DegreeIterable {
                    vertex_iter: v_it,
                    operation: op,
                }
            })),
        }
    }
}

#[pyclass]
pub struct NestedUsizeIter {
    iter: Box<dyn Iterator<Item = DegreeIterable> + Send>,
}

#[pymethods]
impl NestedUsizeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<DegreeIterable> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct USizeIter {
    iter: Box<dyn Iterator<Item = usize> + Send>,
}

#[pymethods]
impl USizeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<usize> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct WindowedVertexIterator {
    pub(crate) iter: Box<dyn Iterator<Item = WindowedVertex> + Send>,
}

#[pymethods]
impl WindowedVertexIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<WindowedVertex> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct NestedVertexIterator {
    pub(crate) iter: Box<dyn Iterator<Item = WindowedVertexIterable> + Send>,
}

#[pymethods]
impl NestedVertexIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<WindowedVertexIterable> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct WindowedEdgeIterator {
    pub(crate) iter: Box<dyn Iterator<Item = WindowedEdge> + Send>,
}

#[pymethods]
impl WindowedEdgeIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<WindowedEdge> {
        slf.iter.next()
    }
}
