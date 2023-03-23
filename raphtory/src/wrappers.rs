use itertools::Itertools;
use pyo3::prelude::*;
use pyo3::types::PyInt;
use std::borrow::{Borrow, BorrowMut};

use db_c::tgraph_shard;
use docbrown_core as db_c;
use docbrown_db as db_db;
use docbrown_db::view_api::*;
use docbrown_db::{graph_window, perspective};

use crate::graph_window::{WindowedEdge, WindowedGraph, WindowedVertex};

#[derive(Copy, Clone)]
pub(crate) enum Direction {
    OUT,
    IN,
    BOTH,
    OutWindow { t_start: i64, t_end: i64 },
    InWindow { t_start: i64, t_end: i64 },
    BothWindow { t_start: i64, t_end: i64 },
}

impl From<Direction> for db_c::Direction {
    fn from(d: Direction) -> db_c::Direction {
        match d {
            Direction::OUT => db_c::Direction::OUT,
            Direction::IN => db_c::Direction::IN,
            Direction::BOTH => db_c::Direction::BOTH,
            Direction::OutWindow { t_start, t_end } => db_c::Direction::OUT,
            Direction::InWindow { t_start, t_end } => db_c::Direction::IN,
            Direction::BothWindow { t_start, t_end } => db_c::Direction::BOTH,
        }
    }
}

#[derive(Copy, Clone)]
pub(crate) enum Operations {
    OutNeighbours,
    InNeighbours,
    Neighbours,
    InNeighboursWindow { t_start: i64, t_end: i64 },
    OutNeighboursWindow { t_start: i64, t_end: i64 },
    NeighboursWindow { t_start: i64, t_end: i64 },
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

    fn id(slf: PyRef<'_, Self>, py: Python) -> PyResult<IdIterable> {
        let vertex_iter = Py::new(
            py,
            WindowedVertexIterable {
                graph: slf.graph.clone(),
                operations: vec![],
                start_at: None,
            },
        )?;
        Ok(IdIterable { vertex_iter })
    }

    fn out_neighbours(
        mut slf: PyRefMut<'_, Self>,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> WindowedVerticesPath {
        WindowedVerticesPath {
            graph: slf.graph.clone(),
            operations: vec![match (t_start, t_end) {
                (None, None) => Operations::OutNeighbours,
                _ => Operations::OutNeighboursWindow {
                    t_start: t_start.unwrap_or(0),
                    t_end: t_end.unwrap_or(0),
                },
            }],
        }
    }

    fn in_neighbours(
        mut slf: PyRefMut<'_, Self>,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> WindowedVerticesPath {
        WindowedVerticesPath {
            graph: slf.graph.clone(),
            operations: vec![match (t_start, t_end) {
                (None, None) => Operations::InNeighbours,
                _ => Operations::InNeighboursWindow {
                    t_start: t_start.unwrap_or(0),
                    t_end: t_end.unwrap_or(0),
                },
            }],
        }
    }

    fn neighbours(
        mut slf: PyRefMut<'_, Self>,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> WindowedVerticesPath {
        WindowedVerticesPath {
            graph: slf.graph.clone(),
            operations: vec![match (t_start, t_end) {
                (None, None) => Operations::Neighbours,
                _ => Operations::NeighboursWindow {
                    t_start: t_start.unwrap_or(0),
                    t_end: t_end.unwrap_or(0),
                },
            }],
        }
    }

    fn in_degree(
        slf: PyRef<'_, Self>,
        py: Python,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> PyResult<DegreeIterable> {
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
            operation: match (t_start, t_end) {
                (None, None) => Direction::IN,
                _ => Direction::InWindow {
                    t_start: t_start.unwrap_or(i64::MIN),
                    t_end: t_end.unwrap_or(i64::MAX),
                },
            },
        })
    }

    fn out_degree(
        slf: PyRef<'_, Self>,
        py: Python,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> PyResult<DegreeIterable> {
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
            operation: match (t_start, t_end) {
                (None, None) => Direction::OUT,
                _ => Direction::OutWindow {
                    t_start: t_start.unwrap_or(i64::MIN),
                    t_end: t_end.unwrap_or(i64::MAX),
                },
            },
        })
    }

    fn degree(
        slf: PyRef<'_, Self>,
        py: Python,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> PyResult<DegreeIterable> {
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
            operation: match (t_start, t_end) {
                (None, None) => Direction::BOTH,
                _ => Direction::BothWindow {
                    t_start: t_start.unwrap_or(i64::MIN),
                    t_end: t_end.unwrap_or(i64::MAX),
                },
            },
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
            start_at: Some(v.id()),
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

    fn id(slf: PyRef<'_, Self>) -> NestedIdIterable {
        NestedIdIterable {
            vertex_iter: slf.into(),
        }
    }

    fn out_neighbours(
        mut slf: PyRefMut<'_, Self>,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> PyRefMut<'_, Self> {
        slf.operations.push(match (t_start, t_end) {
            (None, None) => Operations::OutNeighbours,
            _ => Operations::OutNeighboursWindow {
                t_start: t_start.unwrap_or(i64::MIN),
                t_end: t_end.unwrap_or(i64::MAX),
            },
        });
        slf
    }

    fn in_neighbours(
        mut slf: PyRefMut<'_, Self>,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> PyRefMut<'_, Self> {
        slf.operations.push(match (t_start, t_end) {
            (None, None) => Operations::InNeighbours,
            _ => Operations::InNeighboursWindow {
                t_start: t_start.unwrap_or(i64::MIN),
                t_end: t_end.unwrap_or(i64::MAX),
            },
        });
        slf
    }

    fn neighbours(
        mut slf: PyRefMut<'_, Self>,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> PyRefMut<'_, Self> {
        slf.operations.push(match (t_start, t_end) {
            (None, None) => Operations::Neighbours,
            _ => Operations::NeighboursWindow {
                t_start: t_start.unwrap_or(i64::MIN),
                t_end: t_end.unwrap_or(i64::MAX),
            },
        });
        slf
    }

    fn in_degree(
        slf: PyRef<'_, Self>,
        py: Python,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> NestedDegreeIterable {
        NestedDegreeIterable {
            vertex_iter: slf.into(),
            operation: match (t_start, t_end) {
                (None, None) => Direction::IN,
                _ => Direction::InWindow {
                    t_start: t_start.unwrap_or(i64::MIN),
                    t_end: t_end.unwrap_or(i64::MAX),
                },
            },
        }
    }

    fn out_degree(
        slf: PyRef<'_, Self>,
        py: Python,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> NestedDegreeIterable {
        NestedDegreeIterable {
            vertex_iter: slf.into(),
            operation: match (t_start, t_end) {
                (None, None) => Direction::OUT,
                _ => Direction::OutWindow {
                    t_start: t_start.unwrap_or(i64::MIN),
                    t_end: t_end.unwrap_or(i64::MAX),
                },
            },
        }
    }

    fn degree(
        slf: PyRef<'_, Self>,
        py: Python,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> NestedDegreeIterable {
        NestedDegreeIterable {
            vertex_iter: slf.into(),
            operation: match (t_start, t_end) {
                (None, None) => Direction::BOTH,
                _ => Direction::BothWindow {
                    t_start: t_start.unwrap_or(i64::MIN),
                    t_end: t_end.unwrap_or(i64::MAX),
                },
            },
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
                    Operations::InNeighboursWindow { t_start, t_end } => {
                        vertex.in_neighbours_window(*t_start, *t_end)
                    }
                    Operations::OutNeighboursWindow { t_start, t_end } => {
                        vertex.out_neighbours_window(*t_start, *t_end)
                    }
                    Operations::NeighboursWindow { t_start, t_end } => {
                        vertex.neighbours_window(*t_start, *t_end)
                    }
                }
            }
        };

        for op in ops_iter {
            iter = match op {
                Operations::OutNeighbours => iter.out_neighbours(),
                Operations::InNeighbours => iter.in_neighbours(),
                Operations::Neighbours => iter.neighbours(),
                Operations::InNeighboursWindow { t_start, t_end } => {
                    iter.in_neighbours_window(*t_start, *t_end)
                }
                Operations::OutNeighboursWindow { t_start, t_end } => {
                    iter.out_neighbours_window(*t_start, *t_end)
                }
                Operations::NeighboursWindow { t_start, t_end } => {
                    iter.neighbours_window(*t_start, *t_end)
                }
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

    fn id(slf: PyRef<'_, Self>) -> IdIterable {
        let vertex_iter = slf.into();
        IdIterable { vertex_iter }
    }

    fn out_neighbours(
        mut slf: PyRefMut<'_, Self>,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> PyRefMut<'_, Self> {
        slf.operations.push(match (t_start, t_end) {
            (None, None) => Operations::OutNeighbours,
            _ => Operations::OutNeighboursWindow {
                t_start: t_start.unwrap_or(i64::MIN),
                t_end: t_end.unwrap_or(i64::MAX),
            },
        });
        slf
    }

    fn in_neighbours(
        mut slf: PyRefMut<'_, Self>,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> PyRefMut<'_, Self> {
        slf.operations.push(match (t_start, t_end) {
            (None, None) => Operations::InNeighbours,
            _ => Operations::InNeighboursWindow {
                t_start: t_start.unwrap_or(i64::MIN),
                t_end: t_end.unwrap_or(i64::MAX),
            },
        });
        slf
    }

    fn neighbours(
        mut slf: PyRefMut<'_, Self>,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> PyRefMut<'_, Self> {
        slf.operations.push(match (t_start, t_end) {
            (None, None) => Operations::Neighbours,
            _ => Operations::NeighboursWindow {
                t_start: t_start.unwrap_or(i64::MIN),
                t_end: t_end.unwrap_or(i64::MAX),
            },
        });
        slf
    }

    fn in_degree(slf: PyRef<'_, Self>, t_start: Option<i64>, t_end: Option<i64>) -> DegreeIterable {
        DegreeIterable {
            vertex_iter: slf.into(),
            operation: match (t_start, t_end) {
                (None, None) => Direction::IN,
                _ => Direction::InWindow {
                    t_start: t_start.unwrap_or(i64::MIN),
                    t_end: t_end.unwrap_or(i64::MAX),
                },
            },
        }
    }

    fn out_degree(
        slf: PyRef<'_, Self>,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> DegreeIterable {
        DegreeIterable {
            vertex_iter: slf.into(),
            operation: match (t_start, t_end) {
                (None, None) => Direction::OUT,
                _ => Direction::OutWindow {
                    t_start: t_start.unwrap_or(i64::MIN),
                    t_end: t_end.unwrap_or(i64::MAX),
                },
            },
        }
    }

    fn degree(slf: PyRef<'_, Self>, t_start: Option<i64>, t_end: Option<i64>) -> DegreeIterable {
        DegreeIterable {
            vertex_iter: slf.into(),
            operation: match (t_start, t_end) {
                (None, None) => Direction::BOTH,
                _ => Direction::BothWindow {
                    t_start: t_start.unwrap_or(i64::MIN),
                    t_end: t_end.unwrap_or(i64::MAX),
                },
            },
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
pub struct IdIterable {
    vertex_iter: Py<WindowedVertexIterable>,
}

#[pymethods]
impl IdIterable {
    fn __iter__(&self, py: Python) -> U64Iter {
        let iter = Box::new(
            self.vertex_iter
                .borrow(py)
                .build_iterator(py)
                .map(|v| v.id()),
        );
        U64Iter { iter }
    }
}

#[pyclass]
pub struct NestedIdIterable {
    vertex_iter: Py<WindowedVerticesPath>,
}

#[pymethods]
impl NestedIdIterable {
    fn __iter__(&self, py: Python) -> NestedIdIter {
        let inner = self.vertex_iter.borrow(py);
        let iter = inner.build_iterator(py);
        let iter2 = Box::new(iter.map(move |iterable| {
            let v_it = Python::with_gil(|py| Py::new(py, iterable))?;
            Ok(IdIterable { vertex_iter: v_it })
        }));
        NestedIdIter { iter: iter2 }
    }
}

#[pyclass]
pub struct NestedIdIter {
    iter: Box<dyn Iterator<Item = PyResult<IdIterable>> + Send>,
}

#[pymethods]
impl NestedIdIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<IdIterable>> {
        slf.iter.next().transpose()
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
            Direction::OUT => iter.out_degree(),
            Direction::IN => iter.in_degree(),
            Direction::BOTH => iter.degree(),
            Direction::OutWindow { t_start, t_end } => iter.out_degree_window(t_start, t_end),
            Direction::InWindow { t_start, t_end } => iter.in_degree_window(t_start, t_end),
            Direction::BothWindow { t_start, t_end } => iter.degree_window(t_start, t_end),
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
        let op = self.operation;
        let iter2 = Box::new(iter.map(move |iterable| {
            let v_it = Python::with_gil(|py| Py::new(py, iterable))?;
            Ok(DegreeIterable {
                vertex_iter: v_it,
                operation: op,
            })
        }));
        NestedUsizeIter { iter: iter2 }
    }
}

#[pyclass]
pub struct NestedUsizeIter {
    iter: Box<dyn Iterator<Item = PyResult<DegreeIterable>> + Send>,
}

#[pymethods]
impl NestedUsizeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<DegreeIterable>> {
        slf.iter.next().transpose()
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
pub struct U64Iter {
    iter: Box<dyn Iterator<Item = u64> + Send>,
}

#[pymethods]
impl U64Iter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<u64> {
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

#[derive(Clone)]
#[pyclass]
pub struct Perspective {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[pymethods]
impl Perspective {
    #[new]
    #[pyo3(signature = (start=None, end=None))]
    fn new(start: Option<i64>, end: Option<i64>) -> Self {
        Perspective { start, end }
    }

    #[staticmethod]
    #[pyo3(signature = (step, start=None, end=None))]
    fn expanding(step: u64, start: Option<i64>, end: Option<i64>) -> PerspectiveSet {
        PerspectiveSet {
            ps: perspective::Perspective::expanding(step, start, end),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (window, step=None, start=None, end=None))]
    fn rolling(
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PerspectiveSet {
        PerspectiveSet {
            ps: perspective::Perspective::rolling(window, step, start, end),
        }
    }
}

impl From<perspective::Perspective> for Perspective {
    fn from(value: perspective::Perspective) -> Self {
        Perspective {
            start: value.start,
            end: value.end,
        }
    }
}

impl From<Perspective> for perspective::Perspective {
    fn from(value: Perspective) -> Self {
        perspective::Perspective {
            start: value.start,
            end: value.end,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PerspectiveSet {
    pub(crate) ps: perspective::PerspectiveSet,
}
