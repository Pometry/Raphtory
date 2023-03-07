use pyo3::prelude::*;
use std::borrow::BorrowMut;

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
pub struct WindowedVertexIterator {
    pub(crate) iter: Box<dyn Iterator<Item = WindowedVertex> + Send>,
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
                let op0 = ops_iter
                    .next()
                    .expect("need to have an operation to get here");
                let v = g.graph_w.vertex(g_id).expect("should exist");
                match op0 {
                    Operations::OutNeighbours => v.out_neighbours(),
                    Operations::InNeighbours => v.in_neighbours(),
                    Operations::Neighbours => v.neighbours(),
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
    fn __iter__(slf: PyRef<'_, Self>, py: Python) -> WindowedVertexIterator {
        let iter = slf.build_iterator(py);
        let g = slf.graph.clone_ref(py);
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
