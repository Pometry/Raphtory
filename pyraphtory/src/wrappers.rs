use std::collections::HashMap;

use pyo3::prelude::*;

use dbc::tgraph_shard;
use docbrown_core as dbc;

#[pyclass]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

impl From<Direction> for dbc::Direction {
    fn from(d: Direction) -> dbc::Direction {
        match d {
            Direction::OUT => dbc::Direction::OUT,
            Direction::IN => dbc::Direction::IN,
            Direction::BOTH => dbc::Direction::BOTH,
        }
    }
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

impl From<Prop> for dbc::Prop {
    fn from(prop: Prop) -> dbc::Prop {
        match prop {
            Prop::Str(string) => dbc::Prop::Str(string.clone()),
            Prop::I32(i32) => dbc::Prop::I32(i32),
            Prop::I64(i64) => dbc::Prop::I64(i64),
            Prop::U32(u32) => dbc::Prop::U32(u32),
            Prop::U64(u64) => dbc::Prop::U64(u64),
            Prop::F32(f32) => dbc::Prop::F32(f32),
            Prop::F64(f64) => dbc::Prop::F64(f64),
            Prop::Bool(bool) => dbc::Prop::Bool(bool),
        }
    }
}

impl From<dbc::Prop> for Prop {
    fn from(prop: dbc::Prop) -> Prop {
        match prop {
            dbc::Prop::Str(string) => Prop::Str(string.clone()),
            dbc::Prop::I32(i32) => Prop::I32(i32),
            dbc::Prop::I64(i64) => Prop::I64(i64),
            dbc::Prop::U32(u32) => Prop::U32(u32),
            dbc::Prop::U64(u64) => Prop::U64(u64),
            dbc::Prop::F32(f32) => Prop::F32(f32),
            dbc::Prop::F64(f64) => Prop::F64(f64),
            dbc::Prop::Bool(bool) => Prop::Bool(bool),
        }
    }
}

#[pyclass]
pub struct TEdge {
    #[pyo3(get)]
    pub src: u64,
    #[pyo3(get)]
    pub dst: u64,
    #[pyo3(get)]
    pub t: Option<i64>,
    #[pyo3(get)]
    pub is_remote: bool,
}

impl From<tgraph_shard::TEdge> for TEdge {
    fn from(value: tgraph_shard::TEdge) -> Self {
        let tgraph_shard::TEdge {
            src,
            dst,
            t,
            is_remote,
        } = value;
        TEdge {
            src,
            dst,
            t,
            is_remote,
        }
    }
}

#[pyclass]
pub struct TVertex {
    #[pyo3(get)]
    pub g_id: u64,
    #[pyo3(get)]
    pub props: Option<HashMap<String, Vec<(i64, Prop)>>>,
}

impl From<tgraph_shard::TVertex> for TVertex {
    fn from(value: tgraph_shard::TVertex) -> TVertex {
        let tgraph_shard::TVertex {
            g_id,
            props: maybe_props,
            ..
        } = value;

        if let Some(props) = maybe_props {
            let vs = props
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.iter()
                            .map(move |(t, p)| (*t, (*p).clone().into()))
                            .collect::<Vec<(i64, Prop)>>(),
                    )
                })
                .collect::<HashMap<String, Vec<(i64, Prop)>>>();
            TVertex {
                g_id,
                props: Some(vs),
            }
        } else {
            TVertex { g_id, props: None }
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
pub struct VertexIterator {
    pub(crate) iter: Box<dyn Iterator<Item = TVertex> + Send>,
}

#[pymethods]
impl VertexIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<TVertex> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct EdgeIterator {
    pub(crate) iter: Box<dyn Iterator<Item = TEdge> + Send>,
}

#[pymethods]
impl EdgeIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<TEdge> {
        slf.iter.next()
    }
}
