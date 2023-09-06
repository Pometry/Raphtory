use crate::{
    core::Prop,
    python::{graph::views::graph_view::PyGraphView, types::repr::Repr},
};
use pyo3::{
    exceptions::PyTypeError, types::PyBool, FromPyObject, IntoPy, PyAny, PyObject, PyResult, Python,
};
use std::{ops::Deref, sync::Arc};

impl IntoPy<PyObject> for Prop {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            Prop::Str(s) => s.into_py(py),
            Prop::Bool(bool) => bool.into_py(py),
            Prop::U8(u8) => u8.into_py(py),
            Prop::U16(u16) => u16.into_py(py),
            Prop::I64(i64) => i64.into_py(py),
            Prop::U64(u64) => u64.into_py(py),
            Prop::F64(f64) => f64.into_py(py),
            Prop::DTime(dtime) => dtime.into_py(py),
            Prop::Graph(g) => g.into_py(py), // Need to find a better way
            Prop::I32(v) => v.into_py(py),
            Prop::U32(v) => v.into_py(py),
            Prop::F32(v) => v.into_py(py),
            Prop::List(v) => v.deref().clone().into_py(py), // Fixme: optimise the clone here?
            Prop::Map(v) => v.deref().clone().into_py(py),
        }
    }
}

// Manually implemented to make sure we don't end up with f32/i32/u32 from python ints/floats
impl<'source> FromPyObject<'source> for Prop {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        // TODO: This no longer returns result in newer pyo3
        if ob.is_instance_of::<PyBool>()? {
            return Ok(Prop::Bool(ob.extract()?));
        }
        if let Ok(v) = ob.extract() {
            return Ok(Prop::I64(v));
        }
        if let Ok(v) = ob.extract() {
            return Ok(Prop::F64(v));
        }
        if let Ok(d) = ob.extract() {
            return Ok(Prop::DTime(d));
        }
        if let Ok(s) = ob.extract() {
            return Ok(Prop::Str(s));
        }
        if let Ok(g) = ob.extract() {
            return Ok(Prop::Graph(g));
        }
        if let Ok(list) = ob.extract() {
            return Ok(Prop::List(Arc::new(list)));
        }
        if let Ok(map) = ob.extract() {
            return Ok(Prop::Map(Arc::new(map)));
        }
        Err(PyTypeError::new_err("Not a valid property type"))
    }
}

impl Repr for Prop {
    fn repr(&self) -> String {
        match &self {
            Prop::Str(v) => v.repr(),
            Prop::Bool(v) => v.repr(),
            Prop::I64(v) => v.repr(),
            Prop::U8(v) => v.repr(),
            Prop::U16(v) => v.repr(),
            Prop::U64(v) => v.repr(),
            Prop::F64(v) => v.repr(),
            Prop::DTime(v) => v.repr(),
            Prop::Graph(g) => PyGraphView::from(g.clone()).repr(),
            Prop::I32(v) => v.repr(),
            Prop::U32(v) => v.repr(),
            Prop::F32(v) => v.repr(),
            Prop::List(v) => v.repr(),
            Prop::Map(v) => v.repr(),
        }
    }
}

pub type PropValue = Option<Prop>;
pub type PropHistItems = Vec<(i64, Prop)>;
