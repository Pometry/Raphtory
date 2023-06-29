use crate::core::Prop;
use crate::python::graph_view::PyGraphView;
use crate::python::types::repr::Repr;
use pyo3::exceptions::PyTypeError;
use pyo3::{FromPyObject, IntoPy, PyAny, PyObject, PyResult, Python};
use std::collections::HashMap;

impl IntoPy<PyObject> for Prop {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            Prop::Str(s) => s.into_py(py),
            Prop::Bool(bool) => bool.into_py(py),
            Prop::I64(i64) => i64.into_py(py),
            Prop::U64(u64) => u64.into_py(py),
            Prop::F64(f64) => f64.into_py(py),
            Prop::DTime(dtime) => dtime.into_py(py),
            Prop::Graph(g) => g.into_py(py), // Need to find a better way
            Prop::I32(v) => v.into_py(py),
            Prop::U32(v) => v.into_py(py),
            Prop::F32(v) => v.into_py(py),
        }
    }
}

// Manually implemented to make sure we don't end up with f32/i32/u32 from python ints/floats
impl<'source> FromPyObject<'source> for Prop {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(v) = ob.extract() {
            return Ok(Prop::I64(v));
        }
        if let Ok(v) = ob.extract() {
            return Ok(Prop::F64(v));
        }
        if let Ok(d) = ob.extract() {
            return Ok(Prop::DTime(d));
        }
        if let Ok(b) = ob.extract() {
            return Ok(Prop::Bool(b));
        }
        if let Ok(s) = ob.extract() {
            return Ok(Prop::Str(s));
        }
        if let Ok(g) = ob.extract() {
            return Ok(Prop::Graph(g));
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
            Prop::U64(v) => v.repr(),
            Prop::F64(v) => v.repr(),
            Prop::DTime(v) => v.repr(),
            Prop::Graph(g) => PyGraphView::from(g.clone()).repr(),
            Prop::I32(v) => v.repr(),
            Prop::U32(v) => v.repr(),
            Prop::F32(v) => v.repr(),
        }
    }
}

pub type PropValue = Option<Prop>;
pub type Props = HashMap<String, Prop>;
pub type PropHistory = Vec<(i64, Prop)>;
pub type PropHistories = HashMap<String, PropHistory>;
