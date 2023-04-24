use crate::types::repr::Repr;
use docbrown::core as db_c;
use pyo3::{FromPyObject, IntoPy, PyObject, Python};
use std::collections::HashMap;
use std::{fmt, i64};

#[derive(FromPyObject, Debug, Clone)]
pub enum Prop {
    Str(String),
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
}

impl fmt::Display for Prop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Prop::Str(value) => write!(f, "{}", value),
            Prop::Bool(value) => write!(f, "{}", value),
            Prop::I64(value) => write!(f, "{}", value),
            Prop::U64(value) => write!(f, "{}", value),
            Prop::F64(value) => write!(f, "{}", value),
        }
    }
}

impl IntoPy<PyObject> for Prop {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            Prop::Str(s) => s.into_py(py),
            Prop::Bool(bool) => bool.into_py(py),
            Prop::I64(i64) => i64.into_py(py),
            Prop::U64(u64) => u64.into_py(py),
            Prop::F64(f64) => f64.into_py(py),
        }
    }
}

impl From<Prop> for db_c::Prop {
    fn from(prop: Prop) -> db_c::Prop {
        match prop {
            Prop::Str(string) => db_c::Prop::Str(string),
            Prop::Bool(bool) => db_c::Prop::Bool(bool),
            Prop::I64(i64) => db_c::Prop::I64(i64),
            Prop::U64(u64) => db_c::Prop::U64(u64),
            Prop::F64(f64) => db_c::Prop::F64(f64),
        }
    }
}

impl From<db_c::Prop> for Prop {
    fn from(prop: db_c::Prop) -> Prop {
        match prop {
            db_c::Prop::Str(string) => Prop::Str(string),
            db_c::Prop::Bool(bool) => Prop::Bool(bool),
            db_c::Prop::I32(i32) => Prop::I64(i32 as i64),
            db_c::Prop::I64(i64) => Prop::I64(i64),
            db_c::Prop::U32(u32) => Prop::U64(u32 as u64),
            db_c::Prop::U64(u64) => Prop::U64(u64),
            db_c::Prop::F64(f64) => Prop::F64(f64),
            db_c::Prop::F32(f32) => Prop::F64(f32 as f64),
        }
    }
}

impl Repr for db_c::Prop {
    fn repr(&self) -> String {
        match &self {
            db_c::Prop::Str(v) => v.repr(),
            db_c::Prop::Bool(v) => v.repr(),
            db_c::Prop::I64(v) => v.repr(),
            db_c::Prop::U64(v) => v.repr(),
            db_c::Prop::F64(v) => v.repr(),
            db_c::Prop::I32(v) => v.repr(),
            db_c::Prop::U32(v) => v.repr(),
            db_c::Prop::F32(v) => v.repr(),
        }
    }
}

pub struct PropValue(Option<Prop>);

impl From<Option<db_c::Prop>> for PropValue {
    fn from(value: Option<db_c::Prop>) -> Self {
        Self(value.map(|v| v.into()))
    }
}

impl IntoPy<PyObject> for PropValue {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

pub struct Props(HashMap<String, Prop>);

impl From<HashMap<String, db_c::Prop>> for Props {
    fn from(value: HashMap<String, db_c::Prop>) -> Self {
        Self(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl IntoPy<PyObject> for Props {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

pub struct PropHistory(Vec<(i64, Prop)>);

impl From<Vec<(i64, db_c::Prop)>> for PropHistory {
    fn from(value: Vec<(i64, db_c::Prop)>) -> Self {
        Self(value.into_iter().map(|(t, v)| (t, v.into())).collect())
    }
}

impl IntoPy<PyObject> for PropHistory {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

pub struct PropHistories(HashMap<String, PropHistory>);

impl From<HashMap<String, Vec<(i64, db_c::Prop)>>> for PropHistories {
    fn from(value: HashMap<String, Vec<(i64, db_c::Prop)>>) -> Self {
        Self(value.into_iter().map(|(k, h)| (k, h.into())).collect())
    }
}

impl IntoPy<PyObject> for PropHistories {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}
