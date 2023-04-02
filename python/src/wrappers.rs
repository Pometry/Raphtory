use docbrown_core as db_c;
use docbrown_db::perspective;
use docbrown_db::perspective::PerspectiveSet;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

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
            Prop::Str(string) => db_c::Prop::Str(string),
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
            db_c::Prop::Str(string) => Prop::Str(string),
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

impl From<Box<dyn Iterator<Item = u64> + Send>> for U64Iter {
    fn from(value: Box<dyn Iterator<Item = u64> + Send>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct NestedU64Iter {
    iter: Box<dyn Iterator<Item = U64Iter> + Send>,
}

#[pymethods]
impl NestedU64Iter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<U64Iter> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = U64Iter> + Send>> for NestedU64Iter {
    fn from(value: Box<dyn Iterator<Item = U64Iter> + Send>) -> Self {
        Self { iter: value }
    }
}

impl From<Box<dyn Iterator<Item = Box<dyn Iterator<Item = u64> + Send>> + Send>> for NestedU64Iter {
    fn from(value: Box<dyn Iterator<Item = Box<dyn Iterator<Item = u64> + Send>> + Send>) -> Self {
        let iter = Box::new(value.map(|iter| iter.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedUsizeIter {
    iter: Box<dyn Iterator<Item = UsizeIter> + Send>,
}

#[pymethods]
impl NestedUsizeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<UsizeIter> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = UsizeIter> + Send>> for NestedUsizeIter {
    fn from(value: Box<dyn Iterator<Item = UsizeIter> + Send>) -> Self {
        Self { iter: value }
    }
}

impl From<Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send>>
    for NestedUsizeIter
{
    fn from(
        value: Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send>,
    ) -> Self {
        let iter = Box::new(value.map(|iter| iter.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct UsizeIter {
    iter: Box<dyn Iterator<Item = usize> + Send>,
}

#[pymethods]
impl UsizeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<usize> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = usize> + Send>> for UsizeIter {
    fn from(value: Box<dyn Iterator<Item = usize> + Send>) -> Self {
        Self { iter: value }
    }
}

#[derive(Clone)]
#[pyclass(name = "Perspective")]
pub struct PyPerspective {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[pymethods]
impl PyPerspective {
    #[new]
    #[pyo3(signature = (start=None, end=None))]
    fn new(start: Option<i64>, end: Option<i64>) -> Self {
        PyPerspective { start, end }
    }

    #[staticmethod]
    #[pyo3(signature = (step, start=None, end=None))]
    fn expanding(step: u64, start: Option<i64>, end: Option<i64>) -> PyPerspectiveSet {
        PyPerspectiveSet {
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
    ) -> PyPerspectiveSet {
        PyPerspectiveSet {
            ps: perspective::Perspective::rolling(window, step, start, end),
        }
    }
}

impl From<perspective::Perspective> for PyPerspective {
    fn from(value: perspective::Perspective) -> Self {
        PyPerspective {
            start: value.start,
            end: value.end,
        }
    }
}

impl From<PyPerspective> for perspective::Perspective {
    fn from(value: PyPerspective) -> Self {
        perspective::Perspective {
            start: value.start,
            end: value.end,
        }
    }
}

#[pyclass(name = "PerspectiveSet")]
#[derive(Clone)]
pub struct PyPerspectiveSet {
    pub(crate) ps: PerspectiveSet,
}

pub fn adapt_err<U, E>(result: Result<U, E>) -> PyResult<U>
where
    E: std::error::Error,
{
    result.map_err(|e| {
        let error_log = display_error_chain::DisplayErrorChain::new(&e).to_string();
        PyException::new_err(error_log)
    })
}
