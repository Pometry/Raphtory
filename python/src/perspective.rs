use docbrown::db::perspective;
use docbrown::db::perspective::PerspectiveSet;
use pyo3::{pyclass, pymethods};
use std::i64;

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
