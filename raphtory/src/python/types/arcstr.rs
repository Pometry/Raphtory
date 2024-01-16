use crate::core::ArcStr;
use pyo3::{FromPyObject, IntoPy, PyAny, PyObject, PyResult, Python};

impl IntoPy<PyObject> for ArcStr {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

impl<'source> FromPyObject<'source> for ArcStr {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        ob.extract::<String>().map(|v| v.into())
    }
}
