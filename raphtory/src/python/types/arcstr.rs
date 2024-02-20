use crate::core::ArcStr;
use pyo3::{FromPyObject, IntoPy, PyAny, PyObject, PyResult, Python, ToPyObject};

impl IntoPy<PyObject> for ArcStr {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

impl ToPyObject for ArcStr {
    fn to_object(&self, py: Python) -> PyObject {
        self.0.to_string().to_object(py)
    }
}

impl<'source> FromPyObject<'source> for ArcStr {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        ob.extract::<String>().map(|v| v.into())
    }
}
