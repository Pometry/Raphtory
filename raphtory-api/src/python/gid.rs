use crate::core::entities::GID;
use pyo3::{exceptions::PyTypeError, prelude::*};

impl IntoPy<PyObject> for GID {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            GID::U64(v) => v.into_py(py),
            GID::Str(v) => v.into_py(py),
        }
    }
}

impl ToPyObject for GID {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            GID::U64(v) => v.to_object(py),
            GID::Str(v) => v.to_object(py),
        }
    }
}

impl<'source> FromPyObject<'source> for GID {
    fn extract_bound(id: &Bound<'source, PyAny>) -> PyResult<Self> {
        id.extract::<String>()
            .map(GID::Str)
            .or_else(|_| id.extract::<u64>().map(GID::U64))
            .map_err(|_| {
                let msg = "IDs need to be strings or an unsigned integers";
                PyTypeError::new_err(msg)
            })
    }
}
