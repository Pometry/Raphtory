use crate::core::entities::GID;
use pyo3::{exceptions::PyTypeError, prelude::*};
use std::convert::Infallible;

impl<'py> IntoPyObject<'py> for GID {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            GID::U64(v) => Ok(v.into_pyobject(py)?.into_any()),
            GID::Str(v) => Ok(v.into_pyobject(py)?.into_any()),
        }
    }
}

impl<'py> IntoPyObject<'py> for &GID {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            GID::U64(v) => Ok(v.into_pyobject(py)?.into_any()),
            GID::Str(v) => Ok(v.into_pyobject(py)?.into_any()),
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for GID {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        ob.extract::<String>()
            .map(GID::Str)
            .or_else(|_| ob.extract::<u64>().map(GID::U64))
            .map_err(|_| {
                let msg = "IDs need to be strings or an unsigned integers";
                PyTypeError::new_err(msg)
            })
    }
}
