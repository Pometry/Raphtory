use pyo3::{Borrowed, BoundObject, FromPyObject, PyAny};
use pythonize::{depythonize, PythonizeError};
use storage::Config;

pub struct PyConfig(pub Config);

impl<'a, 'py> FromPyObject<'a, 'py> for PyConfig {
    type Error = PythonizeError;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let config: Config = depythonize(&obj.into_bound())?;
        Ok(PyConfig(config))
    }
}
