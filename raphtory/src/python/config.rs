use pyo3::{Borrowed, BoundObject, FromPyObject, PyAny};
use pythonize::{depythonize, PythonizeError};
use storage::ConfigArgs;

pub struct PyConfig(pub ConfigArgs);

impl<'a, 'py> FromPyObject<'a, 'py> for PyConfig {
    type Error = PythonizeError;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let config_args: ConfigArgs = depythonize(&obj.into_bound())?;
        Ok(PyConfig(config_args))
    }
}
