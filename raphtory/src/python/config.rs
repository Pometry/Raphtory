use pyo3::{Borrowed, BoundObject, FromPyObject, PyAny};
use pythonize::{depythonize, PythonizeError};
use storage::Args;

pub struct PyArgs(pub Args);

impl<'a, 'py> FromPyObject<'a, 'py> for PyArgs {
    type Error = PythonizeError;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let args: Args = depythonize(&obj.into_bound())?;
        Ok(PyArgs(args))
    }
}
