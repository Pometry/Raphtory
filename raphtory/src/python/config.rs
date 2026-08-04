use pyo3::{Borrowed, BoundObject, FromPyObject, PyAny};
use pythonize::{depythonize, PythonizeError};
use storage::{persist::args::ArgsOps, Args};

pub struct PyArgs(pub Args);

impl<'a, 'py> FromPyObject<'a, 'py> for PyArgs {
    type Error = PythonizeError;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let args: Args = depythonize(&obj.into_bound())?;
        let mut args_from_env = Args::from_env();

        // Read values from env first, then apply args on top.
        args_from_env.update(args);
        Ok(PyArgs(args_from_env))
    }
}
