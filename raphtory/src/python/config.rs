use pyo3::{
    Borrowed, BoundObject, FromPyObject, PyAny,
};
use pythonize::{depythonize, PythonizeError};
use storage::{Config, ConfigArgs};



pub struct PyConfig(pub Config); 

impl<'a, 'py> FromPyObject<'a, 'py> for PyConfig {
    type Error = PythonizeError;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let mut config: Config = depythonize(&obj.into_bound())?;
        let config_args: ConfigArgs = depythonize(&obj.into_bound())?;
        config.config_args = Some(config_args);
        Ok(PyConfig(config))
    }
}
