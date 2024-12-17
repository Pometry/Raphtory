use crate::core::storage::arc_str::ArcStr;
use pyo3::{prelude::*, types::PyString};
use std::convert::Infallible;

impl<'py> IntoPyObject<'py> for ArcStr {
    type Target = PyString;
    type Output = Bound<'py, Self::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.into_pyobject(py)
    }
}

impl<'py, 'a> IntoPyObject<'py> for &'a ArcStr {
    type Target = PyString;
    type Output = Bound<'py, Self::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.into_pyobject(py)
    }
}

impl<'source> FromPyObject<'source> for ArcStr {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        ob.extract::<String>().map(|v| v.into())
    }
}
