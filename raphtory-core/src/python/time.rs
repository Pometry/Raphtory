use crate::utils::time::{AlignmentUnit, Interval, ParseTimeError};
use pyo3::{exceptions::PyTypeError, prelude::*, Bound, FromPyObject, PyAny, PyErr, PyResult};
use raphtory_api::python::error::adapt_err_value;

impl From<ParseTimeError> for PyErr {
    fn from(value: ParseTimeError) -> Self {
        adapt_err_value(&value)
    }
}

impl<'py> FromPyObject<'_, 'py> for Interval {
    type Error = PyErr;
    fn extract(interval: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(string) = interval.extract::<String>() {
            return Ok(string.try_into()?);
        };

        if let Ok(number) = interval.extract::<u64>() {
            return Ok(number.try_into()?);
        };

        Err(PyTypeError::new_err(format!(
            "interval '{interval:?}' must be a str or an unsigned integer"
        )))
    }
}

impl<'py> FromPyObject<'_, 'py> for AlignmentUnit {
    type Error = PyErr;
    fn extract(unit: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(string) = unit.extract::<String>() {
            return Ok(string.try_into()?);
        };

        Err(PyTypeError::new_err(format!(
            "unit '{unit:?}' must be a str"
        )))
    }
}
