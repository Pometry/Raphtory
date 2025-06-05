use crate::utils::time::{Interval, ParseTimeError};
use pyo3::{exceptions::PyTypeError, prelude::*, Bound, FromPyObject, PyAny, PyErr, PyResult};
use raphtory_api::python::error::adapt_err_value;

impl From<ParseTimeError> for PyErr {
    fn from(value: ParseTimeError) -> Self {
        adapt_err_value(&value)
    }
}

impl<'source> FromPyObject<'source> for Interval {
    fn extract_bound(interval: &Bound<'source, PyAny>) -> PyResult<Self> {
        if let Ok(string) = interval.extract::<String>() {
            return Ok(string.try_into()?);
        };

        if let Ok(number) = interval.extract::<u64>() {
            return Ok(number.try_into()?);
        };

        Err(PyTypeError::new_err(format!(
            "interval '{interval}' must be a str or an unsigned integer"
        )))
    }
}
