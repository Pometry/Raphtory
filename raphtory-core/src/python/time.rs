use crate::utils::time::{AlignmentUnit, Interval};
use pyo3::{exceptions::PyTypeError, prelude::*, Bound, FromPyObject, PyAny, PyResult};

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

impl<'source> FromPyObject<'source> for AlignmentUnit {
    fn extract_bound(unit: &Bound<'source, PyAny>) -> PyResult<Self> {
        if let Ok(string) = unit.extract::<String>() {
            return Ok(string.try_into()?);
        };

        Err(PyTypeError::new_err(format!("unit '{unit}' must be a str")))
    }
}
