use crate::core::Direction;
use pyo3::{exceptions::PyTypeError, prelude::*};

impl<'py> FromPyObject<'_, 'py> for Direction {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let value: &str = ob.extract()?;
        match value {
            "out" => Ok(Direction::OUT),
            "in" => Ok(Direction::IN),
            "both" => Ok(Direction::BOTH),
            _ => Err(PyTypeError::new_err(PyTypeError::new_err(
                "Direction must be one of { 'out', 'in', 'both' }",
            ))),
        }
    }
}
