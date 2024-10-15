use crate::core::Direction;
use pyo3::{exceptions::PyTypeError, FromPyObject, PyAny, PyResult};

impl<'source> FromPyObject<'source> for Direction {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
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
