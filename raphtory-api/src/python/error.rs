use crate::core::entities::properties::prop::{PropError, PropTypeError};
use pyo3::{
    exceptions::{PyException, PyTypeError},
    PyErr,
};
use std::error::Error;

pub fn adapt_err_value<E>(err: &E) -> PyErr
where
    E: Error + ?Sized,
{
    let error_log = display_error_chain::DisplayErrorChain::new(err).to_string();
    PyException::new_err(error_log)
}

impl From<PropError> for PyErr {
    fn from(value: PropError) -> Self {
        PyTypeError::new_err(value.to_string())
    }
}

impl From<PropTypeError> for PyErr {
    fn from(value: PropTypeError) -> Self {
        PyTypeError::new_err(value.to_string())
    }
}
