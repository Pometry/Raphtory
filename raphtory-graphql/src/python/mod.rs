use crate::url_encode::UrlDecodeError;
use pyo3::PyErr;
use raphtory::python::utils::errors::adapt_err_value;

pub mod graphql;
pub mod pymodule;

impl From<UrlDecodeError> for PyErr {
    fn from(value: UrlDecodeError) -> Self {
        adapt_err_value(&value)
    }
}
