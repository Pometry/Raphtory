use crate::core::utils::errors::GraphError;
use crate::core::utils::time::error::ParseTimeError;
use crate::graph_loader::source::csv_loader::CsvErr;
use pyo3::exceptions::PyException;
use pyo3::PyErr;
use std::error::Error;

impl From<ParseTimeError> for PyErr {
    fn from(value: ParseTimeError) -> Self {
        adapt_err_value(&value)
    }
}

impl From<GraphError> for PyErr {
    fn from(value: GraphError) -> Self {
        adapt_err_value(&value)
    }
}

impl From<CsvErr> for PyErr {
    fn from(value: CsvErr) -> Self {
        adapt_err_value(&value)
    }
}

pub fn adapt_err_value<E>(err: &E) -> PyErr
where
    E: Error + ?Sized,
{
    let error_log = display_error_chain::DisplayErrorChain::new(err).to_string();
    PyException::new_err(error_log)
}
