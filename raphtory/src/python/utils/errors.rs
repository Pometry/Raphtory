use crate::{errors::GraphError, io::csv_loader::CsvErr};
use pyo3::PyErr;
use raphtory_api::python::error::adapt_err_value;

impl From<GraphError> for PyErr {
    fn from(value: GraphError) -> Self {
        match value {
            // an error that started life in Python (a TypeError raised while converting an
            // argument, say) goes back with its own class and message, instead of being
            // flattened into a generic Exception prefixed with "Python error occurred"
            GraphError::PythonError(err) => err,
            other => adapt_err_value(&other),
        }
    }
}

impl From<CsvErr> for PyErr {
    fn from(value: CsvErr) -> Self {
        adapt_err_value(&value)
    }
}
