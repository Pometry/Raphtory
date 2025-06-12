use crate::{errors::GraphError, io::csv_loader::CsvErr};
use pyo3::PyErr;
use raphtory_api::python::error::adapt_err_value;

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
