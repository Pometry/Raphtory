use polars_arrow::array::Array;
use pyo3::{create_exception, exceptions::PyException};

pub mod pandas_loaders;

pub type ArrayRef = Box<dyn Array>;

create_exception!(exceptions, ArrowErrorException, PyException);
create_exception!(exceptions, GraphLoadException, PyException);
