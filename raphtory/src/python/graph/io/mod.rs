use pyo3::{create_exception, exceptions::PyException};

pub mod pandas_loaders;

create_exception!(exceptions, ArrowErrorException, PyException);
create_exception!(exceptions, GraphLoadException, PyException);
