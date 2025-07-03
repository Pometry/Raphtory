use crate::python::{
    types::repr::{iterator_repr, Repr},
    utils::PyGenericIterator,
};
use chrono::{DateTime, Utc};
use pyo3::{exceptions::PyException, prelude::*};
use raphtory_api::{core::storage::timeindex::TimeError, iter::BoxedIter};

#[pyclass(frozen, module = "raphtory")]
pub struct ResultVecUtcDateTimeIterable {
    builder:
        Box<dyn Fn() -> BoxedIter<Result<Vec<DateTime<Utc>>, TimeError>> + Send + Sync + 'static>,
}

impl Repr for ResultVecUtcDateTimeIterable {
    fn repr(&self) -> String {
        // For Result types, we can't easily iterate over failed results to display them.
        // We'll show successful values only.
        let successful_values: Vec<String> = self
            .iter()
            .filter_map(|result| result.ok())
            .take(11) // Same limit as iterator_repr
            .map(|vec_datetime| iterator_repr(vec_datetime.iter()))
            .collect();

        let content = if successful_values.len() < 11 {
            successful_values.join(", ")
        } else {
            successful_values[0..10].join(", ") + ", ..."
        };

        format!("ResultVecUtcDateTimeIterable([{}])", content)
    }
}

impl ResultVecUtcDateTimeIterable {
    pub fn iter(&self) -> BoxedIter<Result<Vec<DateTime<Utc>>, TimeError>> {
        (self.builder)()
    }
}

impl<F: Fn() -> It + Send + Sync + 'static, It: Iterator + Send + Sync + 'static> From<F>
    for ResultVecUtcDateTimeIterable
where
    It::Item: Into<Result<Vec<DateTime<Utc>>, TimeError>>,
{
    fn from(value: F) -> Self {
        let builder = Box::new(move || {
            let iter: BoxedIter<Result<Vec<DateTime<Utc>>, TimeError>> =
                Box::new(value().map(|v| v.into()));
            iter
        });
        Self { builder }
    }
}

#[pymethods]
impl ResultVecUtcDateTimeIterable {
    fn __iter__(&self) -> PyGenericIterator {
        // Convert Result<Option<DateTime<Utc>>, TimeError> to PyResult<PyObject>
        let iter = self.iter().map(|result| {
            Python::with_gil(|py| match result {
                Ok(vec_datetime) => Ok(vec_datetime
                    .into_pyobject(py)
                    .map_err(|e| e.into())?
                    .into_any()
                    .unbind()),
                Err(time_error) => Err(PyErr::from(time_error)),
            })
        });
        PyGenericIterator::new(Box::new(iter))
    }

    fn __len__(&self) -> usize {
        self.iter().count()
    }

    fn __repr__(&self) -> String {
        self.repr()
    }

    fn collect(&self) -> PyResult<Vec<Vec<DateTime<Utc>>>> {
        self.iter()
            .map(|result| result.map_err(PyErr::from))
            .collect()
    }
}
