use crate::python::{types::repr::Repr, utils::PyGenericIterator};
use chrono::{DateTime, Utc};
use pyo3::{exceptions::PyException, prelude::*};
use raphtory_api::{core::storage::timeindex::TimeError, iter::BoxedIter};

#[pyclass(frozen, module = "raphtory")]
pub struct ResultOptionUtcDateTimeIterable {
    builder: Box<
        dyn Fn() -> BoxedIter<Result<Option<DateTime<Utc>>, TimeError>> + Send + Sync + 'static,
    >,
}

impl Repr for ResultOptionUtcDateTimeIterable {
    fn repr(&self) -> String {
        // For Result types, we can't easily iterate over failed results to display them.
        // We'll show successful values only.
        let successful_values: Vec<String> = self
            .iter()
            .filter_map(|result| result.ok())
            .take(11) // Same limit as iterator_repr
            .map(|opt_datetime| opt_datetime.repr())
            .collect();

        let content = if successful_values.len() < 11 {
            successful_values.join(", ")
        } else {
            successful_values[0..10].join(", ") + ", ..."
        };

        format!("ResultOptionUtcDateTimeIterable([{}])", content)
    }
}

impl ResultOptionUtcDateTimeIterable {
    pub fn iter(&self) -> BoxedIter<Result<Option<DateTime<Utc>>, TimeError>> {
        (self.builder)()
    }
}

impl<F: Fn() -> It + Send + Sync + 'static, It: Iterator + Send + Sync + 'static> From<F>
    for ResultOptionUtcDateTimeIterable
where
    It::Item: Into<Result<Option<DateTime<Utc>>, TimeError>>,
{
    fn from(value: F) -> Self {
        let builder = Box::new(move || {
            let iter: BoxedIter<Result<Option<DateTime<Utc>>, TimeError>> =
                Box::new(value().map(|v| v.into()));
            iter
        });
        Self { builder }
    }
}

#[pymethods]
impl ResultOptionUtcDateTimeIterable {
    fn __iter__(&self) -> PyGenericIterator {
        // Convert Result<Option<DateTime<Utc>>, TimeError> to PyResult<PyObject>
        let iter = self.iter().map(|result| {
            Python::with_gil(|py| match result {
                Ok(opt_datetime) => Ok(opt_datetime
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

    fn collect(&self) -> PyResult<Vec<Option<DateTime<Utc>>>> {
        self.iter()
            .map(|result| result.map_err(PyErr::from))
            .collect()
    }
}
