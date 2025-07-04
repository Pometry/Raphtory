use crate::python::{
    types::repr::Repr,
    utils::{PyGenericIterator, PyNestedGenericIterator},
};
use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use raphtory_api::{core::storage::timeindex::TimeError, iter::BoxedIter};
use std::sync::Arc;

#[pyclass(frozen, module = "raphtory")]
pub struct ResultOptionUtcDateTimeIterable {
    builder: Arc<
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
        let builder = Arc::new(move || {
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
        PyGenericIterator::from_result_iter(self.iter())
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

#[pyclass(frozen, module = "raphtory")]
pub struct NestedResultOptionUtcDateTimeIterable {
    builder: Arc<
        dyn Fn() -> BoxedIter<BoxedIter<Result<Option<DateTime<Utc>>, TimeError>>>
            + Send
            + Sync
            + 'static,
    >,
}

impl Repr for NestedResultOptionUtcDateTimeIterable {
    fn repr(&self) -> String {
        // For nested Result types, we show successful values from outer iterators
        let successful_nested_values: Vec<String> = self
            .iter()
            .take(11) // Same limit as iterator_repr
            .map(|inner_iter| {
                let successful_inner: Vec<String> = inner_iter
                    .filter_map(|result| result.ok())
                    .take(11)
                    .map(|opt_datetime| opt_datetime.repr())
                    .collect();

                let inner_content = if successful_inner.len() < 11 {
                    successful_inner.join(", ")
                } else {
                    successful_inner[0..10].join(", ") + ", ..."
                };
                format!("[{}]", inner_content)
            })
            .collect();

        let content = if successful_nested_values.len() < 11 {
            successful_nested_values.join(", ")
        } else {
            successful_nested_values[0..10].join(", ") + ", ..."
        };

        format!("NestedResultOptionUtcDateTimeIterable([{}])", content)
    }
}

impl NestedResultOptionUtcDateTimeIterable {
    pub fn iter(&self) -> BoxedIter<BoxedIter<Result<Option<DateTime<Utc>>, TimeError>>> {
        (self.builder)()
    }
}

impl<F: Fn() -> It + Send + Sync + 'static, It: Iterator + Send + Sync + 'static> From<F>
    for NestedResultOptionUtcDateTimeIterable
where
    It::Item: Iterator + Send + Sync,
    <It::Item as Iterator>::Item: Into<Result<Option<DateTime<Utc>>, TimeError>> + Send + Sync,
{
    fn from(value: F) -> Self {
        let builder = Arc::new(move || {
            let iter: BoxedIter<BoxedIter<Result<Option<DateTime<Utc>>, TimeError>>> =
                Box::new(value().map(|inner_it| {
                    let inner_iter: BoxedIter<Result<Option<DateTime<Utc>>, TimeError>> =
                        Box::new(inner_it.map(|v| v.into()));
                    inner_iter
                }));
            iter
        });
        Self { builder }
    }
}

#[pymethods]
impl NestedResultOptionUtcDateTimeIterable {
    fn __iter__(&self) -> PyNestedGenericIterator {
        PyNestedGenericIterator::from_nested_result_iter(self.iter())
    }

    fn __len__(&self) -> usize {
        self.iter().count()
    }

    fn __repr__(&self) -> String {
        self.repr()
    }

    fn collect(&self) -> PyResult<Vec<Vec<Option<DateTime<Utc>>>>> {
        self.iter()
            .map(|inner_iter| {
                inner_iter
                    .map(|result| result.map_err(PyErr::from))
                    .collect()
            })
            .collect()
    }
}
