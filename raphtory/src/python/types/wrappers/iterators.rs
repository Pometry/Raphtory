use crate::db::api::view::{BoxedLIter, IntoDynBoxed};
use ouroboros::self_referencing;
use pyo3::{
    pyclass, pymethods, BoundObject, IntoPyObject, Py, PyAny, PyErr, PyRef, PyResult, Python,
};

#[pyclass]
#[self_referencing]
pub struct PyBorrowingIterator {
    inner: Box<dyn PyIter>,
    #[borrows(inner)]
    #[covariant]
    iter: BoxedLIter<'this, PyResult<Py<PyAny>>>,
}

#[pymethods]
impl PyBorrowingIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self) -> Option<PyResult<Py<PyAny>>> {
        self.with_iter_mut(|iter| iter.next())
    }
}

pub trait PyIter: Send + Sync + 'static {
    fn iter(&self) -> BoxedLIter<'_, PyResult<Py<PyAny>>>;

    fn into_py_iter(self) -> PyBorrowingIterator
    where
        Self: Sized,
    {
        PyBorrowingIteratorBuilder {
            inner: Box::new(self),
            iter_builder: |inner| inner.iter(),
        }
        .build()
    }
}

pub trait IntoPyIter<'a> {
    fn into_py_iter(self) -> BoxedLIter<'a, PyResult<Py<PyAny>>>;
}

impl<'a, I: Iterator + Send + Sync + 'a> IntoPyIter<'a> for I
where
    I::Item: for<'py> IntoPyObject<'py>,
{
    fn into_py_iter(self) -> BoxedLIter<'a, PyResult<Py<PyAny>>> {
        self.map(|v| {
            Python::attach(|py| {
                Ok(v.into_pyobject(py)
                    .map_err(|e| e.into())?
                    .into_any()
                    .unbind())
            })
        })
        .into_dyn_boxed()
    }
}

pub trait IntoPyIterResult<'a> {
    fn into_py_iter_result(self) -> BoxedLIter<'a, PyResult<Py<PyAny>>>;
}

impl<'a, T, E, I: Iterator<Item = Result<T, E>> + Send + Sync + 'a> IntoPyIterResult<'a> for I
where
    T: for<'py> IntoPyObject<'py>,
    E: Into<PyErr>,
{
    fn into_py_iter_result(self) -> BoxedLIter<'a, PyResult<Py<PyAny>>> {
        self.map(|item| {
            Python::attach(|py| match item {
                Ok(value) => Ok(value
                    .into_pyobject(py)
                    .map_err(|e| e.into())?
                    .into_any()
                    .unbind()),
                Err(err) => Err(err.into()),
            })
        })
        .into_dyn_boxed()
    }
}

pub trait IntoPyIterTupleResult<'a> {
    fn into_py_iter_tuple_result(self) -> BoxedLIter<'a, PyResult<Py<PyAny>>>;
}

impl<'a, X, T, E, I: Iterator<Item = (X, Result<T, E>)> + Send + Sync + 'a>
    IntoPyIterTupleResult<'a> for I
where
    X: for<'py> IntoPyObject<'py>,
    T: for<'py> IntoPyObject<'py>,
    E: Into<PyErr>,
{
    fn into_py_iter_tuple_result(self) -> BoxedLIter<'a, PyResult<Py<PyAny>>> {
        self.map(|(tuple_left, result)| {
            Python::attach(|py| match result {
                Ok(value) => Ok((tuple_left, value).into_pyobject(py)?.into_any().unbind()),
                Err(err) => Err(err.into()),
            })
        })
        .into_dyn_boxed()
    }
}
