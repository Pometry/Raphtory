use crate::db::api::view::{BoxedLIter, IntoDynBoxed};
use ouroboros::self_referencing;
use pyo3::{pyclass, pymethods, BoundObject, IntoPyObject, PyObject, PyRef, PyResult, Python};

#[pyclass]
#[self_referencing]
pub struct PyBorrowingIterator {
    inner: Box<dyn PyIter>,
    #[borrows(inner)]
    #[covariant]
    iter: BoxedLIter<'this, PyResult<PyObject>>,
}

#[pymethods]
impl PyBorrowingIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self) -> Option<PyResult<PyObject>> {
        self.with_iter_mut(|iter| iter.next())
    }
}

pub trait PyIter: Send + Sync + 'static {
    fn iter(&self) -> BoxedLIter<PyResult<PyObject>>;

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
    fn into_py_iter(self) -> BoxedLIter<'a, PyResult<PyObject>>;
}

impl<'a, I: Iterator + Send + Sync + 'a> IntoPyIter<'a> for I
where
    I::Item: for<'py> IntoPyObject<'py>,
{
    fn into_py_iter(self) -> BoxedLIter<'a, PyResult<PyObject>> {
        self.map(|v| {
            Python::with_gil(|py| {
                Ok(v.into_pyobject(py)
                    .map_err(|e| e.into())?
                    .into_any()
                    .unbind())
            })
        })
        .into_dyn_boxed()
    }
}
