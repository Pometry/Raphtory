use ouroboros::self_referencing;
use pyo3::{pyclass, pymethods, IntoPy, PyObject, PyRef, Python};
use raphtory_api::{BoxedLIter, IntoDynBoxed};

#[pyclass]
#[self_referencing]
pub struct PyBorrowingIterator {
    inner: Box<dyn PyIter>,
    #[borrows(inner)]
    #[covariant]
    iter: raphtory_api::BoxedLIter<'this, PyObject>,
}

#[pymethods]
impl PyBorrowingIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self) -> Option<PyObject> {
        self.with_iter_mut(|iter| iter.next())
    }
}

pub trait PyIter: Send + 'static {
    fn iter(&self) -> BoxedLIter<PyObject>;

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
    fn into_py_iter(self) -> BoxedLIter<'a, PyObject>;
}

impl<'a, I: Iterator + Send + 'a> IntoPyIter<'a> for I
where
    I::Item: IntoPy<PyObject>,
{
    fn into_py_iter(self) -> BoxedLIter<'a, PyObject> {
        self.map(|v| Python::with_gil(|py| v.into_py(py)))
            .into_dyn_boxed()
    }
}
