use crate::db::api::view::BoxedIter;
use crate::python::types::repr::{iterator_repr, Repr};
use pyo3::{IntoPy, PyObject};
use std::marker::PhantomData;
use std::sync::Arc;

pub struct Iterable<I: Send, PyI: IntoPy<PyObject> + From<I> + Repr> {
    pub name: String,
    pub builder: Arc<dyn Fn() -> BoxedIter<I> + Send + Sync + 'static>,
    pytype: PhantomData<PyI>,
}

impl<I: Send + 'static, PyI: IntoPy<PyObject> + From<I> + Repr> Iterable<I, PyI> {
    pub fn iter(&self) -> BoxedIter<I> {
        (self.builder)()
    }
    pub fn py_iter(&self) -> BoxedIter<PyI> {
        Box::new(self.iter().map(|i| i.into()))
    }
    pub fn new<F: Fn() -> BoxedIter<I> + Send + Sync + 'static>(name: String, builder: F) -> Self {
        Self {
            name,
            builder: Arc::new(builder),
            pytype: Default::default(),
        }
    }
}

impl<I: Send + 'static, PyI: IntoPy<PyObject> + From<I> + Repr> Repr for Iterable<I, PyI> {
    fn repr(&self) -> String {
        format!("{}([{}])", self.name, iterator_repr(self.py_iter()))
    }
}

pub struct NestedIterable<I: Send, PyI: IntoPy<PyObject> + From<I> + Repr> {
    pub name: String,
    pub builder: Arc<dyn Fn() -> BoxedIter<BoxedIter<I>> + Send + Sync + 'static>,
    pytype: PhantomData<PyI>,
}

impl<I: Send, PyI: IntoPy<PyObject> + From<I> + Repr> NestedIterable<I, PyI> {
    pub fn iter(&self) -> BoxedIter<BoxedIter<I>> {
        (self.builder)()
    }
    pub fn new<F: Fn() -> BoxedIter<BoxedIter<I>> + Send + Sync + 'static>(
        name: String,
        builder: F,
    ) -> Self {
        Self {
            name,
            builder: Arc::new(builder),
            pytype: Default::default(),
        }
    }
}

impl<I: Send, PyI: IntoPy<PyObject> + From<I> + Repr> Repr for NestedIterable<I, PyI> {
    fn repr(&self) -> String {
        format!(
            "{}([{}])",
            self.name,
            iterator_repr(
                self.iter()
                    .map(|it| format!("[{}]", iterator_repr(it.map(|i| PyI::from(i)))))
            )
        )
    }
}
