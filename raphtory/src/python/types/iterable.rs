use crate::{
    db::api::view::BoxedIter,
    python::types::repr::{iterator_repr, Repr},
};
use pyo3::{IntoPy, PyObject};
use std::{marker::PhantomData, sync::Arc};

pub struct Iterable<I: Send, PyI: IntoPy<PyObject> + From<I> + Repr> {
    pub name: &'static str,
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
    pub fn new<F: Fn() -> It + Send + Sync + 'static, It: Iterator + Send + 'static>(
        name: &'static str,
        builder: F,
    ) -> Self
    where
        It::Item: Into<I>,
    {
        let builder = Arc::new(move || {
            let iter: BoxedIter<I> = Box::new(builder().map(|v| v.into()));
            iter
        });
        Self {
            name,
            builder,
            pytype: Default::default(),
        }
    }
    pub fn iter_eq<J: IntoIterator<Item = I>>(&self, other: J) -> bool
    where
        I: PartialEq,
    {
        self.iter().eq(other)
    }
}

impl<I: Send + 'static + PartialEq, PyI: IntoPy<PyObject> + From<I> + Repr, J> PartialEq<J>
    for Iterable<I, PyI>
where
    for<'a> &'a J: IntoIterator<Item = I>,
{
    fn eq(&self, other: &J) -> bool {
        self.iter_eq(other)
    }
}

impl<I: Send + 'static, PyI: IntoPy<PyObject> + From<I> + Repr> Repr for Iterable<I, PyI> {
    fn repr(&self) -> String {
        format!("{}([{}])", self.name, iterator_repr(self.py_iter()))
    }
}

pub struct NestedIterable<I: Send, PyI: IntoPy<PyObject> + From<I> + Repr> {
    pub name: &'static str,
    pub builder: Arc<dyn Fn() -> BoxedIter<BoxedIter<I>> + Send + Sync + 'static>,
    pytype: PhantomData<PyI>,
}

impl<I: Send, PyI: IntoPy<PyObject> + From<I> + Repr> NestedIterable<I, PyI> {
    pub fn iter(&self) -> BoxedIter<BoxedIter<I>> {
        (self.builder)()
    }
    pub fn new<F: Fn() -> It + Send + Sync + 'static, It: Iterator + Send + 'static>(
        name: &'static str,
        builder: F,
    ) -> Self
    where
        It::Item: Iterator + Send,
        <It::Item as Iterator>::Item: Into<I> + Send,
    {
        let builder = Arc::new(move || {
            let iter: BoxedIter<BoxedIter<I>> = Box::new(builder().map(|it| {
                let iter: BoxedIter<I> = Box::new(it.map(|v| v.into()));
                iter
            }));
            iter
        });
        Self {
            name,
            builder,
            pytype: Default::default(),
        }
    }

    pub fn iter_eq<JJ: IntoIterator<Item = J>, J: IntoIterator<Item = I>>(&self, other: JJ) -> bool
    where
        I: PartialEq,
    {
        self.iter()
            .zip(other)
            .all(|(t, o)| t.zip(o).all(|(tt, oo)| tt == oo))
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
