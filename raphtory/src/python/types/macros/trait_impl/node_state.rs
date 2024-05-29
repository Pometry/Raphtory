use crate::{
    core::ArcStr,
    db::{
        api::{
            state::{LazyNodeState, NodeState, NodeStateOps},
            view::{BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic, StaticGraphViewOps},
        },
        graph::node::NodeView,
    },
    prelude::Prop,
    python::types::iterable::{PyBorrowingIterator, PyIter},
};
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use pyo3::prelude::*;
use std::sync::Arc;

trait IntoPyIter<'a> {
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

macro_rules! py_iter {
    ($inner:expr, $inner_t:ty, $closure:expr) => {{
        struct Iterator($inner_t);

        impl PyIter for Iterator {
            fn iter(&self) -> BoxedLIter<PyObject> {
                // forces the type inference to return the correct lifetimes,
                // calling the closure directly does not work
                fn apply<'a, O: IntoPyIter<'a>>(
                    arg: &'a $inner_t,
                    f: impl FnOnce(&'a $inner_t) -> O,
                ) -> BoxedLIter<'a, PyObject> {
                    f(arg).into_py_iter()
                }
                apply(&self.0, $closure)
            }
        }

        Iterator($inner).into_py_iter()
    }};
}

macro_rules! py_iter2 {
    ($inner:expr, |$inner_x:ident: $inner_t:ty| $body:expr) => {{
        struct Iterator($inner_t);

        impl PyIter for Iterator {
            fn iter(&self) -> BoxedLIter<PyObject> {
                // forces the type inference to return the correct lifetimes,
                // calling the closure directly does not work
                fn __cast_signature__<'a>($inner_x: &'a $inner_t) -> BoxedLIter<'a, PyObject> {
                    $body.into_py_iter()
                }
                __cast_signature__(&self.0)
            }
        }

        Iterator($inner).into_py_iter()
    }};
}

macro_rules! impl_node_state_ops {
    ($name:ident) => {
        #[pymethods]
        impl $name {
            fn __len__(&self) -> usize {
                self.inner.len()
            }

            fn nodes(&self) -> PyBorrowingIterator {
                py_iter!(self.inner.clone(), |inner| {
                    inner.nodes().map(move |n| n.cloned())
                })
            }

            fn items(&self) -> PyBorrowingIterator {
                py_iterator(self.inner.clone(), |inner| {
                    inner.iter().map(move |(n, v)| (n.cloned(), v.clone()))
                })
            }

            fn values(&self) -> PyBorrowingIterator {
                self.__iter__()
            }
        }
    };
}

macro_rules! impl_lazy_node_state {
    ($name:ident<$value:ty>) => {
        #[pyclass]
        pub struct $name {
            inner:
                $crate::db::api::state::LazyNodeState<'static, $value, DynamicGraph, DynamicGraph>,
        }

        #[pymethods]
        impl $name {
            fn compute(&self) -> NodeState<'static, $value, DynamicGraph, DynamicGraph> {
                self.inner.compute()
            }
        }

        // impl_node_state_ops!($name);

        impl From<LazyNodeState<'static, $value, DynamicGraph, DynamicGraph>> for $name {
            fn from(inner: LazyNodeState<'static, $value, DynamicGraph, DynamicGraph>) -> Self {
                $name { inner }
            }
        }

        impl pyo3::IntoPy<PyObject> for LazyNodeState<'static, $value, DynamicGraph, DynamicGraph> {
            fn into_py(self, py: Python<'_>) -> PyObject {
                $name::from(self).into_py(py)
            }
        }
    };
}

macro_rules! impl_node_state {
    ($name:ident<$value:ty>) => {
        #[pyclass]
        pub struct $name {
            inner:
                Arc<$crate::db::api::state::NodeState<'static, $value, DynamicGraph, DynamicGraph>>,
        }

        // impl_node_state_ops!($name);

        impl From<NodeState<'static, $value, DynamicGraph, DynamicGraph>> for $name {
            fn from(inner: NodeState<'static, $value, DynamicGraph, DynamicGraph>) -> Self {
                $name {
                    inner: inner.into(),
                }
            }
        }

        impl pyo3::IntoPy<PyObject> for NodeState<'static, $value, DynamicGraph, DynamicGraph> {
            fn into_py(self, py: Python<'_>) -> PyObject {
                $name::from(self).into_py(py)
            }
        }
    };
}

impl_lazy_node_state!(LazyNodeStateUsize<usize>);
impl_node_state!(NodeStateUsize<usize>);

impl_lazy_node_state!(LazyNodeStateU64<u64>);
impl_node_state!(NodeStateU64<u64>);

impl_lazy_node_state!(LazyNodeStateOptionI64<Option<i64>>);
impl_node_state!(NodeStateOptionI64<Option<i64>>);

impl_lazy_node_state!(LazyNodeStateString<String>);
impl_node_state!(NodeStateString<String>);

impl_lazy_node_state!(LazyNodeStateOptionDateTime<Option<DateTime<Utc>>>);
impl_node_state!(NodeStateOptionDateTime<Option<DateTime<Utc>>>);

impl_lazy_node_state!(LazyNodeStateListI64<Vec<i64>>);
impl_node_state!(NodeStateListI64<Vec<i64>>);

impl_lazy_node_state!(LazyNodeStateOptionListDateTime<Option<Vec<DateTime<Utc>>>>);
impl_node_state!(NodeStateOptionListDateTime<Option<Vec<DateTime<Utc>>>>);

impl_lazy_node_state!(LazyNodeStateOptionStr<Option<ArcStr>>);
impl_node_state!(NodeStateOptionStr<Option<ArcStr>>);

impl_lazy_node_state!(LazyNodeStateListDateTime<Vec<DateTime<Utc>>>);
impl_node_state!(NodeStateListDateTime<Vec<DateTime<Utc>>>);

#[pymethods]
impl NodeStateUsize {
    fn __iter__(&self) -> PyBorrowingIterator {
        {
            struct Iterator(Arc<NodeState<'static, usize, DynamicGraph>>);

            impl PyIter for Iterator {
                fn iter(&self) -> BoxedLIter<PyObject> {
                    self.0
                        .values()
                        .map(|&v| Python::with_gil(|py| v.into_py(py)))
                        .into_dyn_boxed()
                }
            }

            Iterator(self.inner.clone()).into_py_iter()
        }
    }

    fn iter2(&self) -> PyBorrowingIterator {
        py_iter!(
            self.inner.clone(),
            Arc<NodeState<'static, usize, DynamicGraph>>,
            |inner| inner.values().copied()
        )
    }
}
