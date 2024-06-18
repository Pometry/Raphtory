#![allow(non_local_definitions)]
use crate::{
    core::{entities::nodes::node_ref::NodeRef, ArcStr},
    db::{
        api::{
            state::{LazyNodeState, NodeState, NodeStateOps, OrderedNodeStateOps},
            view::{DynamicGraph, GraphViewOps},
        },
        graph::node::NodeView,
    },
    py_borrowing_iter,
    python::types::{repr::Repr, wrappers::iterators::PyBorrowingIterator},
};
use chrono::{DateTime, Utc};
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
    types::PyNotImplemented,
};
use std::sync::Arc;

macro_rules! impl_node_state_ops {
    ($name:ident<$value:ty>, $inner_t:ty, $to_owned:expr) => {
        impl $name {
            pub fn iter(&self) -> impl Iterator<Item = $value> + '_ {
                self.inner.values().map($to_owned)
            }
        }

        #[pymethods]
        impl $name {
            fn __len__(&self) -> usize {
                self.inner.len()
            }

            fn nodes(&self) -> PyBorrowingIterator {
                py_borrowing_iter!(self.inner.clone(), $inner_t, |inner| {
                    inner.nodes().map(|n| n.cloned())
                })
            }

            fn __iter__(&self) -> PyBorrowingIterator {
                py_borrowing_iter!(self.inner.clone(), $inner_t, |inner| inner
                    .values()
                    .map($to_owned))
            }

            fn __getitem__(&self, node: NodeRef) -> PyResult<$value> {
                self.inner
                    .get_by_node(node)
                    .map($to_owned)
                    .ok_or_else(|| match node {
                        NodeRef::External(id) => {
                            PyKeyError::new_err(format!("Missing value for node with id {id}"))
                        }
                        NodeRef::Internal(vid) => {
                            let node = self.inner.graph().node(vid);
                            match node {
                                Some(node) => {
                                    PyKeyError::new_err(format!("Missing value {}", node.repr()))
                                }
                                None => PyTypeError::new_err("Invalid node reference"),
                            }
                        }
                        NodeRef::ExternalStr(name) => {
                            PyKeyError::new_err(format!("Missing value for node with name {name}"))
                        }
                    })
            }

            fn items(&self) -> PyBorrowingIterator {
                py_borrowing_iter!(self.inner.clone(), $inner_t, |inner| inner
                    .iter()
                    .map(|(n, v)| (n.cloned(), ($to_owned)(v))))
            }

            fn values(&self) -> PyBorrowingIterator {
                self.__iter__()
            }

            fn __repr__(&self) -> String {
                self.inner.repr()
            }
        }
    };
}

macro_rules! impl_node_state_ord_ops {
    ($name:ident<$value:ty>, $to_owned:expr) => {
        #[pymethods]
        impl $name {
            #[pyo3(signature = (reverse = false))]
            fn sorted(&self, reverse: bool) -> NodeState<'static, $value, DynamicGraph> {
                self.inner.sort_by_values(reverse)
            }

            fn top_k(&self, k: usize) -> NodeState<'static, $value, DynamicGraph> {
                self.inner.top_k(k)
            }

            fn bottom_k(&self, k: usize) -> NodeState<'static, $value, DynamicGraph> {
                self.inner.bottom_k(k)
            }

            fn min_item(&self) -> Option<(NodeView<DynamicGraph>, $value)> {
                self.inner
                    .min_item()
                    .map(|(n, v)| (n.cloned(), ($to_owned)(v)))
            }

            fn min(&self) -> Option<$value> {
                self.inner.min().map($to_owned)
            }

            fn max_item(&self) -> Option<(NodeView<DynamicGraph>, $value)> {
                self.inner
                    .max_item()
                    .map(|(n, v)| (n.cloned(), ($to_owned)(v)))
            }

            fn max(&self) -> Option<$value> {
                self.inner.max().map($to_owned)
            }

            fn median(&self) -> Option<$value> {
                self.inner.median().map($to_owned)
            }

            fn median_item(&self) -> Option<(NodeView<DynamicGraph>, $value)> {
                self.inner
                    .median_item()
                    .map(|(n, v)| (n.cloned(), ($to_owned)(v)))
            }

            fn __eq__<'py>(&'py self, other: &'py PyAny, py: Python<'py>) -> PyObject {
                if let Ok(other) = other.extract::<PyRef<Self>>() {
                    return self.inner.values().eq(other.inner.values()).into_py(py);
                } else if let Ok(other) = other.extract::<Vec<$value>>() {
                    return self
                        .inner
                        .values()
                        .map($to_owned)
                        .eq(other.iter().cloned())
                        .into_py(py);
                }
                PyNotImplemented::get(py).into_py(py)
            }
        }
    };
}

macro_rules! impl_node_state_num_ops {
    ($name:ident<$value:ty>) => {
        #[pymethods]
        impl $name {
            fn sum(&self) -> $value {
                self.inner.sum()
            }

            fn mean(&self) -> f64 {
                self.inner.mean()
            }
        }
    };
}

macro_rules! impl_lazy_node_state {
    ($name:ident<$value:ty>) => {
        #[pyclass]
        pub struct $name {
            inner: LazyNodeState<'static, $value, DynamicGraph, DynamicGraph>,
        }

        #[pymethods]
        impl $name {
            fn compute(&self) -> NodeState<'static, $value, DynamicGraph, DynamicGraph> {
                self.inner.compute()
            }

            fn collect(&self) -> Vec<$value> {
                self.inner.collect()
            }
        }

        impl_node_state_ops!(
            $name<$value>,
            LazyNodeState<'static, $value, DynamicGraph, DynamicGraph>,
            |v: $value| v
        );

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
            inner: Arc<NodeState<'static, $value, DynamicGraph, DynamicGraph>>,
        }

        impl_node_state_ops!(
            $name<$value>,
            Arc<NodeState<'static, $value, DynamicGraph, DynamicGraph>>,
            |v: &$value| v.clone()
        );

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

macro_rules! impl_lazy_node_state_ord {
    ($name:ident<$value:ty>) => {
        impl_lazy_node_state!($name<$value>);
        impl_node_state_ord_ops!($name<$value>, |v: $value| v);
    };
}

macro_rules! impl_node_state_ord {
    ($name:ident<$value:ty>) => {
        impl_node_state!($name<$value>);
        impl_node_state_ord_ops!($name<$value>, |v: &$value| v.clone());
    };
}

macro_rules! impl_lazy_node_state_num {
    ($name:ident<$value:ty>) => {
        impl_lazy_node_state_ord!($name<$value>);
        impl_node_state_num_ops!($name<$value>);
    };
}

macro_rules! impl_node_state_num {
    ($name:ident<$value:ty>) => {
        impl_node_state_ord!($name<$value>);
        impl_node_state_num_ops!($name<$value>);
    };
}

impl_lazy_node_state_num!(LazyNodeStateUsize<usize>);
impl_node_state_num!(NodeStateUsize<usize>);

impl_lazy_node_state_num!(LazyNodeStateU64<u64>);
impl_node_state_num!(NodeStateU64<u64>);

impl_lazy_node_state_ord!(LazyNodeStateOptionI64<Option<i64>>);
impl_node_state_ord!(NodeStateOptionI64<Option<i64>>);

impl_lazy_node_state_ord!(LazyNodeStateString<String>);
impl_node_state_ord!(NodeStateString<String>);

impl_lazy_node_state_ord!(LazyNodeStateOptionDateTime<Option<DateTime<Utc>>>);
impl_node_state_ord!(NodeStateOptionDateTime<Option<DateTime<Utc>>>);

impl_lazy_node_state_ord!(LazyNodeStateListI64<Vec<i64>>);
impl_node_state_ord!(NodeStateListI64<Vec<i64>>);

impl_lazy_node_state_ord!(LazyNodeStateOptionListDateTime<Option<Vec<DateTime<Utc>>>>);
impl_node_state_ord!(NodeStateOptionListDateTime<Option<Vec<DateTime<Utc>>>>);

impl_lazy_node_state_ord!(LazyNodeStateOptionStr<Option<ArcStr>>);
impl_node_state_ord!(NodeStateOptionStr<Option<ArcStr>>);

impl_lazy_node_state_ord!(LazyNodeStateListDateTime<Vec<DateTime<Utc>>>);
impl_node_state_ord!(NodeStateListDateTime<Vec<DateTime<Utc>>>);
