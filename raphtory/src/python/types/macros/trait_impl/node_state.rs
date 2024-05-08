use crate::{
    core::ArcStr,
    db::{
        api::{
            state::{LazyNodeState, NodeState, NodeStateOps},
            view::{DynamicGraph, IntoDynamic, StaticGraphViewOps},
        },
        graph::node::NodeView,
    },
    prelude::Prop,
    python::utils::PyGenericIterator,
};
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use pyo3::prelude::*;

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

            fn __iter__(&self) -> PyGenericIterator {
                self.inner.clone().into_values().into()
            }

            fn __len__(&self) -> usize {
                self.inner.len()
            }

            fn nodes(&self) -> PyGenericIterator {
                self.inner.clone().into_iter().map(|(node, _)| node).into()
            }

            fn items(&self) -> PyGenericIterator {
                self.inner.clone().into_iter().into()
            }
        }

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
            inner: $crate::db::api::state::NodeState<'static, $value, DynamicGraph, DynamicGraph>,
        }

        impl From<NodeState<'static, $value, DynamicGraph, DynamicGraph>> for $name {
            fn from(inner: NodeState<'static, $value, DynamicGraph, DynamicGraph>) -> Self {
                $name { inner }
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
