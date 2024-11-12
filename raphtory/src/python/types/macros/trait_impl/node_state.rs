use crate::{
    core::entities::nodes::node_ref::{AsNodeRef, NodeRef},
    db::{
        api::{
            state::{ops, LazyNodeState, NodeOp, NodeState, NodeStateOps, OrderedNodeStateOps},
            view::{
                internal::Static, DynamicGraph, GraphViewOps, IntoDynHop, IntoDynamic,
                StaticGraphViewOps,
            },
        },
        graph::node::NodeView,
    },
    prelude::*,
    py_borrowing_iter,
    python::{
        types::{repr::Repr, wrappers::iterators::PyBorrowingIterator},
        utils::PyNodeRef,
    },
};
use chrono::{DateTime, Utc};
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
    types::PyNotImplemented,
};
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
use std::{collections::HashMap, sync::Arc};

macro_rules! impl_node_state_ops {
    ($name:ident, $value:ty, $inner_t:ty, $to_owned:expr) => {
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

            fn __getitem__(&self, node: PyNodeRef) -> PyResult<$value> {
                let node = node.as_node_ref();
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

            fn sorted_by_id(&self) -> NodeState<'static, $value, DynamicGraph> {
                self.inner.sort_by_id()
            }

            fn __repr__(&self) -> String {
                self.inner.repr()
            }
        }
    };
}

macro_rules! impl_node_state_ord_ops {
    ($name:ident, $value:ty, $to_owned:expr) => {
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

            fn __eq__<'py>(&self, other: &Bound<'py, PyAny>, py: Python<'py>) -> PyObject {
                if let Ok(other) = other.downcast::<Self>() {
                    let other = Bound::borrow(other);
                    return self.inner.values().eq(other.inner.values()).into_py(py);
                } else if let Ok(other) = other.extract::<Vec<$value>>() {
                    return self
                        .inner
                        .values()
                        .map($to_owned)
                        .eq(other.into_iter())
                        .into_py(py);
                } else if let Ok(other) = other.extract::<HashMap<PyNodeRef, $value>>() {
                    return (self.inner.len() == other.len()
                        && other.into_iter().all(|(node, value)| {
                            self.inner.get_by_node(node).map($to_owned) == Some(value)
                        }))
                    .into_py(py);
                }
                PyNotImplemented::get_bound(py).into_py(py)
            }
        }
    };
}

macro_rules! impl_node_state_num_ops {
    ($name:ident, $value:ty) => {
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
    ($name:ident<$op:ty>) => {
        #[pyclass]
        pub struct $name {
            inner: LazyNodeState<'static, $op, DynamicGraph, DynamicGraph>,
        }

        #[pymethods]
        impl $name {
            fn compute(
                &self,
            ) -> NodeState<'static, <$op as NodeOp>::Output, DynamicGraph, DynamicGraph> {
                self.inner.compute()
            }

            fn collect(&self) -> Vec<<$op as NodeOp>::Output> {
                self.inner.collect()
            }
        }

        impl_node_state_ops!(
            $name,
            <$op as NodeOp>::Output,
            LazyNodeState<'static, $op, DynamicGraph, DynamicGraph>,
            |v: <$op as NodeOp>::Output| v
        );

        impl From<LazyNodeState<'static, $op, DynamicGraph, DynamicGraph>> for $name {
            fn from(inner: LazyNodeState<'static, $op, DynamicGraph, DynamicGraph>) -> Self {
                $name { inner }
            }
        }

        impl pyo3::IntoPy<PyObject> for LazyNodeState<'static, $op, DynamicGraph, DynamicGraph> {
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
            $name,
            $value,
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
        impl_node_state_ord_ops!(
            $name,
            <$value as NodeOp>::Output,
            |v: <$value as NodeOp>::Output| v
        );
    };
}

macro_rules! impl_node_state_ord {
    ($name:ident<$value:ty>) => {
        impl_node_state!($name<$value>);
        impl_node_state_ord_ops!($name, $value, |v: &$value| v.clone());
    };
}

macro_rules! impl_lazy_node_state_num {
    ($name:ident<$value:ty>) => {
        impl_lazy_node_state_ord!($name<$value>);
        impl_node_state_num_ops!($name, <$value as NodeOp>::Output);
    };
}

macro_rules! impl_node_state_num {
    ($name:ident<$value:ty>) => {
        impl_node_state_ord!($name<$value>);
        impl_node_state_num_ops!($name, $value);
    };
}

impl_lazy_node_state_num!(DegreeView<ops::Degree<DynamicGraph>>);

impl<G: StaticGraphViewOps + IntoDynamic + Static> IntoPy<PyObject>
    for LazyNodeState<'static, ops::Degree<G>, DynamicGraph, DynamicGraph>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.into_dyn_hop().into_py(py)
    }
}

impl_timeops!(
    DegreeView,
    inner,
    LazyNodeState<'static, ops::Degree<DynamicGraph>, DynamicGraph>,
    "DegreeView"
);
impl_node_state_num!(NodeStateUsize<usize>);

impl_node_state_num!(NodeStateU64<u64>);

impl_lazy_node_state_ord!(IdView<ops::Id>);
impl_node_state_ord!(NodeStateGID<GID>);

impl_lazy_node_state_ord!(EarliestTimeView<ops::EarliestTime<DynamicGraph>>);
impl_lazy_node_state_ord!(LatestTimeView<ops::LatestTime<DynamicGraph>>);
impl_node_state_ord!(NodeStateOptionI64<Option<i64>>);

impl_lazy_node_state_ord!(NameView<ops::Name>);
impl_node_state_ord!(NodeStateString<String>);

impl_lazy_node_state_ord!(
    EarliestDateTimeView<ops::Map<ops::EarliestTime<DynamicGraph>, Option<DateTime<Utc>>>>
);
impl_lazy_node_state_ord!(
    LatestDateTimeView<ops::Map<ops::LatestTime<DynamicGraph>, Option<DateTime<Utc>>>>
);
impl_node_state_ord!(NodeStateOptionDateTime<Option<DateTime<Utc>>>);

impl_lazy_node_state_ord!(HistoryView<ops::History<DynamicGraph>>);
impl_node_state_ord!(NodeStateListI64<Vec<i64>>);

impl_lazy_node_state_ord!(
    HistoryDateTimeView<ops::Map<ops::History<DynamicGraph>, Option<Vec<DateTime<Utc>>>>>
);
impl_node_state_ord!(NodeStateOptionListDateTime<Option<Vec<DateTime<Utc>>>>);

impl_lazy_node_state_ord!(NodeTypeView<ops::Type>);
impl_node_state_ord!(NodeStateOptionStr<Option<ArcStr>>);

impl_node_state_ord!(NodeStateListDateTime<Vec<DateTime<Utc>>>);
