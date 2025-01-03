use pyo3::prelude::*;

use crate::{
    add_classes,
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
    types::PyNotImplemented,
};
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
use std::{collections::HashMap, sync::Arc};

macro_rules! impl_node_state_ops {
    ($name:ident, $value:ty, $inner_t:ty, $to_owned:expr, $computed:literal, $py_value:literal) => {
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

            /// Iterate over nodes
            ///
            /// Returns:
            ///     Iterator[Node]
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

            /// Returns:
            #[doc = concat!("     Iterator[Tuple[Node, ", $py_value, "]]")]
            fn items(&self) -> PyBorrowingIterator {
                py_borrowing_iter!(self.inner.clone(), $inner_t, |inner| inner
                    .iter()
                    .map(|(n, v)| (n.cloned(), ($to_owned)(v))))
            }

            /// Returns:
            #[doc = concat!("     Iterator[",$py_value, "]")]
            fn values(&self) -> PyBorrowingIterator {
                self.__iter__()
            }

            /// Sort results by node id
            ///
            /// Returns:
            #[doc = concat!("     ", $computed)]
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
    ($name:ident, $value:ty, $to_owned:expr, $computed:literal, $py_value:literal) => {
        #[pymethods]
        impl $name {
            /// Sort by value
            ///
            /// Arguments:
            ///     reverse (bool): If `True`, sort in descending order, otherwise ascending. Defaults to False.
            ///
            /// Returns:
            #[doc = concat!("     ", $computed)]
            #[pyo3(signature = (reverse = false))]
            fn sorted(&self, reverse: bool) -> NodeState<'static, $value, DynamicGraph> {
                self.inner.sort_by_values(reverse)
            }

            /// Compute the k largest values
            ///
            /// Arguments:
            ///     k (int): The number of values to return
            ///
            /// Returns:
            #[doc = concat!("     ", $computed)]
            fn top_k(&self, k: usize) -> NodeState<'static, $value, DynamicGraph> {
                self.inner.top_k(k)
            }

            /// Compute the k smallest values
            ///
            /// Arguments:
            ///     k (int): The number of values to return
            ///
            /// Returns:
            #[doc = concat!("     ", $computed)]
            fn bottom_k(&self, k: usize) -> NodeState<'static, $value, DynamicGraph> {
                self.inner.bottom_k(k)
            }

            /// Return smallest value and corresponding node
            ///
            /// Returns:
            #[doc = concat!("     Optional[Tuple[Node, ", $py_value,"]]")]
            fn min_item(&self) -> Option<(NodeView<DynamicGraph>, $value)> {
                self.inner
                    .min_item()
                    .map(|(n, v)| (n.cloned(), ($to_owned)(v)))
            }

            /// Return the minimum value
            ///
            /// Returns:
            #[doc = concat!("     Optional[", $py_value, "]")]
            fn min(&self) -> Option<$value> {
                self.inner.min().map($to_owned)
            }

            /// Return largest value and corresponding node
            ///
            /// Returns:
            #[doc = concat!("     Optional[Tuple[Node, ", $py_value,"]]")]
            fn max_item(&self) -> Option<(NodeView<DynamicGraph>, $value)> {
                self.inner
                    .max_item()
                    .map(|(n, v)| (n.cloned(), ($to_owned)(v)))
            }

            /// Return the maximum value
            ///
            /// Returns:
            #[doc = concat!("     Optional[", $py_value, "]")]
            fn max(&self) -> Option<$value> {
                self.inner.max().map($to_owned)
            }

            /// Return the median value
            ///
            /// Returns:
            #[doc = concat!("     Optional[", $py_value, "]")]
            fn median(&self) -> Option<$value> {
                self.inner.median().map($to_owned)
            }

            /// Return medain value and corresponding node
            ///
            /// Returns:
            #[doc = concat!("     Optional[Tuple[Node, ", $py_value,"]]")]
            fn median_item(&self) -> Option<(NodeView<DynamicGraph>, $value)> {
                self.inner
                    .median_item()
                    .map(|(n, v)| (n.cloned(), ($to_owned)(v)))
            }

            fn __eq__<'py>(
                &self,
                other: &Bound<'py, PyAny>,
                py: Python<'py>,
            ) -> Result<Bound<'py, PyAny>, std::convert::Infallible> {
                let res = if let Ok(other) = other.downcast::<Self>() {
                    let other = Bound::borrow(other);
                    self.inner.values().eq(other.inner.values())
                } else if let Ok(other) = other.extract::<Vec<$value>>() {
                    self.inner.values().map($to_owned).eq(other.into_iter())
                } else if let Ok(other) = other.extract::<HashMap<PyNodeRef, $value>>() {
                    (self.inner.len() == other.len()
                        && other.into_iter().all(|(node, value)| {
                            self.inner.get_by_node(node).map($to_owned) == Some(value)
                        }))
                } else {
                    return Ok(PyNotImplemented::get(py).to_owned().into_any());
                };
                Ok(res.into_pyobject(py)?.to_owned().into_any())
            }
        }
    };
}

macro_rules! impl_node_state_num_ops {
    ($name:ident, $value:ty, $py_value:literal) => {
        #[pymethods]
        impl $name {
            /// sum of values over all nodes
            ///
            /// Returns:
            #[doc= concat!("        ", $py_value)]
            fn sum(&self) -> $value {
                self.inner.sum()
            }

            /// mean of values over all nodes
            ///
            /// Returns:
            ///     float
            fn mean(&self) -> f64 {
                self.inner.mean()
            }
        }
    };
}

macro_rules! impl_lazy_node_state {
    ($name:ident<$op:ty>, $computed:literal, $py_value:literal) => {
        /// A lazy view over node values
        #[pyclass(module = "raphtory.node_state", frozen)]
        pub struct $name {
            inner: LazyNodeState<'static, $op, DynamicGraph, DynamicGraph>,
        }

        impl $name {
            pub fn inner(&self) -> &LazyNodeState<'static, $op, DynamicGraph, DynamicGraph> {
                &self.inner
            }
        }

        #[pymethods]
        impl $name {
            /// Compute all values and return the result as a node view
            ///
            /// Returns:
            #[doc = concat!("     ", $computed)]
            fn compute(
                &self,
            ) -> NodeState<'static, <$op as NodeOp>::Output, DynamicGraph, DynamicGraph> {
                self.inner.compute()
            }

            /// Compute all values and return the result as a list
            ///
            /// Returns
            #[doc = concat!("     list[", $py_value, "]")]
            fn collect(&self) -> Vec<<$op as NodeOp>::Output> {
                self.inner.collect()
            }
        }

        impl_node_state_ops!(
            $name,
            <$op as NodeOp>::Output,
            LazyNodeState<'static, $op, DynamicGraph, DynamicGraph>,
            |v: <$op as NodeOp>::Output| v,
            $computed,
            $py_value
        );

        impl From<LazyNodeState<'static, $op, DynamicGraph, DynamicGraph>> for $name {
            fn from(inner: LazyNodeState<'static, $op, DynamicGraph, DynamicGraph>) -> Self {
                $name { inner }
            }
        }

        impl<'py> pyo3::IntoPyObject<'py>
            for LazyNodeState<'static, $op, DynamicGraph, DynamicGraph>
        {
            type Target = $name;
            type Output = Bound<'py, Self::Target>;
            type Error = <Self::Target as pyo3::IntoPyObject<'py>>::Error;

            fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
                $name::from(self).into_pyobject(py)
            }
        }
    };
}

macro_rules! impl_node_state {
    ($name:ident<$value:ty>, $computed:literal, $py_value:literal) => {
        #[pyclass(module = "raphtory.node_state", frozen)]
        pub struct $name {
            inner: Arc<NodeState<'static, $value, DynamicGraph, DynamicGraph>>,
        }

        impl $name {
            pub fn inner(&self) -> &Arc<NodeState<'static, $value, DynamicGraph, DynamicGraph>> {
                &self.inner
            }
        }

        impl_node_state_ops!(
            $name,
            $value,
            Arc<NodeState<'static, $value, DynamicGraph, DynamicGraph>>,
            |v: &$value| v.clone(),
            $computed,
            $py_value
        );

        impl From<NodeState<'static, $value, DynamicGraph, DynamicGraph>> for $name {
            fn from(inner: NodeState<'static, $value, DynamicGraph, DynamicGraph>) -> Self {
                $name {
                    inner: inner.into(),
                }
            }
        }

        impl From<Arc<NodeState<'static, $value, DynamicGraph, DynamicGraph>>> for $name {
            fn from(inner: Arc<NodeState<'static, $value, DynamicGraph, DynamicGraph>>) -> Self {
                $name { inner }
            }
        }

        impl<'py> pyo3::IntoPyObject<'py>
            for NodeState<'static, $value, DynamicGraph, DynamicGraph>
        {
            type Target = $name;
            type Output = Bound<'py, Self::Target>;
            type Error = <Self::Target as pyo3::IntoPyObject<'py>>::Error;

            fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
                $name::from(self).into_pyobject(py)
            }
        }
    };
}

macro_rules! impl_lazy_node_state_ord {
    ($name:ident<$value:ty>, $computed:literal, $py_value:literal) => {
        impl_lazy_node_state!($name<$value>, $computed, $py_value);
        impl_node_state_ord_ops!(
            $name,
            <$value as NodeOp>::Output,
            |v: <$value as NodeOp>::Output| v,
            $computed,
            $py_value
        );
    };
}

macro_rules! impl_node_state_ord {
    ($name:ident<$value:ty>, $computed:literal, $py_value:literal) => {
        impl_node_state!($name<$value>, $computed, $py_value);
        impl_node_state_ord_ops!($name, $value, |v: &$value| v.clone(), $computed, $py_value);
    };
}

macro_rules! impl_lazy_node_state_num {
    ($name:ident<$value:ty>, $computed:literal, $py_value:literal) => {
        impl_lazy_node_state_ord!($name<$value>, $computed, $py_value);
        impl_node_state_num_ops!($name, <$value as NodeOp>::Output, $py_value);
    };
}

macro_rules! impl_node_state_num {
    ($name:ident<$value:ty>, $computed:literal, $py_value:literal) => {
        impl_node_state_ord!($name<$value>, $computed, $py_value);
        impl_node_state_num_ops!($name, $value, $py_value);
    };
}

macro_rules! impl_one_hop {
        ($name:ident<$($path:ident)::+>, $py_name:literal) => {
            impl<'py, G: StaticGraphViewOps + IntoDynamic + Static> pyo3::IntoPyObject<'py>
                for LazyNodeState<'static, $($path)::+<G>, DynamicGraph, DynamicGraph>
            {
                type Target = $name;
                type Output = Bound<'py, Self::Target>;
                type Error = <Self::Target as pyo3::IntoPyObject<'py>>::Error;

                fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
                    self.into_dyn_hop().into_pyobject(py)
                }
            }

            impl_timeops!($name, inner, LazyNodeState<'static, $($path)::+<DynamicGraph>, DynamicGraph>, $py_name);
            impl_layerops!($name, inner, LazyNodeState<'static, $($path)::+<DynamicGraph>, DynamicGraph>, $py_name);
        }
    }

impl_lazy_node_state_num!(
    DegreeView<ops::Degree<DynamicGraph>>,
    "NodeStateUsize",
    "int"
);
impl_one_hop!(DegreeView<ops::Degree>, "DegreeView");

impl_node_state_num!(NodeStateUsize<usize>, "NodeStateUsize", "int");

impl_node_state_num!(NodeStateU64<u64>, "NodeStateU64", "int");

impl_lazy_node_state_ord!(IdView<ops::Id>, "NodeStateGID", "GID");
impl_node_state_ord!(NodeStateGID<GID>, "NodeStateGID", "GID");

impl_lazy_node_state_ord!(
    EarliestTimeView<ops::EarliestTime<DynamicGraph>>,
    "NodeStateOptionI64",
    "Optional[int]"
);
impl_one_hop!(EarliestTimeView<ops::EarliestTime>, "EarliestTimeView");
impl_lazy_node_state_ord!(
    LatestTimeView<ops::LatestTime<DynamicGraph>>,
    "NodeStateOptionI64",
    "Optional[int]"
);
impl_one_hop!(LatestTimeView<ops::LatestTime>, "LatestTimeView");
impl_node_state_ord!(
    NodeStateOptionI64<Option<i64>>,
    "NodeStateOptionI64",
    "Optional[int]"
);

impl_lazy_node_state_ord!(NameView<ops::Name>, "NodeStateString", "str");
impl_node_state_ord!(NodeStateString<String>, "NodeStateString", "str");

type EarliestDateTime<G> = ops::Map<ops::EarliestTime<G>, Option<DateTime<Utc>>>;
impl_lazy_node_state_ord!(
    EarliestDateTimeView<EarliestDateTime<DynamicGraph>>,
    "NodeStateOptionDateTime",
    "Optional[Datetime]"
);
impl_one_hop!(
    EarliestDateTimeView<EarliestDateTime>,
    "EarliestDateTimeView"
);

type LatestDateTime<G> = ops::Map<ops::LatestTime<G>, Option<DateTime<Utc>>>;
impl_lazy_node_state_ord!(
    LatestDateTimeView<ops::Map<ops::LatestTime<DynamicGraph>, Option<DateTime<Utc>>>>,
    "NodeStateOptionDateTime",
    "Optional[Datetime]"
);
impl_one_hop!(LatestDateTimeView<LatestDateTime>, "LatestDateTimeView");
impl_node_state_ord!(
    NodeStateOptionDateTime<Option<DateTime<Utc>>>,
    "NodeStateOptionDateTime",
    "Optional[Datetime]"
);

impl_lazy_node_state_ord!(
    HistoryView<ops::History<DynamicGraph>>,
    "NodeStateListI64",
    "list[int]"
);
impl_one_hop!(HistoryView<ops::History>, "HistoryView");
impl_node_state_ord!(NodeStateListI64<Vec<i64>>, "NodeStateListI64", "list[int]");

type HistoryDateTime<G> = ops::Map<ops::History<G>, Option<Vec<DateTime<Utc>>>>;
impl_lazy_node_state_ord!(
    HistoryDateTimeView<HistoryDateTime<DynamicGraph>>,
    "NodeStateOptionListDateTime",
    "Optional[list[Datetime]]"
);
impl_one_hop!(HistoryDateTimeView<HistoryDateTime>, "HistoryDateTimeView");
impl_node_state_ord!(
    NodeStateOptionListDateTime<Option<Vec<DateTime<Utc>>>>,
    "NodeStateOptionListDateTime",
    "Optional[list[Datetime]]"
);

impl_lazy_node_state_ord!(
    NodeTypeView<ops::Type>,
    "NodeStateOptionStr",
    "Optional[str]"
);
impl_node_state_ord!(
    NodeStateOptionStr<Option<ArcStr>>,
    "NodeStateOptionStr",
    "Optional[str]"
);

impl_node_state_ord!(
    NodeStateListDateTime<Vec<DateTime<Utc>>>,
    "NodeStateListDateTime",
    "list[Datetime]"
);

pub fn base_node_state_module(py: Python<'_>) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "node_state")?;
    add_classes!(
        &m,
        DegreeView,
        NodeStateUsize,
        NodeStateU64,
        IdView,
        NodeStateGID,
        EarliestTimeView,
        LatestTimeView,
        NameView,
        NodeStateString,
        EarliestDateTimeView,
        LatestDateTimeView,
        NodeStateOptionDateTime,
        HistoryView,
        NodeStateListI64,
        HistoryDateTimeView,
        NodeStateOptionListDateTime,
        NodeTypeView,
        NodeStateOptionStr,
        NodeStateListDateTime
    );
    Ok(m)
}
