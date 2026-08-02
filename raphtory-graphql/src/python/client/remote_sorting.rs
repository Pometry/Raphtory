use crate::client::op::{EdgeSortBy, NodeSortBy, SortByTime};
use pyo3::{pyclass, pymethods};

/// Which time boundary of a member to sort by.
#[derive(Clone, Copy)]
#[pyclass(
    name = "SortByTime",
    module = "raphtory.graphql",
    eq,
    eq_int,
    from_py_object
)]
#[derive(PartialEq, Eq)]
pub enum PySortByTime {
    #[pyo3(name = "LATEST")]
    Latest,
    #[pyo3(name = "EARLIEST")]
    Earliest,
}

impl From<PySortByTime> for SortByTime {
    fn from(v: PySortByTime) -> Self {
        match v {
            PySortByTime::Latest => SortByTime::Latest,
            PySortByTime::Earliest => SortByTime::Earliest,
        }
    }
}

/// One entry in a `Nodes.sorted(...)` sort key list. Construct with the
/// static factories `by_id` / `by_time` / `by_property` — each enforces
/// that exactly one key type is set per entry.
#[derive(Clone)]
#[pyclass(name = "NodeSortBy", module = "raphtory.graphql")]
pub struct PyNodeSortBy {
    pub inner: NodeSortBy,
}

#[pymethods]
impl PyNodeSortBy {
    /// Sort by node id (a stable, deterministic ordering).
    ///
    /// Arguments:
    ///     reverse (bool, optional): sort descending. Defaults to False.
    #[staticmethod]
    #[pyo3(signature = (reverse=false))]
    fn by_id(reverse: bool) -> Self {
        Self {
            inner: NodeSortBy {
                reverse: Some(reverse),
                id: Some(true),
                time: None,
                property: None,
            },
        }
    }

    /// Sort by node time (either earliest or latest observed event on the node).
    ///
    /// Arguments:
    ///     time (SortByTime): the time boundary to use.
    ///     reverse (bool, optional): sort descending. Defaults to False.
    #[staticmethod]
    #[pyo3(signature = (time, reverse=false))]
    fn by_time(time: PySortByTime, reverse: bool) -> Self {
        Self {
            inner: NodeSortBy {
                reverse: Some(reverse),
                id: None,
                time: Some(time.into()),
                property: None,
            },
        }
    }

    /// Sort by a temporal property value on each node.
    ///
    /// Arguments:
    ///     key (str): the property name.
    ///     reverse (bool, optional): sort descending. Defaults to False.
    #[staticmethod]
    #[pyo3(signature = (key, reverse=false))]
    fn by_property(key: String, reverse: bool) -> Self {
        Self {
            inner: NodeSortBy {
                reverse: Some(reverse),
                id: None,
                time: None,
                property: Some(key),
            },
        }
    }
}

/// One entry in an `Edges.sorted(...)` sort key list. Construct with the
/// static factories `by_src` / `by_dst` / `by_time` / `by_property`.
#[derive(Clone)]
#[pyclass(name = "EdgeSortBy", module = "raphtory.graphql")]
pub struct PyEdgeSortBy {
    pub inner: EdgeSortBy,
}

#[pymethods]
impl PyEdgeSortBy {
    /// Sort by source node id.
    ///
    /// Arguments:
    ///     reverse (bool, optional): sort descending. Defaults to False.
    #[staticmethod]
    #[pyo3(signature = (reverse=false))]
    fn by_src(reverse: bool) -> Self {
        Self {
            inner: EdgeSortBy {
                reverse: Some(reverse),
                src: Some(true),
                dst: None,
                time: None,
                property: None,
            },
        }
    }

    /// Sort by destination node id.
    ///
    /// Arguments:
    ///     reverse (bool, optional): sort descending. Defaults to False.
    #[staticmethod]
    #[pyo3(signature = (reverse=false))]
    fn by_dst(reverse: bool) -> Self {
        Self {
            inner: EdgeSortBy {
                reverse: Some(reverse),
                src: None,
                dst: Some(true),
                time: None,
                property: None,
            },
        }
    }

    /// Sort by edge time (either earliest or latest event on the edge).
    ///
    /// Arguments:
    ///     time (SortByTime): the time boundary to use.
    ///     reverse (bool, optional): sort descending. Defaults to False.
    #[staticmethod]
    #[pyo3(signature = (time, reverse=false))]
    fn by_time(time: PySortByTime, reverse: bool) -> Self {
        Self {
            inner: EdgeSortBy {
                reverse: Some(reverse),
                src: None,
                dst: None,
                time: Some(time.into()),
                property: None,
            },
        }
    }

    /// Sort by a temporal property value on each edge.
    ///
    /// Arguments:
    ///     key (str): the property name.
    ///     reverse (bool, optional): sort descending. Defaults to False.
    #[staticmethod]
    #[pyo3(signature = (key, reverse=false))]
    fn by_property(key: String, reverse: bool) -> Self {
        Self {
            inner: EdgeSortBy {
                reverse: Some(reverse),
                src: None,
                dst: None,
                time: None,
                property: Some(key),
            },
        }
    }
}
