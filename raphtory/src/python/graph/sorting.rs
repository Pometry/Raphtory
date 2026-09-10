//! Python wrappers for the collection sort keys ([`crate::db::api::view::sort`]).
//!
//! The same classes serve the local `Nodes.sorted(...)` / `Edges.sorted(...)`
//! and the remote collections in `raphtory.graphql`, so a drop-in swap between
//! `Graph` and `RemoteGraph` uses one set of sort-key types.

use crate::db::api::view::sort::{EdgeSortBy, EdgeSortKey, NodeSortBy, NodeSortKey, SortByTime};
use pyo3::{pyclass, pymethods};

/// Which time boundary of a member to sort by.
#[derive(Clone, Copy)]
#[pyclass(name = "SortByTime", module = "raphtory", eq, eq_int, from_py_object)]
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
/// static factories `by_id` / `by_name` / `by_type` / `by_time` /
/// `by_property` — the key is an enum, so exactly one is set by construction.
#[derive(Clone)]
#[pyclass(name = "NodeSortBy", module = "raphtory", from_py_object)]
pub struct PyNodeSortBy {
    pub inner: NodeSortBy,
}

#[pymethods]
impl PyNodeSortBy {
    /// Sort by node id (a stable, deterministic ordering).
    ///
    /// Arguments:
    ///     reverse (bool, optional): sort descending. Defaults to False.
    ///
    /// Returns:
    ///     NodeSortBy: a sort key usable in `Nodes.sorted(...)`.
    #[staticmethod]
    #[pyo3(signature = (reverse=false))]
    fn by_id(reverse: bool) -> Self {
        Self {
            inner: NodeSortBy {
                reverse,
                key: NodeSortKey::Id,
            },
        }
    }

    /// Sort by node name.
    ///
    /// Arguments:
    ///     reverse (bool, optional): sort descending. Defaults to False.
    ///
    /// Returns:
    ///     NodeSortBy: a sort key usable in `Nodes.sorted(...)`.
    #[staticmethod]
    #[pyo3(signature = (reverse=false))]
    fn by_name(reverse: bool) -> Self {
        Self {
            inner: NodeSortBy {
                reverse,
                key: NodeSortKey::Name,
            },
        }
    }

    /// Sort by node type. Untyped nodes sort first, before any named type.
    ///
    /// Arguments:
    ///     reverse (bool, optional): sort descending. Defaults to False.
    ///
    /// Returns:
    ///     NodeSortBy: a sort key usable in `Nodes.sorted(...)`.
    #[staticmethod]
    #[pyo3(signature = (reverse=false))]
    fn by_type(reverse: bool) -> Self {
        Self {
            inner: NodeSortBy {
                reverse,
                key: NodeSortKey::Type,
            },
        }
    }

    /// Sort by node time (either earliest or latest observed event on the node).
    ///
    /// Arguments:
    ///     time (SortByTime): the time boundary to use.
    ///     reverse (bool, optional): sort descending. Defaults to False.
    ///
    /// Returns:
    ///     NodeSortBy: a sort key usable in `Nodes.sorted(...)`.
    #[staticmethod]
    #[pyo3(signature = (time, reverse=false))]
    fn by_time(time: PySortByTime, reverse: bool) -> Self {
        Self {
            inner: NodeSortBy {
                reverse,
                key: NodeSortKey::Time(time.into()),
            },
        }
    }

    /// Sort by a temporal property value on each node.
    ///
    /// Arguments:
    ///     key (str): the property name.
    ///     reverse (bool, optional): sort descending. Defaults to False.
    ///
    /// Returns:
    ///     NodeSortBy: a sort key usable in `Nodes.sorted(...)`.
    #[staticmethod]
    #[pyo3(signature = (key, reverse=false))]
    fn by_property(key: String, reverse: bool) -> Self {
        Self {
            inner: NodeSortBy {
                reverse,
                key: NodeSortKey::Property(key),
            },
        }
    }
}

/// One entry in an `Edges.sorted(...)` sort key list. Construct with the
/// static factories `by_src` / `by_dst` / `by_neighbour` / `by_time` /
/// `by_property`.
#[derive(Clone)]
#[pyclass(name = "EdgeSortBy", module = "raphtory", from_py_object)]
pub struct PyEdgeSortBy {
    pub inner: EdgeSortBy,
}

#[pymethods]
impl PyEdgeSortBy {
    /// Sort by the source node, using a node sort key.
    ///
    /// Arguments:
    ///     key (NodeSortBy): how to order the source nodes, e.g.
    ///         `NodeSortBy.by_id()`. Its own `reverse` controls direction.
    ///
    /// Returns:
    ///     EdgeSortBy: a sort key usable in `Edges.sorted(...)`.
    #[staticmethod]
    #[pyo3(signature = (key))]
    fn by_src(key: PyNodeSortBy) -> Self {
        Self {
            inner: EdgeSortBy {
                // direction comes from the nested node key
                reverse: false,
                key: EdgeSortKey::Src(key.inner),
            },
        }
    }

    /// Sort by the destination node, using a node sort key.
    ///
    /// Arguments:
    ///     key (NodeSortBy): how to order the destination nodes, e.g.
    ///         `NodeSortBy.by_id()`. Its own `reverse` controls direction.
    ///
    /// Returns:
    ///     EdgeSortBy: a sort key usable in `Edges.sorted(...)`.
    #[staticmethod]
    #[pyo3(signature = (key))]
    fn by_dst(key: PyNodeSortBy) -> Self {
        Self {
            inner: EdgeSortBy {
                // direction comes from the nested node key
                reverse: false,
                key: EdgeSortKey::Dst(key.inner),
            },
        }
    }

    /// Sort by the neighbour node, using a node sort key. The neighbour is the
    /// endpoint that is NOT the node the edges were traversed from — for a
    /// graph-level edge collection that is the destination.
    ///
    /// Arguments:
    ///     key (NodeSortBy): how to order the neighbour nodes, e.g.
    ///         `NodeSortBy.by_name()`. Its own `reverse` controls direction.
    ///
    /// Returns:
    ///     EdgeSortBy: a sort key usable in `Edges.sorted(...)`.
    #[staticmethod]
    #[pyo3(signature = (key))]
    fn by_neighbour(key: PyNodeSortBy) -> Self {
        Self {
            inner: EdgeSortBy {
                // direction comes from the nested node key
                reverse: false,
                key: EdgeSortKey::Neighbour(key.inner),
            },
        }
    }

    /// Sort by edge time (either earliest or latest event on the edge).
    ///
    /// Arguments:
    ///     time (SortByTime): the time boundary to use.
    ///     reverse (bool, optional): sort descending. Defaults to False.
    ///
    /// Returns:
    ///     EdgeSortBy: a sort key usable in `Edges.sorted(...)`.
    #[staticmethod]
    #[pyo3(signature = (time, reverse=false))]
    fn by_time(time: PySortByTime, reverse: bool) -> Self {
        Self {
            inner: EdgeSortBy {
                reverse,
                key: EdgeSortKey::Time(time.into()),
            },
        }
    }

    /// Sort by a temporal property value on each edge.
    ///
    /// Arguments:
    ///     key (str): the property name.
    ///     reverse (bool, optional): sort descending. Defaults to False.
    ///
    /// Returns:
    ///     EdgeSortBy: a sort key usable in `Edges.sorted(...)`.
    #[staticmethod]
    #[pyo3(signature = (key, reverse=false))]
    fn by_property(key: String, reverse: bool) -> Self {
        Self {
            inner: EdgeSortBy {
                reverse,
                key: EdgeSortKey::Property(key),
            },
        }
    }
}
