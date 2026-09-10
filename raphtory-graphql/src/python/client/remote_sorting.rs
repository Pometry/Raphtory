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
/// static factories `by_id` / `by_name` / `by_type` / `by_time` /
/// `by_property` — each enforces that exactly one key type is set per entry.
#[derive(Clone)]
#[pyclass(name = "NodeSortBy", module = "raphtory.graphql", from_py_object)]
pub struct PyNodeSortBy {
    pub inner: NodeSortBy,
}

/// A `NodeSortBy` with every key unset — the base each factory fills in one
/// field of, so adding a server-side key can't silently leave one stale.
fn empty_node_sort_by(reverse: bool) -> NodeSortBy {
    NodeSortBy {
        reverse: Some(reverse),
        id: None,
        name: None,
        type_: None,
        time: None,
        property: None,
    }
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
                id: Some(true),
                ..empty_node_sort_by(reverse)
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
                name: Some(true),
                ..empty_node_sort_by(reverse)
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
                type_: Some(true),
                ..empty_node_sort_by(reverse)
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
                time: Some(time.into()),
                ..empty_node_sort_by(reverse)
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
                property: Some(key),
                ..empty_node_sort_by(reverse)
            },
        }
    }
}

/// One entry in an `Edges.sorted(...)` sort key list. Construct with the
/// static factories `by_src` / `by_dst` / `by_neighbour` / `by_time` /
/// `by_property`.
#[derive(Clone)]
#[pyclass(name = "EdgeSortBy", module = "raphtory.graphql", from_py_object)]
pub struct PyEdgeSortBy {
    pub inner: EdgeSortBy,
}

/// An `EdgeSortBy` with every key unset. `reverse` applies to the `time` /
/// `property` keys only — the node keys carry their own `reverse` inside the
/// nested `NodeSortBy`, so those factories leave it unset.
fn empty_edge_sort_by(reverse: Option<bool>) -> EdgeSortBy {
    EdgeSortBy {
        reverse,
        src: None,
        dst: None,
        neighbour: None,
        time: None,
        property: None,
    }
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
                src: Some(key.inner),
                ..empty_edge_sort_by(None)
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
                dst: Some(key.inner),
                ..empty_edge_sort_by(None)
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
                neighbour: Some(key.inner),
                ..empty_edge_sort_by(None)
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
                time: Some(time.into()),
                ..empty_edge_sort_by(Some(reverse))
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
                property: Some(key),
                ..empty_edge_sort_by(Some(reverse))
            },
        }
    }
}
