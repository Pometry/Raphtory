use crate::{
    algorithms::bipartite::max_weight_matching::Matching,
    db::{
        api::view::{DynamicGraph, IntoDynamic, StaticGraphViewOps},
        graph::{edge::EdgeView, edges::Edges, node::NodeView},
    },
    prelude::GraphViewOps,
    python::{
        types::{repr::Repr, wrappers::iterators::PyBorrowingIterator},
        utils::PyNodeRef,
    },
};
use pyo3::prelude::*;

/// A Matching (i.e., a set of edges that do not share any nodes)
#[pyclass(frozen, name = "Matching", module = "raphtory.algorithms")]
pub struct PyMatching {
    inner: Matching<DynamicGraph>,
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for Matching<G> {
    type Target = PyMatching;
    type Output = Bound<'py, PyMatching>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyMatching {
            inner: self.into_dyn(),
        }
        .into_pyobject(py)
    }
}

#[pymethods]
impl PyMatching {
    /// The number of edges in the matching
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Returns True if the matching is not empty
    fn __bool__(&self) -> bool {
        !self.inner.is_empty()
    }

    fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(self.inner.clone(), Matching<DynamicGraph>, |inner| inner
            .edges_iter())
    }

    /// Get the matched source node for a destination node
    ///
    /// Arguments:
    ///     dst (NodeInput): The destination node
    ///
    /// Returns:
    ///     Optional[Node]: The matched source node if it exists
    ///
    fn src(&self, dst: PyNodeRef) -> Option<NodeView<DynamicGraph>> {
        self.inner.src(dst).map(|n| n.cloned())
    }

    /// Get the matched destination node for a source node
    ///
    /// Arguments:
    ///     src (NodeInput): The source node
    ///
    /// Returns:
    ///     Optional[Node]: The matched destination node if it exists
    ///
    fn dst(&self, src: PyNodeRef) -> Option<NodeView<DynamicGraph>> {
        self.inner.dst(src).map(|n| n.cloned())
    }

    /// Get a view of the matched edges
    ///
    /// Returns:
    ///     Edges: The edges in the matching
    fn edges(&self) -> Edges<'static, DynamicGraph> {
        self.inner.edges()
    }

    /// Get the matched edge for a source node
    ///
    /// Arguments:
    ///     src (NodeInput): The source node
    ///
    /// Returns:
    ///     Optional[Edge]: The matched edge if it exists
    fn edge_for_src(&self, src: PyNodeRef) -> Option<EdgeView<DynamicGraph>> {
        self.inner.edge_for_src(src).map(|e| e.cloned())
    }

    /// Get the matched edge for a destination node
    ///
    /// Arguments:
    ///     dst (NodeInput): The source node
    ///
    /// Returns:
    ///     Optional[Edge]: The matched edge if it exists
    fn edge_for_dst(&self, dst: PyNodeRef) -> Option<EdgeView<DynamicGraph>> {
        self.inner.edge_for_dst(dst).map(|e| e.cloned())
    }

    /// Check if an edge is part of the matching
    ///
    /// Arguments:
    ///     edge (Tuple[NodeInput, NodeInput]): The edge to check
    ///
    /// Returns:
    ///     bool: Returns True if the edge is part of the matching, False otherwise
    fn __contains__(&self, edge: (PyNodeRef, PyNodeRef)) -> bool {
        self.inner.contains(edge.0, edge.1)
    }

    fn __repr__(&self) -> String {
        self.inner.repr()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Repr for Matching<G> {
    fn repr(&self) -> String {
        format!("{self}")
    }
}
