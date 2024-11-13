use crate::{
    algorithms::bipartite::max_weight_matching::Matching,
    db::{
        api::view::{DynamicGraph, IntoDynamic, StaticGraphViewOps},
        graph::{edge::EdgeView, edges::Edges, node::NodeView},
    },
    prelude::GraphViewOps,
    python::{types::repr::Repr, utils::PyNodeRef},
};
use pyo3::prelude::*;

#[pyclass(frozen, name = "Matching")]
pub struct PyMatching {
    inner: Matching<DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject> for Matching<G> {
    fn into_py(self, py: Python) -> PyObject {
        PyMatching {
            inner: self.into_dyn(),
        }
        .into_py(py)
    }
}

#[pymethods]
impl PyMatching {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn src(&self, src: PyNodeRef) -> Option<NodeView<DynamicGraph>> {
        self.inner.src(src).map(|n| n.cloned())
    }

    fn dst(&self, dst: PyNodeRef) -> Option<NodeView<DynamicGraph>> {
        self.inner.dst(dst).map(|n| n.cloned())
    }

    fn edges(&self) -> Edges<'static, DynamicGraph> {
        self.inner.edges()
    }

    fn edge_for_src(&self, src: PyNodeRef) -> Option<EdgeView<DynamicGraph>> {
        self.inner.edge_for_src(src).map(|e| e.cloned())
    }

    fn edge_for_dst(&self, dst: PyNodeRef) -> Option<EdgeView<DynamicGraph>> {
        self.inner.edge_for_dst(dst).map(|e| e.cloned())
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
