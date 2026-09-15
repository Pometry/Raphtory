use crate::{
    db::{
        api::{
            state::NodeOp,
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{model::tree::FilterExpr, CreateFilter},
    },
    errors::GraphError,
};
use pyo3::prelude::*;
use std::sync::Arc;

/// A filter as a tree. The same tree runs locally, is sent to a server, and is
/// what `repr` prints, so there is nothing to keep in step.
#[pyclass(
    frozen,
    name = "FilterExpr",
    module = "raphtory.filter",
    subclass,
    from_py_object
)]
#[derive(Clone)]
pub struct PyFilterExpr(pub FilterExpr);

impl PyFilterExpr {
    pub fn tree(&self) -> &FilterExpr {
        &self.0
    }
}

#[pymethods]
impl PyFilterExpr {
    pub fn __and__(&self, other: &Self) -> Self {
        PyFilterExpr(FilterExpr::And(vec![self.0.clone(), other.0.clone()]))
    }

    pub fn __or__(&self, other: &Self) -> Self {
        PyFilterExpr(FilterExpr::Or(vec![self.0.clone(), other.0.clone()]))
    }

    fn __invert__(&self) -> Self {
        PyFilterExpr(FilterExpr::Not(Box::new(self.0.clone())))
    }

    /// Shows the filter tree: what runs locally and what a server receives.
    fn __repr__(&self) -> String {
        format!("FilterExpr({})", self.0)
    }
}

impl CreateFilter for PyFilterExpr {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph;

    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        self.0.create_filter(graph, filtered)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        self.0.create_node_filter(graph, filtered)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.0.filter_graph_view(graph)
    }
}
