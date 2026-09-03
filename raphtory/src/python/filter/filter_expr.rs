use crate::{
    db::{
        api::{
            state::ops::NodeOp,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter,
                not_filter::NotFilter, or_filter::OrFilter, AndFilter, DynCreateFilter, FilterTree,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
};
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(
    frozen,
    name = "FilterExpr",
    module = "raphtory.filter",
    subclass,
    from_py_object
)]
#[derive(Clone)]
pub struct PyFilterExpr(pub Arc<dyn DynCreateFilter>, pub Option<FilterTree>);

impl PyFilterExpr {
    /// The wire form recorded at construction; filters built in ways the wire
    /// schema cannot express (an expression on both sides of a comparison)
    /// carry none and cannot be sent to a server.
    pub fn try_as_filter_tree(&self) -> Result<FilterTree, GraphError> {
        self.1.clone().ok_or_else(|| {
            GraphError::InvalidFilter(
                "this filter has no server-side form; use plain values rather than \
                 expressions on the right-hand side of comparisons"
                    .to_string(),
            )
        })
    }
}

#[pymethods]
impl PyFilterExpr {
    pub fn __and__(&self, other: &Self) -> Self {
        let left = self.0.clone();
        let right = other.0.clone();
        let wire = match (&self.1, &other.1) {
            (Some(a), Some(b)) => Some(FilterTree::And(vec![a.clone(), b.clone()])),
            _ => None,
        };
        PyFilterExpr(Arc::new(AndFilter { left, right }), wire)
    }

    pub fn __or__(&self, other: &Self) -> Self {
        let left = self.0.clone();
        let right = other.0.clone();
        let wire = match (&self.1, &other.1) {
            (Some(a), Some(b)) => Some(FilterTree::Or(vec![a.clone(), b.clone()])),
            _ => None,
        };
        PyFilterExpr(Arc::new(OrFilter { left, right }), wire)
    }

    fn __invert__(&self) -> Self {
        let wire = self.1.clone().map(|t| FilterTree::Not(Box::new(t)));
        PyFilterExpr(Arc::new(NotFilter(self.0.clone())), wire)
    }
}

impl CreateFilter for PyFilterExpr {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn BoxableGraphView + 'graph>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph;

    type FilteredGraph<'graph, G>
        = Arc<dyn BoxableGraphView + 'graph>
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
        self.0.dyn_filter_graph_view(Arc::new(graph))
    }
}
