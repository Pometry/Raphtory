use crate::{
    db::{
        api::{
            state::ops::NodeOp,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter,
                not_filter::NotFilter, or_filter::OrFilter, AndFilter, TryAsCompositeFilter,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
    python::filter::create_filter::DynInternalFilterOps,
};
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(frozen, name = "FilterExpr", module = "raphtory.filter", subclass)]
#[derive(Clone)]
pub struct PyFilterExpr(pub Arc<dyn DynInternalFilterOps>);

impl PyFilterExpr {
    pub fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        self.0.try_as_composite_node_filter()
    }

    pub fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        self.0.try_as_composite_edge_filter()
    }
}

#[pymethods]
impl PyFilterExpr {
    pub fn __and__(&self, other: &Self) -> Self {
        let left = self.0.clone();
        let right = other.0.clone();
        PyFilterExpr(Arc::new(AndFilter { left, right }))
    }

    pub fn __or__(&self, other: &Self) -> Self {
        let left = self.0.clone();
        let right = other.0.clone();
        PyFilterExpr(Arc::new(OrFilter { left, right }))
    }

    fn __invert__(&self) -> Self {
        PyFilterExpr(Arc::new(NotFilter(self.0.clone())))
    }
}

impl CreateFilter for PyFilterExpr {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    type NodeFilter<'graph, G: GraphView + 'graph>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        self.0.create_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        self.0.create_node_filter(graph)
    }
}
