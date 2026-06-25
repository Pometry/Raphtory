use crate::{
    db::{
        api::{
            state::ops::NodeOp,
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            model::{not_filter::NotFilter, or_filter::OrFilter, AndFilter, DynCreateFilter},
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
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
pub struct PyFilterExpr(pub Arc<dyn DynCreateFilter>);

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
