use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            internal::CreateFilter,
            model::{
                edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter,
                not_filter::NotFilter, or_filter::OrFilter, AndFilter, TryAsCompositeFilter,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
    python::filter::{
        create_filter::DynInternalFilterOps, property_filter_builders::PyPropertyFilterOps,
    },
};
use pyo3::{exceptions::PyTypeError, prelude::*};
use std::sync::Arc;

#[pyclass(frozen, name = "FilterExpr", module = "raphtory.filter")]
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
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        self.0.create_filter(graph)
    }
}

pub enum AcceptFilter {
    Expr(PyFilterExpr),
    Chain(Py<PyPropertyFilterOps>),
}

impl<'py> FromPyObject<'py> for AcceptFilter {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(expr) = ob.extract::<PyFilterExpr>() {
            return Ok(AcceptFilter::Expr(expr));
        }

        if let Ok(chain) = ob.extract::<Py<PyPropertyFilterOps>>() {
            return Ok(AcceptFilter::Chain(chain));
        }

        Err(PyTypeError::new_err(
            "Expected a FilterExpr or a PropertyFilterOps chain",
        ))
    }
}
