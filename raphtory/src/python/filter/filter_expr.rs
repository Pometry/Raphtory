use crate::{
    db::{
        api::{
            state::NodeOp,
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{model::expr::FilterExpr, CreateFilter, DynEdgeFilter},
    },
    errors::GraphError,
    python::filter::{node_expr::PyExpr, repr},
};
use pyo3::{exceptions::PyTypeError, prelude::*, Borrowed};
use std::sync::Arc;

/// A filter as a tree. The same tree runs locally, is sent to a server, and is
/// what `repr` prints, so there is nothing to keep in step.
///
/// Anywhere a filter is expected, a yes/no `Expr` is accepted too: it is the
/// filter on its own entity.
///
/// `&`, `|` and `~` combine filters. `~` keeps what the filter drops: a negated
/// node test keeps the nodes that fail it and the edges between them, and a
/// combination is negated test by test, node tests on nodes and edge tests on
/// edges. A view combines with `&` only.
#[pyclass(
    frozen,
    name = "FilterExpr",
    module = "raphtory.filter",
    subclass,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyFilterExpr(pub FilterExpr);

impl PyFilterExpr {
    /// The tree itself, for a caller that owns the wrapper and is done with it.
    pub fn into_tree(self) -> FilterExpr {
        self.0
    }
}

impl<'py> FromPyObject<'_, 'py> for PyFilterExpr {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(filter) = ob.cast::<PyFilterExpr>() {
            return Ok(filter.get().clone());
        }
        if let Ok(expr) = ob.cast::<PyExpr>() {
            return Ok(PyFilterExpr(expr.get().0.clone().into_filter()));
        }
        Err(PyTypeError::new_err(format!(
            "expected a filter (filter.Expr or filter.FilterExpr), got {}",
            ob.get_type().name()?
        )))
    }
}

/// Either side of `&`, `|`: a yes/no expression or a filter.
#[derive(FromPyObject)]
pub(crate) enum ExprOrFilter {
    Expr(PyExpr),
    Filter(PyFilterExpr),
}

impl ExprOrFilter {
    pub(crate) fn into_filter(self) -> FilterExpr {
        match self {
            ExprOrFilter::Expr(e) => e.0.into_filter(),
            ExprOrFilter::Filter(f) => f.0,
        }
    }
}

#[pymethods]
impl PyFilterExpr {
    fn __and__(&self, other: ExprOrFilter) -> Self {
        PyFilterExpr(FilterExpr::And(vec![self.0.clone(), other.into_filter()]))
    }

    fn __or__(&self, other: ExprOrFilter) -> PyResult<Self> {
        let other = other.into_filter();
        no_view(&self.0)?;
        no_view(&other)?;
        Ok(PyFilterExpr(FilterExpr::Or(vec![self.0.clone(), other])))
    }

    fn __invert__(&self) -> PyResult<Self> {
        no_view(&self.0)?;
        Ok(PyFilterExpr(FilterExpr::Not(Box::new(self.0.clone()))))
    }

    /// The Python expression that builds this filter, module-qualified, so
    /// `eval` rebuilds it after `import raphtory`.
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        repr::filter(py, &self.0)
    }
}

/// A view applies to the whole filter, so it can be `&`-ed with predicates or applied
/// alone, but has no meaning under `|` or `~`. Refused where it is written, as the
/// engine would refuse it when applied.
pub(crate) fn no_view(filter: &FilterExpr) -> PyResult<()> {
    if filter.has_view() {
        return Err(PyTypeError::new_err(
            "a view (filter.Graph...) applies to the whole filter: combine it with `&` or apply it alone, not with `|` or `~`",
        ));
    }
    Ok(())
}

impl CreateFilter for PyFilterExpr {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.0.create_graph_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        self.0.create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        self.0.create_edge_filter(graph)
    }
}
