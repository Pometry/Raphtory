use crate::python::{
    filter::{
        edge_expr::{PyEdgeEndpoint, PyEdgeFilter},
        exploded_edge_expr::PyExplodedEdgeFilter,
        filter_expr::PyFilterExpr,
        graph_filter::PyGraphFilter,
        node_expr::PyNodeFilter,
    },
    types::iterable::FromIterable,
};
use pyo3::{
    prelude::{PyModule, PyModuleMethods},
    Bound, PyErr, Python,
};
use raphtory_api::core::entities::Layer;

pub mod edge_expr;
pub mod exploded_edge_expr;
pub mod filter_expr;
pub mod graph_filter;
pub mod node_expr;
pub(crate) mod wire;

impl From<FromIterable<String>> for Layer {
    fn from(iter: FromIterable<String>) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}

pub fn base_filter_module(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
    let filter_module = PyModule::new(py, "filter")?;

    filter_module.add_class::<PyFilterExpr>()?;

    filter_module.add_class::<PyNodeFilter>()?;

    filter_module.add_class::<PyEdgeFilter>()?;
    filter_module.add_class::<PyEdgeEndpoint>()?;

    filter_module.add_class::<PyExplodedEdgeFilter>()?;
    filter_module.add_class::<PyGraphFilter>()?;

    // The entry points are instances: `filter.Edge.src()` chains through
    // instance methods, so the module attributes shadow the classes with
    // ready-made roots.
    filter_module.add("Node", PyNodeFilter::root())?;
    filter_module.add("Edge", PyEdgeFilter::root())?;
    filter_module.add("ExplodedEdge", PyExplodedEdgeFilter::root())?;
    filter_module.add("Graph", PyGraphFilter::root())?;

    Ok(filter_module)
}
