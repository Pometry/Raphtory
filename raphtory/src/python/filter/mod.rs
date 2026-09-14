use crate::python::{
    filter::{
        edge_expr::{PyEdge, PyEdgeEndpoint, PyEdgeFilter},
        exploded_edge_expr::{PyExplodedEdge, PyExplodedEdgeFilter},
        filter_expr::PyFilterExpr,
        graph_filter::{PyGraph, PyGraphFilter},
        node_expr::{PyExpr, PyNode, PyNodeFilter, PyPropertyExpr},
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
    filter_module.add_class::<PyExpr>()?;
    filter_module.add_class::<PyPropertyExpr>()?;

    filter_module.add_class::<PyNode>()?;
    filter_module.add_class::<PyNodeFilter>()?;

    filter_module.add_class::<PyEdge>()?;
    filter_module.add_class::<PyEdgeFilter>()?;
    filter_module.add_class::<PyEdgeEndpoint>()?;

    filter_module.add_class::<PyExplodedEdge>()?;
    filter_module.add_class::<PyExplodedEdgeFilter>()?;

    filter_module.add_class::<PyGraph>()?;
    filter_module.add_class::<PyGraphFilter>()?;

    Ok(filter_module)
}
