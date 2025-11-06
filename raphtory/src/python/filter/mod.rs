use crate::python::filter::{
    edge_filter_builders::{PyEdgeEndpoint, PyEdgeFilter, PyEdgeFilterOp, PyExplodedEdgeFilter},
    filter_expr::PyFilterExpr,
    node_filter_builders::PyNodeFilter,
    property_filter_builders::{PyFilterOps, PyPropertyFilterBuilder},
    window_filter::{PyEdgeWindow, PyExplodedEdgeWindow, PyNodeWindow},
};
use pyo3::{
    prelude::{PyModule, PyModuleMethods},
    Bound, PyErr, Python,
};

mod create_filter;
pub mod edge_filter_builders;
pub mod filter_expr;
pub mod node_filter_builders;
pub mod property_filter_builders;
pub mod window_filter;

pub fn base_filter_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let filter_module = PyModule::new(py, "filter")?;
    filter_module.add_class::<PyFilterExpr>()?;
    filter_module.add_class::<PyPropertyFilterBuilder>()?;
    filter_module.add_class::<PyNodeFilter>()?;
    filter_module.add_class::<PyEdgeFilterOp>()?;
    filter_module.add_class::<PyEdgeEndpoint>()?;
    filter_module.add_class::<PyEdgeFilter>()?;
    filter_module.add_class::<PyExplodedEdgeFilter>()?;
    filter_module.add_class::<PyFilterOps>()?;
    filter_module.add_class::<PyPropertyFilterBuilder>()?;
    filter_module.add_class::<PyNodeWindow>()?;
    filter_module.add_class::<PyEdgeWindow>()?;
    filter_module.add_class::<PyExplodedEdgeWindow>()?;

    Ok(filter_module)
}
