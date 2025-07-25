use crate::python::filter::{
    edge_filter_builders::{PyEdgeEndpoint, PyEdgeFilter, PyEdgeFilterOp},
    filter_expr::PyFilterExpr,
    node_filter_builders::{PyNodeFilter, PyNodeFilterBuilder},
    property_filter_builders::{PyPropertyFilterBuilder, PyTemporalPropertyFilterBuilder},
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

pub fn base_filter_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let filter_module = PyModule::new(py, "filter")?;
    filter_module.add_class::<PyFilterExpr>()?;
    filter_module.add_class::<PyNodeFilterBuilder>()?;
    filter_module.add_class::<PyNodeFilter>()?;
    filter_module.add_class::<PyEdgeFilterOp>()?;
    filter_module.add_class::<PyEdgeEndpoint>()?;
    filter_module.add_class::<PyEdgeFilter>()?;
    filter_module.add_class::<PyPropertyFilterBuilder>()?;
    filter_module.add_class::<PyTemporalPropertyFilterBuilder>()?;
    filter_module.add_class::<PyPropertyFilterBuilder>()?;

    Ok(filter_module)
}
