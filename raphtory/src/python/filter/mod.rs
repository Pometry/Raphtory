use crate::python::{
    filter::{
        edge_filter_builders::{
            PyEdgeEndpoint, PyEdgeEndpointIdFilterBuilder, PyEdgeEndpointNameFilterBuilder,
            PyEdgeEndpointTypeFilterBuilder, PyEdgeFilter,
        },
        exploded_edge_filter_builder::PyExplodedEdgeFilter,
        filter_expr::PyFilterExpr,
        node_filter_builders::{
            PyNodeFilter, PyNodeIdFilterBuilder, PyNodeNameFilterBuilder, PyNodeTypeFilterBuilder,
        },
        property_filter_builders::{PyPropertyExprBuilder, PyPropertyFilterBuilder},
    },
    types::iterable::FromIterable,
};
use pyo3::{
    prelude::{PyModule, PyModuleMethods},
    Bound, PyErr, Python,
};
use raphtory_api::core::entities::Layer;

pub mod edge_filter_builders;
pub mod exploded_edge_filter_builder;
pub mod filter_expr;
pub mod node_filter_builders;
pub mod property_filter_builders;

impl From<FromIterable<String>> for Layer {
    fn from(iter: FromIterable<String>) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}

pub fn base_filter_module(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
    let filter_module = PyModule::new(py, "filter")?;

    filter_module.add_class::<PyFilterExpr>()?;
    filter_module.add_class::<PyPropertyExprBuilder>()?;
    filter_module.add_class::<PyPropertyFilterBuilder>()?;

    filter_module.add_class::<PyNodeFilter>()?;
    filter_module.add_class::<PyNodeIdFilterBuilder>()?;
    filter_module.add_class::<PyNodeNameFilterBuilder>()?;
    filter_module.add_class::<PyNodeTypeFilterBuilder>()?;

    filter_module.add_class::<PyEdgeFilter>()?;
    filter_module.add_class::<PyEdgeEndpoint>()?;
    filter_module.add_class::<PyEdgeEndpointIdFilterBuilder>()?;
    filter_module.add_class::<PyEdgeEndpointNameFilterBuilder>()?;
    filter_module.add_class::<PyEdgeEndpointTypeFilterBuilder>()?;

    filter_module.add_class::<PyExplodedEdgeFilter>()?;

    Ok(filter_module)
}
