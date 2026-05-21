use crate::{
    db::graph::views::filter::model::{
        degree_filter::DegreeFilterFactory,
        node_filter::NodeFilter,
    },
    python::filter::property_filter_builders::PyPropertyExprBuilder,
};
use pyo3::{Bound, IntoPyObject, PyResult, Python};
use raphtory_api::core::Direction;

/// Builds a Python property-style expression builder for node degree filtering.
///
/// This exposes comparison operators (`==`, `<`, `>=`, `is_in`, `is_not_in`, etc.)
/// through the existing `FilterOps` Python interface.
pub fn degree_builder<'py>(
    py: Python<'py>,
    direction: Direction,
) -> PyResult<Bound<'py, PyPropertyExprBuilder>> {
    PyPropertyExprBuilder::wrap(NodeFilter.degree(direction)).into_pyobject(py)
}