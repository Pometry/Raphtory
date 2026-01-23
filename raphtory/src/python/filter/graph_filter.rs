use crate::{
    db::graph::views::filter::model::{graph_filter::GraphFilter, ViewWrapOps},
    python::{
        filter::property_filter_builders::PyViewFilterBuilder, types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;

#[pyclass(frozen, name = "Graph", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyGraphFilter;

#[pymethods]
impl PyGraphFilter {
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.window(start, end)))
    }

    #[staticmethod]
    fn at(time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.at(time)))
    }

    #[staticmethod]
    fn after(time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.after(time)))
    }

    #[staticmethod]
    fn before(time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.before(time)))
    }

    #[staticmethod]
    fn latest() -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.latest()))
    }

    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.snapshot_at(time)))
    }

    #[staticmethod]
    fn snapshot_latest() -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.snapshot_latest()))
    }

    #[staticmethod]
    fn layer(layer: String) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.layer(layer)))
    }

    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(GraphFilter.layer(layers)))
    }
}
