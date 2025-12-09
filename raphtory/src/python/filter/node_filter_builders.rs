use crate::{
    db::graph::views::filter::model::{
        node_filter::{
            builders::{NodeIdFilterBuilder, NodeNameFilterBuilder, NodeTypeFilterBuilder},
            ops::{NodeFilterOps, NodeIdFilterOps},
            NodeFilter,
        },
        property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
        PropertyFilterFactory,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            property_filter_builders::{
                PyFilterOps, PyPropertyFilterBuilder, PyPropertyFilterFactory,
            },
        },
        types::iterable::FromIterable,
        utils::PyTime,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyResult, Python};
use raphtory_api::core::entities::GID;
use std::sync::Arc;

#[pyclass(frozen, name = "NodeIdFilterBuilder", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeIdFilterBuilder(Arc<NodeIdFilterBuilder>);

#[pymethods]
impl PyNodeIdFilterBuilder {
    fn __eq__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.eq(value)))
    }

    fn __ne__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ne(value)))
    }

    fn __lt__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.lt(value)))
    }

    fn __le__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.le(value)))
    }

    fn __gt__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.gt(value)))
    }

    fn __ge__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ge(value)))
    }

    fn is_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.is_in(values)))
    }

    fn is_not_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.is_not_in(values)))
    }

    fn starts_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.starts_with(value)))
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ends_with(value)))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.contains(value)))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.not_contains(value)))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.fuzzy_search(
            value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

#[pyclass(frozen, name = "NodeNameFilterBuilder", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeNameFilterBuilder(Arc<NodeNameFilterBuilder>);

#[pyclass(frozen, name = "NodeTypeFilterBuilder", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeTypeFilterBuilder(Arc<NodeTypeFilterBuilder>);

macro_rules! impl_node_text_filter_builder {
    ($py_ty:ident) => {
        #[pymethods]
        impl $py_ty {
            fn __eq__(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.eq(value)))
            }

            fn __ne__(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.ne(value)))
            }

            fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
                let vals: Vec<String> = values.into_iter().collect();
                PyFilterExpr(Arc::new(self.0.is_in(vals)))
            }

            fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
                let vals: Vec<String> = values.into_iter().collect();
                PyFilterExpr(Arc::new(self.0.is_not_in(vals)))
            }

            fn starts_with(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.starts_with(value)))
            }

            fn ends_with(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.ends_with(value)))
            }

            fn contains(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.contains(value)))
            }

            fn not_contains(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.not_contains(value)))
            }

            fn fuzzy_search(
                &self,
                value: String,
                levenshtein_distance: usize,
                prefix_match: bool,
            ) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.fuzzy_search(
                    value,
                    levenshtein_distance,
                    prefix_match,
                )))
            }
        }
    };
}

impl_node_text_filter_builder!(PyNodeNameFilterBuilder);
impl_node_text_filter_builder!(PyNodeTypeFilterBuilder);

#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilter;

#[pymethods]
impl PyNodeFilter {
    #[staticmethod]
    fn id() -> PyNodeIdFilterBuilder {
        PyNodeIdFilterBuilder(Arc::new(NodeFilter::id()))
    }

    #[staticmethod]
    fn name() -> PyNodeNameFilterBuilder {
        PyNodeNameFilterBuilder(Arc::new(NodeFilter::name()))
    }

    #[staticmethod]
    fn node_type() -> PyNodeTypeFilterBuilder {
        PyNodeTypeFilterBuilder(Arc::new(NodeFilter::node_type()))
    }

    #[staticmethod]
    fn property<'py>(
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b: PropertyFilterBuilder<NodeFilter> =
            PropertyFilterFactory::property(&NodeFilter, name);
        b.into_pyobject(py)
    }

    #[staticmethod]
    fn metadata<'py>(py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b: MetadataFilterBuilder<NodeFilter> =
            PropertyFilterFactory::metadata(&NodeFilter, name);
        b.into_pyobject(py)
    }

    #[staticmethod]
    fn window(start: PyTime, end: PyTime) -> PyPropertyFilterFactory {
        PyPropertyFilterFactory::wrap(NodeFilter::window(start, end))
    }
}
