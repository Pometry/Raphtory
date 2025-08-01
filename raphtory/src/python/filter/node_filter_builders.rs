use crate::{
    db::graph::views::filter::model::{
        node_filter::{InternalNodeFilterBuilderOps, NodeFilter, NodeFilterBuilderOps},
        property_filter::{MetadataFilterBuilder, PropertyFilterBuilder},
        PropertyFilterFactory,
    },
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods};
use std::sync::Arc;

#[pyclass(frozen, name = "NodeFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilterBuilder(Arc<dyn DynNodeFilterBuilderOps>);

impl<T: InternalNodeFilterBuilderOps + 'static> From<T> for PyNodeFilterBuilder {
    fn from(value: T) -> Self {
        PyNodeFilterBuilder(Arc::new(value))
    }
}

#[pymethods]
impl PyNodeFilterBuilder {
    fn __eq__(&self, value: String) -> PyFilterExpr {
        self.0.eq(value)
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        self.0.ne(value)
    }

    fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        self.0.is_in(values.into())
    }

    fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        self.0.is_not_in(values.into())
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        self.0.contains(value)
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        self.0.not_contains(value)
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        self.0
            .fuzzy_search(value, levenshtein_distance, prefix_match)
    }
}

#[derive(Clone)]
#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
pub struct PyNodeFilter;

#[pymethods]
impl PyNodeFilter {
    /// Filter node by name
    ///
    /// Returns:
    ///     NodeFilterBuilder: A filter builder for filtering by node name
    #[staticmethod]
    fn name() -> PyNodeFilterBuilder {
        PyNodeFilterBuilder(Arc::new(NodeFilter::name()))
    }

    /// Filter node by type
    ///
    /// Returns:
    ///     NodeFilterBuilder: A filter builder for filtering by node type
    #[staticmethod]
    fn node_type() -> PyNodeFilterBuilder {
        PyNodeFilterBuilder(Arc::new(NodeFilter::node_type()))
    }

    #[staticmethod]
    fn property(name: String) -> PropertyFilterBuilder<NodeFilter> {
        NodeFilter::property(name)
    }

    #[staticmethod]
    fn metadata(name: String) -> MetadataFilterBuilder<NodeFilter> {
        NodeFilter::metadata(name)
    }
}

pub trait DynNodeFilterBuilderOps: Send + Sync {
    fn eq(&self, value: String) -> PyFilterExpr;

    fn ne(&self, value: String) -> PyFilterExpr;

    fn is_in(&self, values: Vec<String>) -> PyFilterExpr;

    fn is_not_in(&self, values: Vec<String>) -> PyFilterExpr;

    fn contains(&self, value: String) -> PyFilterExpr;

    fn not_contains(&self, value: String) -> PyFilterExpr;

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr;
}

impl<T> DynNodeFilterBuilderOps for T
where
    T: InternalNodeFilterBuilderOps,
{
    fn eq(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeFilterBuilderOps::eq(self, value)))
    }

    fn ne(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeFilterBuilderOps::ne(self, value)))
    }

    fn is_in(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeFilterBuilderOps::is_in(self, values)))
    }

    fn is_not_in(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeFilterBuilderOps::is_not_in(self, values)))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeFilterBuilderOps::contains(self, value)))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeFilterBuilderOps::not_contains(self, value)))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeFilterBuilderOps::fuzzy_search(
            self,
            value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}
