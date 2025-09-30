use crate::{
    db::graph::views::filter::model::{
        InternalNodeFilterBuilderOps, NodeFilter, NodeFilterBuilderOps,
    },
    python::{
        filter::filter_expr::{PyFilterExpr, PyInnerFilterExpr},
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods};
use std::sync::Arc;

/// A builder for constructing node filters
///
/// To create a filter builder see [Node][raphtory.filter.Node].
#[pyclass(frozen, name = "NodeFilterBuilder", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilterBuilder(Arc<dyn DynNodeFilterBuilderOps>);

impl<T: InternalNodeFilterBuilderOps + 'static> From<T> for PyNodeFilterBuilder {
    fn from(value: T) -> Self {
        PyNodeFilterBuilder(Arc::new(value))
    }
}

#[pymethods]
/// Implements various filter builder methods for node filtering.
impl PyNodeFilterBuilder {
    /// Returns a filter expression that checks if a given value is equal to a specified string.
    ///
    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     filter expression
    fn __eq__(&self, value: String) -> PyFilterExpr {
        self.0.eq(value)
    }

    /// Returns a filter expression that checks if a given value is not equal to a specified string.
    ///
    ///  
    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     filter expression
    fn __ne__(&self, value: String) -> PyFilterExpr {
        self.0.ne(value)
    }

    /// Returns a filter expression that checks if a specified value is contained within a given iterable of strings.
    ///
    /// Arguments:
    ///     list[str]:
    ///
    /// Returns:
    ///     filter expression
    fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        self.0.is_in(values.into())
    }

    /// Returns a filter expression that checks if specified value is not contained within a given iterable of strings.
    ///
    /// Arguments:
    ///     list[str]:
    ///
    /// Returns:
    ///     filter expression
    fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        self.0.is_not_in(values.into())
    }

    /// Returns a filter expression that checks if the specified iterable of strings contains a given value.
    ///
    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     filter expression
    fn contains(&self, value: String) -> PyFilterExpr {
        self.0.contains(value)
    }

    /// Returns a filter expression that checks if the specified iterable of strings does not contain a given value.
    ///
    ///  
    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     filter expression
    fn not_contains(&self, value: String) -> PyFilterExpr {
        self.0.not_contains(value)
    }

    /// Returns a filter expression that checks if the specified properties approximately match the specified string.
    ///
    /// Uses a specified Levenshtein distance and optional prefix matching.
    ///
    /// Arguments:
    ///     str:
    ///     levenshtein_distance:
    ///     prefix_match:
    ///
    /// Returns:
    ///     filter expression
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
}

pub trait DynNodeFilterBuilderOps: Send + Sync {
    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     FilterExpr:
    fn eq(&self, value: String) -> PyFilterExpr;

    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     FilterExpr:
    fn ne(&self, value: String) -> PyFilterExpr;

    /// Arguments:
    ///     list[str]:
    ///
    /// Returns:
    ///     FilterExpr:
    fn is_in(&self, values: Vec<String>) -> PyFilterExpr;

    /// Arguments:
    ///     list[str]:
    ///
    /// Returns:
    ///     FilterExpr:
    fn is_not_in(&self, values: Vec<String>) -> PyFilterExpr;

    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     FilterExpr:
    fn contains(&self, value: String) -> PyFilterExpr;

    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     FilterExpr:
    fn not_contains(&self, value: String) -> PyFilterExpr;

    /// Returns a filter expression that checks if the specified properties approximately match the specified string.
    ///
    /// Uses a specified Levenshtein distance and optional prefix matching.
    ///
    /// Arguments:
    ///     prop_value (str):
    ///     levenshtein_distance (usize):
    ///     prefix_match (bool):
    ///  
    /// Returns:
    ///     FilterExpr:
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
    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     FilterExpr:
    fn eq(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(NodeFilterBuilderOps::eq(
            self, value,
        ))))
    }

    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     FilterExpr:
    fn ne(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(NodeFilterBuilderOps::ne(
            self, value,
        ))))
    }

    /// Arguments:
    ///     values:
    ///
    /// Returns:
    ///     FilterExpr:
    fn is_in(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
            NodeFilterBuilderOps::is_in(self, values),
        )))
    }

    /// Arguments:
    ///     values:
    ///
    /// Returns:
    ///     FilterExpr:
    fn is_not_in(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
            NodeFilterBuilderOps::is_not_in(self, values),
        )))
    }

    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     FilterExpr:
    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
            NodeFilterBuilderOps::contains(self, value),
        )))
    }

    /// Arguments:
    ///     str:
    ///
    /// Returns:
    ///     FilterExpr:
    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
            NodeFilterBuilderOps::not_contains(self, value),
        )))
    }

    /// Returns a filter expression that checks if the specified properties approximately match the specified string.
    ///
    /// Uses a specified Levenshtein distance and optional prefix matching.
    ///
    /// Arguments:
    ///     prop_value (str):
    ///     levenshtein_distance (usize):
    ///     prefix_match (bool):
    ///  
    /// Returns:
    ///     FilterExpr:
    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
            NodeFilterBuilderOps::fuzzy_search(self, value, levenshtein_distance, prefix_match),
        )))
    }
}
