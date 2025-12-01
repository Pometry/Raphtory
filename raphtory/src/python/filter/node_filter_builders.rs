use crate::{
    db::graph::views::filter::model::{
        node_filter::{
            NodeFilter, NodeFilterBuilderOps, NodeIdFilterBuilder, NodeIdFilterBuilderOps,
            NodeNameFilterBuilder, NodeTypeFilterBuilder,
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

#[pyclass(frozen, name = "NodeIdFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyIdNodeFilterBuilder(Arc<NodeIdFilterBuilder>);

#[pymethods]
impl PyIdNodeFilterBuilder {
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

#[derive(Clone)]
enum NodeTextBuilder {
    Name(NodeNameFilterBuilder),
    Type(NodeTypeFilterBuilder),
}

#[pyclass(frozen, name = "NodeFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilterOp(NodeTextBuilder);

impl From<NodeNameFilterBuilder> for PyNodeFilterOp {
    fn from(v: NodeNameFilterBuilder) -> Self {
        PyNodeFilterOp(NodeTextBuilder::Name(v))
    }
}

impl From<NodeTypeFilterBuilder> for PyNodeFilterOp {
    fn from(v: NodeTypeFilterBuilder) -> Self {
        PyNodeFilterOp(NodeTextBuilder::Type(v))
    }
}

impl PyNodeFilterOp {
    #[inline]
    fn map<T>(
        &self,
        f_name: impl FnOnce(&NodeNameFilterBuilder) -> T,
        f_type: impl FnOnce(&NodeTypeFilterBuilder) -> T,
    ) -> T {
        match &self.0 {
            NodeTextBuilder::Name(n) => f_name(n),
            NodeTextBuilder::Type(t) => f_type(t),
        }
    }
}

#[pymethods]
impl PyNodeFilterOp {
    /// Returns a filter expression that checks if a given value is equal to a specified string.
    ///
    /// Arguments:
    ///     value (str):
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn __eq__(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(NodeFilterBuilderOps::eq(n, value.clone()))),
            |t| PyFilterExpr(Arc::new(NodeFilterBuilderOps::eq(t, value.clone()))),
        )
    }

    /// Returns a filter expression that checks if a given value is not equal to a specified string.
    ///
    /// Arguments:
    ///     value (str):
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn __ne__(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(NodeFilterBuilderOps::ne(n, value.clone()))),
            |t| PyFilterExpr(Arc::new(NodeFilterBuilderOps::ne(t, value.clone()))),
        )
    }

    /// Returns a filter expression that checks if a specified value is contained within a given iterable of strings.
    ///
    /// Arguments:
    ///     values (list[str]):
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let vals: Vec<String> = values.into_iter().collect();
        self.map(
            |n| PyFilterExpr(Arc::new(NodeFilterBuilderOps::is_in(n, vals.clone()))),
            |t| PyFilterExpr(Arc::new(NodeFilterBuilderOps::is_in(t, vals.clone()))),
        )
    }

    /// Returns a filter expression that checks if specified value is not contained within a given iterable of strings.
    ///
    /// Arguments:
    ///     values (list[str]):
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let vals: Vec<String> = values.into_iter().collect();
        self.map(
            |n| PyFilterExpr(Arc::new(NodeFilterBuilderOps::is_not_in(n, vals.clone()))),
            |t| PyFilterExpr(Arc::new(NodeFilterBuilderOps::is_not_in(t, vals.clone()))),
        )
    }

    fn starts_with(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| {
                PyFilterExpr(Arc::new(NodeFilterBuilderOps::starts_with(
                    n,
                    value.clone(),
                )))
            },
            |t| {
                PyFilterExpr(Arc::new(NodeFilterBuilderOps::starts_with(
                    t,
                    value.clone(),
                )))
            },
        )
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(NodeFilterBuilderOps::ends_with(n, value.clone()))),
            |t| PyFilterExpr(Arc::new(NodeFilterBuilderOps::ends_with(t, value.clone()))),
        )
    }

    /// Returns a filter expression that checks if the specified iterable of strings contains a given value.
    ///
    /// Arguments:
    ///     value (str):
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn contains(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(NodeFilterBuilderOps::contains(n, value.clone()))),
            |t| PyFilterExpr(Arc::new(NodeFilterBuilderOps::contains(t, value.clone()))),
        )
    }

    /// Returns a filter expression that checks if the specified iterable of strings does not contain a given value.
    ///
    ///
    /// Arguments:
    ///     value (str):
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn not_contains(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| {
                PyFilterExpr(Arc::new(NodeFilterBuilderOps::not_contains(
                    n,
                    value.clone(),
                )))
            },
            |t| {
                PyFilterExpr(Arc::new(NodeFilterBuilderOps::not_contains(
                    t,
                    value.clone(),
                )))
            },
        )
    }

    /// Returns a filter expression that checks if the specified properties approximately match the specified string.
    ///
    /// Uses a specified Levenshtein distance and optional prefix matching.
    ///
    /// Arguments:
    ///     prop_value (str): Property to match against.
    ///     levenshtein_distance (int): Maximum levenshtein distance between the specified prop_value and the result.
    ///     prefix_match (bool): Enable prefix matching.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        self.map(
            |n| {
                PyFilterExpr(Arc::new(NodeFilterBuilderOps::fuzzy_search(
                    n,
                    value.clone(),
                    levenshtein_distance,
                    prefix_match,
                )))
            },
            |t| {
                PyFilterExpr(Arc::new(NodeFilterBuilderOps::fuzzy_search(
                    t,
                    value.clone(),
                    levenshtein_distance,
                    prefix_match,
                )))
            },
        )
    }
}

#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilter;

#[pymethods]
impl PyNodeFilter {
    #[staticmethod]
    fn id() -> PyIdNodeFilterBuilder {
        PyIdNodeFilterBuilder(Arc::new(NodeFilter::id()))
    }

    #[staticmethod]
    fn name() -> PyNodeFilterOp {
        PyNodeFilterOp::from(NodeFilter::name())
    }

    #[staticmethod]
    fn node_type() -> PyNodeFilterOp {
        PyNodeFilterOp::from(NodeFilter::node_type())
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
