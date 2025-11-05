use crate::{
    db::graph::views::filter::model::{
        edge_filter::EndpointWrapper,
        node_filter::{
            InternalNodeFilterBuilderOps, InternalNodeIdFilterBuilderOps, NodeFilter,
            NodeFilterBuilderOps, NodeIdFilterBuilder, NodeIdFilterBuilderOps,
            NodeNameFilterBuilder, NodeTypeFilterBuilder,
        },
        property_filter::{MetadataFilterBuilder, PropertyFilterBuilder},
        PropertyFilterFactory, TryAsCompositeFilter, Windowed,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            window_filter::{py_into_millis, PyNodeWindow},
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{exceptions::PyTypeError, pyclass, pymethods, Bound, PyAny, PyResult};
use raphtory_api::core::entities::GID;
use std::sync::Arc;

#[pyclass(frozen, name = "NodeFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilterBuilder(Arc<dyn DynNodeFilterBuilderOps>);

impl<T: InternalNodeFilterBuilderOps + 'static> From<T> for PyNodeFilterBuilder {
    fn from(value: T) -> Self {
        PyNodeFilterBuilder(Arc::new(value))
    }
}

impl From<EndpointWrapper<NodeNameFilterBuilder>> for PyNodeFilterBuilder {
    fn from(value: EndpointWrapper<NodeNameFilterBuilder>) -> Self {
        PyNodeFilterBuilder(Arc::new(value))
    }
}

impl From<EndpointWrapper<NodeTypeFilterBuilder>> for PyNodeFilterBuilder {
    fn from(value: EndpointWrapper<NodeTypeFilterBuilder>) -> Self {
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

    fn starts_with(&self, value: String) -> PyFilterExpr {
        self.0.starts_with(value)
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        self.0.ends_with(value)
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

#[pyclass(frozen, name = "NodeIdFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeIdFilterBuilder(Arc<dyn DynNodeIdFilterBuilderOps>);

impl<T: InternalNodeIdFilterBuilderOps + 'static> From<T> for PyNodeIdFilterBuilder {
    fn from(value: T) -> Self {
        PyNodeIdFilterBuilder(Arc::new(value))
    }
}

impl From<EndpointWrapper<NodeIdFilterBuilder>> for PyNodeIdFilterBuilder {
    fn from(value: EndpointWrapper<NodeIdFilterBuilder>) -> Self {
        PyNodeIdFilterBuilder(Arc::new(value))
    }
}

#[pymethods]
impl PyNodeIdFilterBuilder {
    fn __eq__(&self, value: GID) -> PyFilterExpr {
        self.0.eq(value)
    }

    fn __ne__(&self, value: GID) -> PyFilterExpr {
        self.0.ne(value)
    }

    fn __lt__(&self, value: GID) -> PyFilterExpr {
        self.0.lt(value)
    }

    fn __le__(&self, value: GID) -> PyFilterExpr {
        self.0.le(value)
    }

    fn __gt__(&self, value: GID) -> PyFilterExpr {
        self.0.gt(value)
    }

    fn __ge__(&self, value: GID) -> PyFilterExpr {
        self.0.ge(value)
    }

    fn is_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        self.0.is_in(values)
    }

    fn is_not_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        self.0.is_not_in(values)
    }

    fn starts_with(&self, value: String) -> PyFilterExpr {
        self.0.starts_with(value)
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        self.0.ends_with(value)
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
    /// Filter node by id
    ///
    /// Returns:
    ///     NodeFilterBuilder: A filter builder for filtering by node id
    #[staticmethod]
    fn id() -> PyNodeIdFilterBuilder {
        PyNodeIdFilterBuilder(Arc::new(NodeFilter::id()))
    }

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

    #[staticmethod]
    fn window(py_start: Bound<PyAny>, py_end: Bound<PyAny>) -> PyResult<PyNodeWindow> {
        let s = py_into_millis(&py_start)?;
        let e = py_into_millis(&py_end)?;
        if s > e {
            return Err(PyTypeError::new_err("window.start must be <= window.end"));
        }
        Ok(PyNodeWindow(Arc::new(Windowed::<NodeFilter>::from_times(
            s, e,
        ))))
    }
}

pub trait DynNodeFilterBuilderOps: Send + Sync {
    fn eq(&self, value: String) -> PyFilterExpr;

    fn ne(&self, value: String) -> PyFilterExpr;

    fn is_in(&self, values: Vec<String>) -> PyFilterExpr;

    fn is_not_in(&self, values: Vec<String>) -> PyFilterExpr;

    fn starts_with(&self, value: String) -> PyFilterExpr;

    fn ends_with(&self, value: String) -> PyFilterExpr;

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

    fn starts_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeFilterBuilderOps::starts_with(self, value)))
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeFilterBuilderOps::ends_with(self, value)))
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

impl<T> DynNodeFilterBuilderOps for EndpointWrapper<T>
where
    T: InternalNodeFilterBuilderOps + Clone + 'static,
    // We will produce EndpointWrapper<T::NodeFilterType>
    T::NodeFilterType: TryAsCompositeFilter + Clone + 'static,
{
    fn eq(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeFilterBuilderOps::eq(&self.inner, value)),
        ))
    }

    fn ne(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeFilterBuilderOps::ne(&self.inner, value)),
        ))
    }

    fn is_in(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeFilterBuilderOps::is_in(&self.inner, values)),
        ))
    }

    fn is_not_in(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeFilterBuilderOps::is_not_in(&self.inner, values)),
        ))
    }

    fn starts_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeFilterBuilderOps::starts_with(&self.inner, value)),
        ))
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeFilterBuilderOps::ends_with(&self.inner, value)),
        ))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeFilterBuilderOps::contains(&self.inner, value)),
        ))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeFilterBuilderOps::not_contains(&self.inner, value)),
        ))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.with(NodeFilterBuilderOps::fuzzy_search(
            &self.inner,
            value,
            levenshtein_distance,
            prefix_match,
        ))))
    }
}

pub trait DynNodeIdFilterBuilderOps: Send + Sync {
    fn eq(&self, value: GID) -> PyFilterExpr;

    fn ne(&self, value: GID) -> PyFilterExpr;

    fn lt(&self, value: GID) -> PyFilterExpr;

    fn le(&self, value: GID) -> PyFilterExpr;

    fn gt(&self, value: GID) -> PyFilterExpr;

    fn ge(&self, value: GID) -> PyFilterExpr;

    fn is_in(&self, values: FromIterable<GID>) -> PyFilterExpr;

    fn is_not_in(&self, values: FromIterable<GID>) -> PyFilterExpr;

    fn starts_with(&self, value: String) -> PyFilterExpr;

    fn ends_with(&self, value: String) -> PyFilterExpr;

    fn contains(&self, value: String) -> PyFilterExpr;

    fn not_contains(&self, value: String) -> PyFilterExpr;

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr;
}

impl<T> DynNodeIdFilterBuilderOps for T
where
    T: InternalNodeIdFilterBuilderOps,
{
    fn eq(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::eq(self, value)))
    }

    fn ne(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::ne(self, value)))
    }

    fn lt(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::lt(self, value)))
    }

    fn le(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::le(self, value)))
    }

    fn gt(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::gt(self, value)))
    }

    fn ge(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::ge(self, value)))
    }

    fn is_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::is_in(self, values)))
    }

    fn is_not_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::is_not_in(self, values)))
    }

    fn starts_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::starts_with(self, value)))
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::ends_with(self, value)))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::contains(self, value)))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::not_contains(self, value)))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeIdFilterBuilderOps::fuzzy_search(
            self,
            value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

impl<T> DynNodeIdFilterBuilderOps for EndpointWrapper<T>
where
    T: InternalNodeIdFilterBuilderOps + Clone + 'static,
    // We will produce EndpointWrapper<T::NodeIdFilterType>
    T::NodeIdFilterType: TryAsCompositeFilter + Clone + 'static,
{
    fn eq(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::eq(&self.inner, value)),
        ))
    }

    fn ne(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::ne(&self.inner, value)),
        ))
    }

    fn lt(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::lt(&self.inner, value)),
        ))
    }

    fn le(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::le(&self.inner, value)),
        ))
    }

    fn gt(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::gt(&self.inner, value)),
        ))
    }

    fn ge(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::ge(&self.inner, value)),
        ))
    }

    fn is_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::is_in(&self.inner, values)),
        ))
    }

    fn is_not_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::is_not_in(&self.inner, values)),
        ))
    }

    fn starts_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::starts_with(&self.inner, value)),
        ))
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::ends_with(&self.inner, value)),
        ))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::contains(&self.inner, value)),
        ))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(
            self.with(NodeIdFilterBuilderOps::not_contains(&self.inner, value)),
        ))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.with(NodeIdFilterBuilderOps::fuzzy_search(
            &self.inner,
            value,
            levenshtein_distance,
            prefix_match,
        ))))
    }
}
