use crate::{
    db::graph::views::filter::model::{
        exploded_edge_filter::{ExplodedEdgeEndpoint, ExplodedEdgeFilter, ExplodedEndpointWrapper},
        node_filter::{
            NodeFilter, NodeIdFilterBuilder, NodeNameFilterBuilder, NodeTypeFilterBuilder,
        },
        property_filter::{MetadataFilterBuilder, PropertyFilterBuilder},
        PropertyFilterFactory, Windowed,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            property_filter_builders::{PyFilterOps, PyPropertyFilterBuilder},
            window_filter::PyExplodedEdgeWindow,
        },
        types::iterable::FromIterable,
        utils::PyTime,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyResult, Python};
use raphtory_api::core::entities::GID;
use std::sync::Arc;

#[pyclass(frozen, name = "ExplodedEdgeIdFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeIdFilterOp(pub ExplodedEndpointWrapper<NodeIdFilterBuilder>);

#[pymethods]
impl PyExplodedEdgeIdFilterOp {
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

#[pyclass(frozen, name = "ExplodedEdgeFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeFilterOp(ExplodedEdgeTextBuilder);

#[derive(Clone)]
enum ExplodedEdgeTextBuilder {
    Name(ExplodedEndpointWrapper<NodeNameFilterBuilder>),
    Type(ExplodedEndpointWrapper<NodeTypeFilterBuilder>),
}

impl From<ExplodedEndpointWrapper<NodeNameFilterBuilder>> for PyExplodedEdgeFilterOp {
    fn from(v: ExplodedEndpointWrapper<NodeNameFilterBuilder>) -> Self {
        PyExplodedEdgeFilterOp(ExplodedEdgeTextBuilder::Name(v))
    }
}

impl From<ExplodedEndpointWrapper<NodeTypeFilterBuilder>> for PyExplodedEdgeFilterOp {
    fn from(v: ExplodedEndpointWrapper<NodeTypeFilterBuilder>) -> Self {
        PyExplodedEdgeFilterOp(ExplodedEdgeTextBuilder::Type(v))
    }
}

impl PyExplodedEdgeFilterOp {
    #[inline]
    fn map<T>(
        &self,
        f_name: impl FnOnce(&ExplodedEndpointWrapper<NodeNameFilterBuilder>) -> T,
        f_type: impl FnOnce(&ExplodedEndpointWrapper<NodeTypeFilterBuilder>) -> T,
    ) -> T {
        match &self.0 {
            ExplodedEdgeTextBuilder::Name(n) => f_name(n),
            ExplodedEdgeTextBuilder::Type(t) => f_type(t),
        }
    }
}

#[pymethods]
impl PyExplodedEdgeFilterOp {
    fn __eq__(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.eq(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.eq(value.clone()))),
        )
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.ne(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.ne(value.clone()))),
        )
    }

    fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let vals: Vec<String> = values.into_iter().collect();
        self.map(
            |n| PyFilterExpr(Arc::new(n.is_in(vals.clone()))),
            |t| PyFilterExpr(Arc::new(t.is_in(vals.clone()))),
        )
    }

    fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let vals: Vec<String> = values.into_iter().collect();
        self.map(
            |n| PyFilterExpr(Arc::new(n.is_not_in(vals.clone()))),
            |t| PyFilterExpr(Arc::new(t.is_not_in(vals.clone()))),
        )
    }

    fn starts_with(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.starts_with(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.starts_with(value.clone()))),
        )
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.ends_with(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.ends_with(value.clone()))),
        )
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.contains(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.contains(value.clone()))),
        )
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.not_contains(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.not_contains(value.clone()))),
        )
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        self.map(
            |n| {
                PyFilterExpr(Arc::new(n.fuzzy_search(
                    value.clone(),
                    levenshtein_distance,
                    prefix_match,
                )))
            },
            |t| {
                PyFilterExpr(Arc::new(t.fuzzy_search(
                    value.clone(),
                    levenshtein_distance,
                    prefix_match,
                )))
            },
        )
    }
}

#[pyclass(frozen, name = "ExplodedEdgeEndpoint", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeEndpoint(pub ExplodedEdgeEndpoint);

#[pymethods]
impl PyExplodedEdgeEndpoint {
    fn id(&self) -> PyExplodedEdgeIdFilterOp {
        PyExplodedEdgeIdFilterOp(self.0.id())
    }

    fn name(&self) -> PyExplodedEdgeFilterOp {
        PyExplodedEdgeFilterOp::from(self.0.name())
    }

    fn node_type(&self) -> PyExplodedEdgeFilterOp {
        PyExplodedEdgeFilterOp::from(self.0.node_type())
    }

    fn property<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b: PropertyFilterBuilder<ExplodedEndpointWrapper<NodeFilter>> = self.0.property(name);
        b.into_pyobject(py)
    }

    fn metadata<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b: MetadataFilterBuilder<ExplodedEndpointWrapper<NodeFilter>> = self.0.metadata(name);
        b.into_pyobject(py)
    }

    fn window(&self, py_start: PyTime, py_end: PyTime) -> PyResult<PyExplodedEdgeEndpointWindow> {
        Ok(PyExplodedEdgeEndpointWindow(
            self.0.window(py_start, py_end),
        ))
    }
}

#[pyclass(frozen, name = "ExplodedEdge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeFilter;

#[pymethods]
impl PyExplodedEdgeFilter {
    #[staticmethod]
    fn src() -> PyExplodedEdgeEndpoint {
        PyExplodedEdgeEndpoint(ExplodedEdgeEndpoint::src())
    }

    #[staticmethod]
    fn dst() -> PyExplodedEdgeEndpoint {
        PyExplodedEdgeEndpoint(ExplodedEdgeEndpoint::dst())
    }

    #[staticmethod]
    fn property<'py>(
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b: PropertyFilterBuilder<ExplodedEdgeFilter> =
            PropertyFilterFactory::property(&ExplodedEdgeFilter, name);
        b.into_pyobject(py)
    }

    #[staticmethod]
    fn metadata<'py>(py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b: MetadataFilterBuilder<ExplodedEdgeFilter> =
            PropertyFilterFactory::metadata(&ExplodedEdgeFilter, name);
        b.into_pyobject(py)
    }

    #[staticmethod]
    fn window(start: PyTime, end: PyTime) -> PyResult<PyExplodedEdgeWindow> {
        Ok(PyExplodedEdgeWindow(Windowed::from_times(
            start,
            end,
            ExplodedEdgeFilter,
        )))
    }
}

#[pyclass(
    frozen,
    name = "ExplodedEdgeEndpointWindow",
    module = "raphtory.filter"
)]
#[derive(Clone)]
pub struct PyExplodedEdgeEndpointWindow(pub ExplodedEndpointWrapper<Windowed<NodeFilter>>);

#[pymethods]
impl PyExplodedEdgeEndpointWindow {
    fn property<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b: PropertyFilterBuilder<ExplodedEndpointWrapper<Windowed<NodeFilter>>> =
            self.0.property(name);
        b.into_pyobject(py)
    }

    fn metadata<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b: MetadataFilterBuilder<ExplodedEndpointWrapper<Windowed<NodeFilter>>> =
            self.0.metadata(name);
        b.into_pyobject(py)
    }
}
