//! The edge module contains the PyEdge class, which is used to represent edges in the graph and
//! provides access to the edge's properties and vertices.
//!
//! The PyEdge class also provides access to the perspective APIs, which allow the user to view the
//! edge as it existed at a particular point in time, or as it existed over a particular time range.
//!
use crate::{
    core::{
        utils::{errors::GraphError, time::error::ParseTimeError},
        ArcStr, Direction,
    },
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{DynamicGraph, Immutable, IntoDynamic, MaterializedGraph, Static},
                BoxedIter, WindowSet,
            },
        },
        graph::{
            edge::EdgeView,
            views::{
                deletion_graph::GraphWithDeletions, layer_graph::LayeredGraph,
                window_graph::WindowedGraph,
            },
        },
    },
    prelude::*,
    python::{
        graph::{
            properties::{PyNestedPropsIterable, PyPropsList},
            vertex::{PyNestedVertexIterable, PyVertex, PyVertexIterable},
        },
        types::{
            repr::{iterator_repr, Repr},
            wrappers::iterators::{
                ArcStringVecIterable, I64VecIterable, NestedArcStringVecIterable,
                NestedI64VecIterable, NestedNaiveDateTimeIterable, NestedOptionArcStringIterable,
                NestedOptionI64Iterable, NestedU64U64Iterable, OptionArcStringIterable,
                OptionI64Iterable, OptionNaiveDateTimeIterable, U64U64Iterable,
            },
        },
        utils::{PyGenericIterator, PyInterval, PyTime},
    },
};
use chrono::NaiveDateTime;
use itertools::Itertools;
use pyo3::{prelude::*, pyclass::CompareOp};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
};

/// PyEdge is a Python class that represents an edge in the graph.
/// An edge is a directed connection between two vertices.
#[pyclass(name = "Edge", subclass)]
pub struct PyEdge {
    pub(crate) edge: EdgeView<DynamicGraph>,
}

#[pyclass(name="MutableEdge", extends=PyEdge)]
pub struct PyMutableEdge {
    edge: EdgeView<MaterializedGraph>,
}

impl<G: GraphViewOps + IntoDynamic> From<EdgeView<G>> for PyEdge {
    fn from(value: EdgeView<G>) -> Self {
        Self {
            edge: EdgeView {
                graph: value.graph.clone().into_dynamic(),
                edge: value.edge,
            },
        }
    }
}

impl<G: GraphViewOps + Static + IntoDynamic> From<EdgeView<G>> for EdgeView<DynamicGraph> {
    fn from(value: EdgeView<G>) -> Self {
        EdgeView {
            graph: value.graph.into_dynamic(),
            edge: value.edge,
        }
    }
}

impl<G: Into<MaterializedGraph> + GraphViewOps> From<EdgeView<G>> for PyMutableEdge {
    fn from(value: EdgeView<G>) -> Self {
        let edge = EdgeView {
            edge: value.edge,
            graph: value.graph.into(),
        };

        Self { edge }
    }
}

impl<G: GraphViewOps + IntoDynamic + Immutable> IntoPy<PyObject> for EdgeView<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let py_version: PyEdge = self.into();
        py_version.into_py(py)
    }
}

impl IntoPy<PyObject> for EdgeView<Graph> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let graph: MaterializedGraph = self.graph.into();
        let edge = self.edge;
        let vertex = EdgeView { graph, edge };
        vertex.into_py(py)
    }
}

impl IntoPy<PyObject> for EdgeView<GraphWithDeletions> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let graph: MaterializedGraph = self.graph.into();
        let edge = self.edge;
        let vertex = EdgeView { graph, edge };
        vertex.into_py(py)
    }
}

impl IntoPy<PyObject> for EdgeView<MaterializedGraph> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, (PyMutableEdge::from(self.clone()), PyEdge::from(self)))
            .unwrap() // I think this only fails if we are out of memory? Seems to be unavoidable!
            .into_py(py)
    }
}

impl IntoPy<PyObject> for ArcStr {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

impl<'source> FromPyObject<'source> for ArcStr {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        ob.extract::<String>().map(|v| v.into())
    }
}

/// PyEdge is a Python class that represents an edge in the graph.
/// An edge is a directed connection between two vertices.
#[pymethods]
impl PyEdge {
    /// Rich Comparison for Vertex objects
    pub fn __richcmp__(&self, other: PyRef<PyEdge>, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        match op {
            CompareOp::Eq => (self.edge.id() == other.id()).into_py(py),
            CompareOp::Ne => (self.edge.id() != other.id()).into_py(py),
            _ => py.NotImplemented(),
        }
    }

    /// Returns the hash of the edge and edge properties.
    ///
    /// Returns:
    ///   A hash of the edge.
    pub fn __hash__(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.edge.id().hash(&mut s);
        s.finish()
    }

    /// The id of the edge.
    #[getter]
    pub fn id(&self) -> (u64, u64) {
        self.edge.id()
    }

    pub fn __getitem__(&self, name: &str) -> Option<Prop> {
        self.edge.properties().get(name)
    }

    /// Returns a list of timestamps of when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///     A list of timestamps.
    ///
    pub fn history(&self) -> Vec<i64> {
        self.edge.history()
    }

    /// Returns a view of the properties of the edge.
    ///
    /// Returns:
    ///   Properties on the Edge.
    #[getter]
    pub fn properties(&self) -> Properties<EdgeView<DynamicGraph>> {
        self.edge.properties()
    }

    /// Get the source vertex of the Edge.
    ///
    /// Returns:
    ///   The source vertex of the Edge.
    #[getter]
    fn src(&self) -> PyVertex {
        self.edge.src().into()
    }

    /// Get the destination vertex of the Edge.
    ///
    /// Returns:
    ///   The destination vertex of the Edge.
    #[getter]
    fn dst(&self) -> PyVertex {
        self.edge.dst().into()
    }

    //******  Perspective APIS  ******//

    /// Get the start time of the Edge.
    ///
    /// Returns:
    ///  The start time of the Edge.
    #[getter]
    pub fn start(&self) -> Option<i64> {
        self.edge.start()
    }

    /// Get the start datetime of the Edge.
    ///
    /// Returns:
    ///     The start datetime of the Edge.
    #[getter]
    pub fn start_date_time(&self) -> Option<NaiveDateTime> {
        let start_time = self.edge.start()?;
        NaiveDateTime::from_timestamp_millis(start_time)
    }

    /// Get the end time of the Edge.
    ///
    /// Returns:
    ///   The end time of the Edge.
    #[getter]
    pub fn end(&self) -> Option<i64> {
        self.edge.end()
    }

    /// Get the end datetime of the Edge.
    ///
    /// Returns:
    ///    The end datetime of the Edge
    #[getter]
    pub fn end_date_time(&self) -> Option<NaiveDateTime> {
        let end_time = self.edge.end()?;
        NaiveDateTime::from_timestamp_millis(end_time)
    }

    /// Get the duration of the Edge.
    ///
    /// Arguments:
    ///   step (int or str): The step size to use when calculating the duration.
    ///
    /// Returns:
    ///   A set of windows containing edges that fall in the time period
    #[pyo3(signature = (step))]
    fn expanding(
        &self,
        step: PyInterval,
    ) -> Result<WindowSet<EdgeView<DynamicGraph>>, ParseTimeError> {
        self.edge.expanding(step)
    }

    /// Get a set of Edge windows for a given window size, step, start time
    /// and end time using rolling window.
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    ///
    /// Arguments:
    ///   window (int or str): The size of the window.
    ///   step (int or str): The step size to use when calculating the duration. (optional)
    ///
    /// Returns:
    ///   A set of windows containing edges that fall in the time period
    fn rolling(
        &self,
        window: PyInterval,
        step: Option<PyInterval>,
    ) -> Result<WindowSet<EdgeView<DynamicGraph>>, ParseTimeError> {
        self.edge.rolling(window, step)
    }

    /// Get a new Edge with the properties of this Edge within the specified time window.
    ///
    /// Arguments:
    ///   start (int or str): The start time of the window (optional).
    ///   end (int or str): The end time of the window (optional).
    ///
    /// Returns:
    ///   A new Edge with the properties of this Edge within the specified time window.
    #[pyo3(signature = (start = None, end = None))]
    pub fn window(
        &self,
        start: Option<PyTime>,
        end: Option<PyTime>,
    ) -> EdgeView<WindowedGraph<DynamicGraph>> {
        self.edge
            .window(start.unwrap_or(PyTime::MIN), end.unwrap_or(PyTime::MAX))
    }
    /// Get a new Edge with the properties of this Edge within the specified layer.
    ///
    /// Arguments:
    ///   layer_names (str): Layer to be included in the new edge.
    ///
    /// Returns:
    ///   A new Edge with the properties of this Edge within the specified time window.
    #[pyo3(signature = (name))]
    pub fn layer(&self, name: String) -> PyResult<EdgeView<LayeredGraph<DynamicGraph>>> {
        if let Some(edge) = self.edge.layer(name.clone()) {
            Ok(edge)
        } else {
            let available_layers = self.edge.layer_names().collect_vec();
            Err(PyErr::new::<pyo3::exceptions::PyAttributeError, _>(
                format!(
                    "Layer {name:?} not available for edge, available layers: {available_layers:?}"
                ),
            ))
        }
    }

    /// Get a new Edge with the properties of this Edge within the specified layers.
    ///
    /// Arguments:
    ///   layer_names (List<str>): Layers to be included in the new edge.
    ///
    /// Returns:
    ///   A new Edge with the properties of this Edge within the specified time window.
    #[pyo3(signature = (layer_names))]
    pub fn layers(
        &self,
        layer_names: Vec<String>,
    ) -> PyResult<EdgeView<LayeredGraph<DynamicGraph>>> {
        if let Some(edge) = self.edge.layer(layer_names.clone()) {
            Ok(edge)
        } else {
            let available_layers: Vec<_> = self.edge.layer_names().collect();
            Err(PyErr::new::<pyo3::exceptions::PyAttributeError, _>(
                format!("Layers {layer_names:?} not available for edge, available layers: {available_layers:?}"),
            ))
        }
    }

    /// Get a new Edge with the properties of this Edge at a specified time.
    ///
    /// Arguments:
    ///   end (int, str or datetime(utrc)): The time to get the properties at.
    ///
    /// Returns:
    ///   A new Edge with the properties of this Edge at a specified time.
    #[pyo3(signature = (end))]
    pub fn at(&self, end: PyTime) -> EdgeView<WindowedGraph<DynamicGraph>> {
        self.edge.at(end)
    }

    /// Explodes an Edge into a list of PyEdges. This is useful when you want to iterate over
    /// the properties of an Edge at every single point in time. This will return a seperate edge
    /// each time a property had been changed.
    ///
    /// Returns:
    ///     A list of PyEdges
    pub fn explode(&self) -> PyEdges {
        let edge = self.edge.clone();
        (move || edge.explode()).into()
    }

    /// Explodes an Edge into a list of PyEdges, one for each layer the edge is part of. This is useful when you want to iterate over
    /// the properties of an Edge for every layer.
    ///
    /// Returns:
    ///     A list of PyEdges
    pub fn explode_layers(&self) -> PyEdges {
        let edge = self.edge.clone();
        (move || edge.explode_layers()).into()
    }

    /// Gets the earliest time of an edge.
    ///
    /// Returns:
    ///     (int) The earliest time of an edge
    #[getter]
    pub fn earliest_time(&self) -> Option<i64> {
        self.edge.earliest_time()
    }

    /// Gets of earliest datetime of an edge.
    ///
    /// Returns:
    ///     the earliest datetime of an edge
    #[getter]
    pub fn earliest_date_time(&self) -> Option<NaiveDateTime> {
        NaiveDateTime::from_timestamp_millis(self.edge.earliest_time()?)
    }

    /// Gets the latest time of an edge.
    ///
    /// Returns:
    ///     (int) The latest time of an edge
    #[getter]
    pub fn latest_time(&self) -> Option<i64> {
        self.edge.latest_time()
    }

    /// Gets of latest datetime of an edge.
    ///
    /// Returns:
    ///     (datetime) the latest datetime of an edge
    #[getter]
    pub fn latest_date_time(&self) -> Option<NaiveDateTime> {
        let latest_time = self.edge.latest_time()?;
        NaiveDateTime::from_timestamp_millis(latest_time)
    }

    /// Gets the time of an exploded edge.
    ///
    /// Returns:
    ///     (int) The time of an exploded edge
    #[getter]
    pub fn time(&self) -> Option<i64> {
        self.edge.time()
    }

    /// Gets the names of the layers this edge belongs to
    ///
    /// Returns:
    ///     (List<str>) The name of the layer
    #[getter]
    pub fn layer_names(&self) -> Vec<ArcStr> {
        self.edge.layer_names().collect_vec()
    }

    /// Gets the name of the layer this edge belongs to - assuming it only belongs to one layer
    ///
    /// Returns:
    ///     (List<str>) The name of the layer
    #[getter]
    pub fn layer_name(&self) -> Option<ArcStr> {
        self.edge.layer_name().map(|v| v.clone())
    }

    /// Gets the datetime of an exploded edge.
    ///
    /// Returns:
    ///     (datetime) the datetime of an exploded edge
    #[getter]
    pub fn date_time(&self) -> Option<NaiveDateTime> {
        let date_time = self.edge.time()?;
        NaiveDateTime::from_timestamp_millis(date_time)
    }

    /// Displays the Edge as a string.
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyEdge {
    fn repr(&self) -> String {
        self.edge.repr()
    }
}

impl<G: GraphViewOps> Repr for EdgeView<G> {
    fn repr(&self) -> String {
        let properties: String = self
            .properties()
            .iter()
            .map(|(k, v)| format!("{}: {}", k.deref(), v))
            .join(", ");

        let source = self.src().name();
        let target = self.dst().name();
        let earliest_time = self.earliest_time().repr();
        let latest_time = self.latest_time().repr();
        if properties.is_empty() {
            format!(
                "Edge(source={}, target={}, earliest_time={}, latest_time={})",
                source.trim_matches('"'),
                target.trim_matches('"'),
                earliest_time,
                latest_time,
            )
        } else {
            format!(
                "Edge(source={}, target={}, earliest_time={}, latest_time={}, properties={})",
                source.trim_matches('"'),
                target.trim_matches('"'),
                earliest_time,
                latest_time,
                format!("{{{properties}}}")
            )
        }
    }
}

impl Repr for PyMutableEdge {
    fn repr(&self) -> String {
        self.edge.repr()
    }
}
#[pymethods]
impl PyMutableEdge {
    fn add_updates(
        &self,
        t: PyTime,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.edge
            .add_updates(t, properties.unwrap_or_default(), layer)
    }

    fn add_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.edge.add_constant_properties(properties, layer)
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}

/// A list of edges that can be iterated over.
#[pyclass(name = "Edges")]
pub struct PyEdges {
    builder: Arc<dyn Fn() -> BoxedIter<EdgeView<DynamicGraph>> + Send + Sync + 'static>,
}

impl PyEdges {
    /// an iterable that can be used in rust
    fn iter(&self) -> BoxedIter<EdgeView<DynamicGraph>> {
        (self.builder)()
    }

    /// returns an iterable used in python
    fn py_iter(&self) -> BoxedIter<PyEdge> {
        Box::new(self.iter().map(|e| e.into()))
    }
}

#[pymethods]
impl PyEdges {
    fn __iter__(&self) -> PyGenericIterator {
        self.py_iter().into()
    }

    /// Returns all source vertices of the Edges as an iterable.
    ///
    /// Returns:
    ///   The source vertices of the Edges as an iterable.
    #[getter]
    fn src(&self) -> PyVertexIterable {
        let builder = self.builder.clone();
        (move || builder().src()).into()
    }

    /// Returns all destination vertices as an iterable
    #[getter]
    fn dst(&self) -> PyVertexIterable {
        let builder = self.builder.clone();
        (move || builder().dst()).into()
    }

    /// Returns all edges as a list
    fn collect(&self) -> Vec<PyEdge> {
        self.py_iter().collect()
    }

    /// Returns the number of edges
    fn count(&self) -> usize {
        self.py_iter().count()
    }

    /// Explodes the edges into a list of edges. This is useful when you want to iterate over
    /// the properties of an Edge at every single point in time. This will return a seperate edge
    /// each time a property had been changed.
    fn explode(&self) -> PyEdges {
        let builder = self.builder.clone();
        (move || {
            let iter: BoxedIter<EdgeView<DynamicGraph>> =
                Box::new(builder().flat_map(|e| e.explode()));
            iter
        })
        .into()
    }

    /// Explodes each edge into a list of edges, one for each layer the edge is part of. This is useful when you want to iterate over
    /// the properties of an Edge for every layer.
    fn explode_layers(&self) -> PyEdges {
        let builder = self.builder.clone();
        (move || {
            let iter: BoxedIter<EdgeView<DynamicGraph>> =
                Box::new(builder().flat_map(|e| e.explode_layers()));
            iter
        })
        .into()
    }

    /// Returns the earliest time of the edges.
    ///
    /// Returns:
    /// Earliest time of the edges.
    #[getter]
    fn earliest_time(&self) -> OptionI64Iterable {
        let edges: Arc<
            dyn Fn() -> Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send> + Send + Sync,
        > = self.builder.clone();
        (move || edges().earliest_time()).into()
    }

    /// Returns the earliest date time of the edges.
    ///
    /// Returns:
    ///  Earliest date time of the edges.
    #[getter]
    fn earliest_date_time(&self) -> OptionNaiveDateTimeIterable {
        let edges = self.builder.clone();
        (move || edges().earliest_date_time()).into()
    }

    /// Returns the latest time of the edges.
    ///
    /// Returns:
    ///  Latest time of the edges.
    #[getter]
    fn latest_time(&self) -> OptionI64Iterable {
        let edges: Arc<
            dyn Fn() -> Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send> + Send + Sync,
        > = self.builder.clone();
        (move || edges().latest_time()).into()
    }

    /// Returns the latest date time of the edges.
    ///
    /// Returns:
    ///   Latest date time of the edges.
    #[getter]
    fn latest_date_time(&self) -> OptionNaiveDateTimeIterable {
        let edges = self.builder.clone();
        (move || edges().latest_date_time()).into()
    }

    /// Returns the date times of exploded edges
    ///
    /// Returns:
    ///    A list of date times.
    #[getter]
    fn date_time(&self) -> OptionNaiveDateTimeIterable {
        let edges = self.builder.clone();
        (move || edges().date_time()).into()
    }

    /// Returns the times of exploded edges
    ///
    /// Returns:
    ///   Time of edge
    #[getter]
    fn time(&self) -> OptionI64Iterable {
        let edges: Arc<
            dyn Fn() -> Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send> + Send + Sync,
        > = self.builder.clone();
        (move || edges().time()).into()
    }

    /// Returns all properties of the edges
    #[getter]
    fn properties(&self) -> PyPropsList {
        let builder = self.builder.clone();
        (move || builder().properties()).into()
    }

    /// Returns all ids of the edges.
    #[getter]
    fn id(&self) -> U64U64Iterable {
        let edges = self.builder.clone();
        (move || edges().id()).into()
    }

    /// Returns all timestamps of edges, when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///    A list of timestamps.
    ///

    fn history(&self) -> I64VecIterable {
        let edges = self.builder.clone();
        (move || edges().history()).into()
    }

    /// Get the start time of all edges
    ///
    /// Returns:
    /// The start time of all edges
    #[getter]
    fn start(&self) -> OptionI64Iterable {
        let edges = self.builder.clone();
        (move || edges().start()).into()
    }

    /// Get the start date time of all edges
    ///
    /// Returns:
    /// The start date time of all edges
    #[getter]
    fn start_date_time(&self) -> OptionNaiveDateTimeIterable {
        let edges = self.builder.clone();
        (move || edges().start_date_time()).into()
    }

    /// Get the end time of all edges
    ///
    /// Returns:
    /// The end time of all edges
    #[getter]
    fn end(&self) -> OptionI64Iterable {
        let edges = self.builder.clone();
        (move || edges().end()).into()
    }

    /// Get the end date time of all edges
    ///
    /// Returns:
    ///  The end date time of all edges
    #[getter]
    fn end_date_time(&self) -> OptionNaiveDateTimeIterable {
        let edges = self.builder.clone();
        (move || edges().end_date_time()).into()
    }

    /// Get the layer name that all edges belong to - assuming they only belong to one layer
    ///
    /// Returns:
    ///  The name of the layer
    #[getter]
    fn layer_name(&self) -> OptionArcStringIterable {
        let edges = self.builder.clone();
        (move || edges().layer_name()).into()
    }

    /// Get the layer names that all edges belong to - assuming they only belong to one layer
    ///
    /// Returns:
    ///   A list of layer names
    #[getter]
    fn layer_names(&self) -> ArcStringVecIterable {
        let edges = self.builder.clone();
        (move || edges().layer_names().map(|e| e.collect_vec())).into()
    }

    /// Get edges with the properties of these edges within the specified layer.
    ///
    /// Arguments:
    ///     name (str): The name of the layer.
    ///
    /// Returns:
    ///    A list of edges with the properties of these edges within the specified layer.
    #[pyo3(signature = (name))]
    fn layer(&self, name: String) -> PyEdges {
        let builder = self.builder.clone();
        let layers: Layer = name.into();
        (move || {
            let layers = layers.clone();
            let box_builder: Box<(dyn Iterator<Item = EdgeView<DynamicGraph>> + Send + 'static)> =
                Box::new(builder().flat_map(move |e| {
                    e.layer(layers.clone())
                        .map(|e| <EdgeView<DynamicGraph>>::from(e))
                }));
            box_builder
        })
        .into()
    }

    /// Get edges with the properties of these edges within the specified layers.
    ///
    /// Arguments:
    ///    layer_names ([str]): The names of the layers.
    ///
    /// Returns:
    ///   A list of edges with the properties of these edges within the specified layers.
    #[pyo3(signature = (layer_names))]
    fn layers(&self, layer_names: Vec<String>) -> PyEdges {
        let builder = self.builder.clone();
        let layers: Layer = layer_names.into();

        (move || {
            let layers = layers.clone();
            let box_builder: Box<(dyn Iterator<Item = EdgeView<DynamicGraph>> + Send + 'static)> =
                Box::new(builder().flat_map(move |e| {
                    e.layer(layers.clone())
                        .map(|e| <EdgeView<DynamicGraph>>::from(e))
                }));
            box_builder
        })
        .into()
    }

    /// Get edges with the properties of these edges within the specific time window.
    ///
    /// Arguments:
    ///    start (int | str): The start time of the window (optional).
    ///    end (int | str): The end time of the window (optional).
    ///
    /// Returns:
    ///  A list of edges with the properties of these edges within the specified time window.
    #[pyo3(signature = (start = None, end = None))]
    fn window(&self, start: Option<PyTime>, end: Option<PyTime>) -> PyEdges {
        let builder = self.builder.clone();

        (move || {
            let start = start.clone().unwrap_or(PyTime::MIN);
            let end = end.clone().unwrap_or(PyTime::MAX);
            let box_builder: Box<(dyn Iterator<Item = EdgeView<DynamicGraph>> + Send + 'static)> =
                Box::new(
                    builder()
                        .map(move |e| e.window(start.clone(), end.clone()))
                        .map(|e| <EdgeView<DynamicGraph>>::from(e)),
                );
            box_builder
        })
        .into()
    }

    /// Get edges with the properties of these edges at a specific time.
    ///
    /// Arguments:
    ///     end(int): The time to get the properties at.
    ///
    /// Returns:
    ///    A list of edges with the properties of these edges at a specific time.
    #[pyo3(signature = (end))]
    fn at(&self, end: PyTime) -> PyEdges {
        let builder = self.builder.clone();

        (move || {
            let end = end.clone();
            let box_builder: Box<(dyn Iterator<Item = EdgeView<DynamicGraph>> + Send + 'static)> =
                Box::new(
                    builder()
                        .map(move |e| e.at(end.clone()))
                        .map(|e| <EdgeView<DynamicGraph>>::from(e)),
                );
            box_builder
        })
        .into()
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyEdges {
    fn repr(&self) -> String {
        format!("Edges({})", iterator_repr(self.iter()))
    }
}

impl<F: Fn() -> BoxedIter<EdgeView<DynamicGraph>> + Send + Sync + 'static> From<F> for PyEdges {
    fn from(value: F) -> Self {
        Self {
            builder: Arc::new(value),
        }
    }
}

py_nested_iterable!(PyNestedEdges, EdgeView<DynamicGraph>);

#[pymethods]
impl PyNestedEdges {
    /// Returns all source vertices of the Edges as an iterable.
    ///
    /// Returns:
    ///   The source verticeÍs of the Edges as an iterable.
    #[getter]
    fn src(&self) -> PyNestedVertexIterable {
        let builder = self.builder.clone();
        (move || builder().src()).into()
    }

    /// Returns all destination vertices as an iterable
    #[getter]
    fn dst(&self) -> PyNestedVertexIterable {
        let builder = self.builder.clone();
        (move || builder().dst()).into()
    }

    /// Returns the earliest time of the edges.
    #[getter]
    fn earliest_time(&self) -> NestedOptionI64Iterable {
        let edges = self.builder.clone();
        (move || edges().earliest_time()).into()
    }

    /// Returns the earliest date time of the edges.
    #[getter]
    fn earliest_date_time(&self) -> NestedNaiveDateTimeIterable {
        let edges = self.builder.clone();
        (move || edges().earliest_date_time()).into()
    }

    /// Returns the latest time of the edges.
    #[getter]
    fn latest_time(&self) -> NestedOptionI64Iterable {
        let edges = self.builder.clone();
        (move || edges().latest_time()).into()
    }

    /// Returns the latest date time of the edges.
    #[getter]
    fn latest_date_time(&self) -> NestedNaiveDateTimeIterable {
        let edges = self.builder.clone();
        (move || edges().latest_date_time()).into()
    }

    /// Returns the times of exploded edges
    #[getter]
    fn time(&self) -> NestedOptionI64Iterable {
        let edges = self.builder.clone();
        (move || edges().time()).into()
    }

    /// Returns the name of the layer the edges belong to - assuming they only belong to one layer
    #[getter]
    fn layer_name(&self) -> NestedOptionArcStringIterable {
        let edges = self.builder.clone();
        (move || edges().layer_name()).into()
    }

    /// Returns the names of the layers the edges belong to
    #[getter]
    fn layer_names(&self) -> NestedArcStringVecIterable {
        let edges = self.builder.clone();
        (move || {
            edges().layer_names().map(
                |e: Box<dyn Iterator<Item = Box<dyn Iterator<Item = ArcStr> + Send>> + Send>| {
                    e.map(|e| e.collect_vec())
                },
            )
        })
        .into()
    }

    // FIXME: needs a view that allows indexing into the properties
    /// Returns all properties of the edges
    #[getter]
    fn properties(&self) -> PyNestedPropsIterable {
        let builder = self.builder.clone();
        (move || builder().properties()).into()
    }

    /// Returns all ids of the edges.
    #[getter]
    fn id(&self) -> NestedU64U64Iterable {
        let edges = self.builder.clone();
        (move || edges().id()).into()
    }

    /// Explode each edge, creating a separate edge instance for each edge event
    fn explode(&self) -> PyNestedEdges {
        let builder = self.builder.clone();
        (move || {
            let iter: BoxedIter<BoxedIter<EdgeView<DynamicGraph>>> = Box::new(builder().map(|e| {
                let inner_box: BoxedIter<EdgeView<DynamicGraph>> =
                    Box::new(e.flat_map(|e| e.explode()));
                inner_box
            }));
            iter
        })
        .into()
    }

    /// Explode each edge over layers, creating a separate edge instance for each layer the edge is part of
    fn explode_layers(&self) -> PyNestedEdges {
        let builder = self.builder.clone();
        (move || {
            let iter: BoxedIter<BoxedIter<EdgeView<DynamicGraph>>> = Box::new(builder().map(|e| {
                let inner_box: BoxedIter<EdgeView<DynamicGraph>> =
                    Box::new(e.flat_map(|e| e.explode_layers()));
                inner_box
            }));
            iter
        })
        .into()
    }

    /// Returns all timestamps of edges, when an edge is added or change to an edge is made.
    fn history(&self) -> NestedI64VecIterable {
        let edges = self.builder.clone();
        (move || edges().history()).into()
    }

    /// Get the start time of all edges
    #[getter]
    fn start(&self) -> NestedOptionI64Iterable {
        let edges = self.builder.clone();
        (move || edges().start()).into()
    }

    /// Get the start date time of all edges
    #[getter]
    fn start_date_time(&self) -> NestedNaiveDateTimeIterable {
        let edges = self.builder.clone();
        (move || edges().start_date_time()).into()
    }

    /// Get the end time of all edges
    #[getter]
    fn end(&self) -> NestedOptionI64Iterable {
        let edges = self.builder.clone();
        (move || edges().end()).into()
    }

    /// Get the end date time of all edges
    #[getter]
    fn end_date_time(&self) -> NestedNaiveDateTimeIterable {
        let edges = self.builder.clone();
        (move || edges().end_date_time()).into()
    }

    /// Get the date times of exploded edges
    #[getter]
    fn date_time(&self) -> NestedNaiveDateTimeIterable {
        let edges = self.builder.clone();
        (move || edges().date_time()).into()
    }
}

/// A direction used by an edge, being incoming or outgoing
#[pyclass]
#[derive(Clone)]
pub struct PyDirection {
    inner: Direction,
}

#[pymethods]
impl PyDirection {
    #[new]
    pub fn new(direction: &str) -> Self {
        match direction {
            "OUT" => PyDirection {
                inner: Direction::OUT,
            },
            "IN" => PyDirection {
                inner: Direction::IN,
            },
            "BOTH" => PyDirection {
                inner: Direction::BOTH,
            },
            _ => panic!("Invalid direction"),
        }
    }

    fn as_str(&self) -> &str {
        match self.inner {
            Direction::OUT => "OUT",
            Direction::IN => "IN",
            Direction::BOTH => "BOTH",
        }
    }
}

impl Into<Direction> for PyDirection {
    fn into(self) -> Direction {
        self.inner
    }
}

impl From<String> for PyDirection {
    fn from(s: String) -> Self {
        match s.to_uppercase().as_str() {
            "OUT" => PyDirection {
                inner: Direction::OUT,
            },
            "IN" => PyDirection {
                inner: Direction::IN,
            },
            "BOTH" => PyDirection {
                inner: Direction::BOTH,
            },
            _ => panic!("Invalid direction string"),
        }
    }
}
