//! The edge module contains the PyEdge class, which is used to represent edges in the graph and
//! provides access to the edge's properties and vertices.
//!
//! The PyEdge class also provides access to the perspective APIs, which allow the user to view the
//! edge as it existed at a particular point in time, or as it existed over a particular time range.
//!
use crate::{
    core::{utils::time::error::ParseTimeError, Direction},
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{DynamicGraph, IntoDynamic},
                BoxedIter, WindowSet,
            },
        },
        graph::{
            edge::EdgeView,
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
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
                NestedOptionI64Iterable, NestedU64U64Iterable, OptionI64Iterable,
            },
        },
        utils::{PyGenericIterable, PyGenericIterator, PyInterval, PyTime},
    },
};
use chrono::NaiveDateTime;
use pyo3::{prelude::*, pyclass::CompareOp, types::PyString};
use serde_json::to_string;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// PyEdge is a Python class that represents an edge in the graph.
/// An edge is a directed connection between two vertices.
#[pyclass(name = "Edge")]
pub struct PyEdge {
    pub(crate) edge: EdgeView<DynamicGraph>,
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

impl<G: GraphViewOps + IntoDynamic> IntoPy<PyObject> for EdgeView<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let py_version: PyEdge = self.into();
        py_version.into_py(py)
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
    #[getter]
    pub fn properties(&self) -> Properties<EdgeView<DynamicGraph>> {
        self.edge.properties()
    }

    /// Get the source vertex of the Edge.
    ///
    /// Returns:
    ///   The source vertex of the Edge.
    fn src(&self) -> PyVertex {
        self.edge.src().into()
    }

    /// Get the destination vertex of the Edge.
    ///
    /// Returns:
    ///   The destination vertex of the Edge.
    fn dst(&self) -> PyVertex {
        self.edge.dst().into()
    }

    //******  Perspective APIS  ******//

    /// Get the start time of the Edge.
    ///
    /// Returns:
    ///  The start time of the Edge.
    pub fn start(&self) -> Option<i64> {
        self.edge.start()
    }

    /// Get the start datetime of the Edge.
    ///
    /// Returns:
    ///     the start datetime of the Edge.
    pub fn start_date_time(&self) -> Option<NaiveDateTime> {
        let start_time = self.edge.start()?;
        Some(NaiveDateTime::from_timestamp_millis(start_time).unwrap())
    }

    /// Get the end time of the Edge.
    ///
    /// Returns:
    ///   The end time of the Edge.
    pub fn end(&self) -> Option<i64> {
        self.edge.end()
    }

    /// Get the end datetime of the Edge.
    ///
    /// Returns:
    ///    The end datetime of the Edge
    pub fn end_date_time(&self) -> Option<NaiveDateTime> {
        let end_time = self.edge.end()?;
        Some(NaiveDateTime::from_timestamp_millis(end_time).unwrap())
    }

    /// Get the duration of the Edge.
    ///
    /// Arguments:
    ///   step (int): The step size to use when calculating the duration.
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
    ///   window (int | str): The size of the window.
    ///   step (int | str): The step size to use when calculating the duration.
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
    ///   t_start (int | str): The start time of the window (optional).
    ///   t_end (int | str): The end time of the window (optional).
    ///
    /// Returns:
    ///   A new Edge with the properties of this Edge within the specified time window.
    #[pyo3(signature = (t_start = None, t_end = None))]
    pub fn window(
        &self,
        t_start: Option<PyTime>,
        t_end: Option<PyTime>,
    ) -> EdgeView<WindowedGraph<DynamicGraph>> {
        self.edge
            .window(t_start.unwrap_or(PyTime::MIN), t_end.unwrap_or(PyTime::MAX))
    }

    /// Get a new Edge with the properties of this Edge within the specified layers.
    ///
    /// Arguments:
    ///   layer_names ([str]): Layers to be included in the new edge.
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
            let available_layers = self.edge.layer_names();
            Err(PyErr::new::<pyo3::exceptions::PyAttributeError, _>(
                format!("Layers {layer_names:?} not available for edge, available layers: {available_layers:?}"),
            ))
        }
    }

    /// Get a new Edge with the properties of this Edge at a specified time.
    ///
    /// Arguments:
    ///   end (int): The time to get the properties at.
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

    /// Gets the earliest time of an edge.
    ///
    /// Returns:
    ///     (int) The earliest time of an edge
    pub fn earliest_time(&self) -> Option<i64> {
        self.edge.earliest_time()
    }

    /// Gets of earliest datetime of an edge.
    ///
    /// Returns:
    ///     the earliest datetime of an edge
    pub fn earliest_date_time(&self) -> Option<NaiveDateTime> {
        Some(NaiveDateTime::from_timestamp_millis(self.edge.earliest_time()?).unwrap())
    }

    /// Gets the latest time of an edge.
    ///
    /// Returns:
    ///     (int) The latest time of an edge
    pub fn latest_time(&self) -> Option<i64> {
        self.edge.latest_time()
    }

    /// Gets of latest datetime of an edge.
    ///
    /// Returns:
    ///     the latest datetime of an edge
    pub fn latest_date_time(&self) -> Option<NaiveDateTime> {
        let latest_time = self.edge.latest_time()?;
        Some(NaiveDateTime::from_timestamp_millis(latest_time).unwrap())
    }

    /// Gets the time of an exploded edge.
    ///
    /// Returns:
    ///     (int) The time of an exploded edge
    pub fn time(&self) -> Option<i64> {
        self.edge.time()
    }

    /// Gets the name of the layer this edge belongs to
    ///
    /// Returns:
    ///     (str) The name of the layer
    pub fn layer_names(&self) -> Vec<String> {
        self.edge.layer_names()
    }

    /// Gets the datetime of an exploded edge.
    ///
    /// Returns:
    ///     (datetime) the datetime of an exploded edge
    pub fn date_time(&self) -> Option<NaiveDateTime> {
        let date_time = self.edge.time()?;
        Some(NaiveDateTime::from_timestamp_millis(date_time).unwrap())
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

impl Repr for EdgeView<DynamicGraph> {
    fn repr(&self) -> String {
        let properties = &self.properties().repr();

        let source = self.src().name();
        let target = self.dst().name();
        let earliest_time = self.earliest_time();
        let latest_time = self.latest_time();
        if properties.is_empty() {
            format!(
                "Edge(source={}, target={}, earliest_time={}, latest_time={})",
                source.trim_matches('"'),
                target.trim_matches('"'),
                earliest_time.unwrap_or(0),
                latest_time.unwrap_or(0),
            )
        } else {
            let property_string: String = format!("{{{properties}}}");
            format!(
                "Edge(source={}, target={}, earliest_time={}, latest_time={}, properties={})",
                source.trim_matches('"'),
                target.trim_matches('"'),
                earliest_time.unwrap_or(0),
                latest_time.unwrap_or(0),
                property_string
            )
        }
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

    fn __len__(&self) -> usize {
        self.iter().count()
    }

    /// Returns all source vertices of the Edges as an iterable.
    ///
    /// Returns:
    ///   The source vertices of the Edges as an iterable.
    fn src(&self) -> PyVertexIterable {
        let builder = self.builder.clone();
        (move || builder().src()).into()
    }

    /// Returns all destination vertices as an iterable
    fn dst(&self) -> PyVertexIterable {
        let builder = self.builder.clone();
        (move || builder().dst()).into()
    }

    /// Returns all edges as a list
    fn collect(&self) -> Vec<PyEdge> {
        self.py_iter().collect()
    }

    /// Returns the first edge
    fn first(&self) -> Option<PyEdge> {
        self.py_iter().next()
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

    /// Returns the earliest time of the edges.
    fn earliest_time(&self) -> OptionI64Iterable {
        let edges: Arc<
            dyn Fn() -> Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send> + Send + Sync,
        > = self.builder.clone();
        (move || edges().earliest_time()).into()
    }

    /// Returns the latest time of the edges.
    fn latest_time(&self) -> OptionI64Iterable {
        let edges: Arc<
            dyn Fn() -> Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send> + Send + Sync,
        > = self.builder.clone();
        (move || edges().latest_time()).into()
    }

    /// Returns all properties of the edges
    #[getter]
    fn properties(&self) -> PyPropsList {
        let builder = self.builder.clone();
        (move || builder().properties()).into()
    }

    /// Returns all ids of the edges.
    fn id(&self) -> PyGenericIterable {
        let edges = self.builder.clone();
        (move || edges().id()).into()
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
    ///   The source vertices of the Edges as an iterable.
    fn src(&self) -> PyNestedVertexIterable {
        let builder = self.builder.clone();
        (move || builder().src()).into()
    }

    /// Returns all destination vertices as an iterable
    fn dst(&self) -> PyNestedVertexIterable {
        let builder = self.builder.clone();
        (move || builder().dst()).into()
    }

    /// Returns the earliest time of the edges.
    fn earliest_time(&self) -> NestedOptionI64Iterable {
        let edges = self.builder.clone();
        (move || edges().earliest_time()).into()
    }

    /// Returns the latest time of the edges.
    fn latest_time(&self) -> NestedOptionI64Iterable {
        let edges = self.builder.clone();
        (move || edges().latest_time()).into()
    }

    // FIXME: needs a view that allows indexing into the properties
    /// Returns all properties of the edges
    #[getter]
    fn properties(&self) -> PyNestedPropsIterable {
        let builder = self.builder.clone();
        (move || builder().properties()).into()
    }

    /// Returns all ids of the edges.
    fn id(&self) -> NestedU64U64Iterable {
        let edges = self.builder.clone();
        (move || edges().id()).into()
    }

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
}

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
