//! Defines the `Vertex`, which represents a vertex in the graph.
//! A vertex is a node in the graph, and can have properties and edges.
//! It can also be used to navigate the graph.

use crate::{
    core::{entities::vertices::vertex_ref::VertexRef, utils::time::error::ParseTimeError, Prop},
    db::{
        api::view::{
            internal::{DynamicGraph, IntoDynamic},
            *,
        },
        graph::{
            path::{PathFromGraph, PathFromVertex},
            vertex::VertexView,
            vertices::Vertices,
            views::window_graph::WindowedGraph,
        },
    },
    python::{
        graph::edge::{PyEdges, PyNestedEdges},
        types::wrappers::iterators::*,
        utils::{PyInterval, PyTime},
    },
    *,
};
use chrono::NaiveDateTime;
use itertools::Itertools;
use pyo3::{
    exceptions::PyIndexError, prelude::*, pyclass, pyclass::CompareOp, pymethods, PyAny, PyObject,
    PyRef, PyRefMut, PyResult, Python,
};
use python::types::repr::{iterator_repr, Repr};
use std::collections::HashMap;

/// A vertex (or node) in the graph.
#[pyclass(name = "Vertex")]
#[derive(Clone)]
pub struct PyVertex {
    vertex: VertexView<DynamicGraph>,
}

impl<G: GraphViewOps + IntoDynamic> From<VertexView<G>> for PyVertex {
    fn from(value: VertexView<G>) -> Self {
        Self {
            vertex: VertexView {
                graph: value.graph.clone().into_dynamic(),
                vertex: value.vertex,
            },
        }
    }
}

impl<G: GraphViewOps + IntoDynamic> IntoPy<PyObject> for VertexView<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyVertex::from(self).into_py(py)
    }
}

/// Converts a python vertex into a rust vertex.
impl From<PyVertex> for VertexRef {
    fn from(value: PyVertex) -> Self {
        value.vertex.into()
    }
}

/// Defines the `Vertex`, which represents a vertex in the graph.
/// A vertex is a node in the graph, and can have properties and edges.
/// It can also be used to navigate the graph.
#[pymethods]
impl PyVertex {
    /// Rich Comparison for Vertex objects
    pub fn __richcmp__(&self, other: PyRef<PyVertex>, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        match op {
            CompareOp::Eq => (self.vertex.id() == other.id()).into_py(py),
            CompareOp::Ne => (self.vertex.id() != other.id()).into_py(py),
            _ => py.NotImplemented(),
        }
    }

    /// TODO: uncomment when we update to py03 0.2
    /// checks if a vertex is equal to another by their id (ids are unqiue)
    ///
    /// Arguments:
    ///    other: The other vertex to compare to.
    ///
    /// Returns:
    ///   True if the vertices are equal, false otherwise.
    // pub fn __eq__(&self, other: &PyVertex) -> bool {
    //     self.vertex.id() == other.vertex.id()
    // }

    /// Returns the hash of the vertex.
    ///
    /// Returns:
    ///   The vertex id.
    pub fn __hash__(&self) -> u64 {
        self.vertex.id()
    }

    /// Returns the id of the vertex.
    /// This is a unique identifier for the vertex.
    ///
    /// Returns:
    ///    The id of the vertex as an integer.
    pub fn id(&self) -> u64 {
        self.vertex.id()
    }

    /// Returns the name of the vertex.
    ///
    /// Returns:
    ///  The name of the vertex as a string.
    pub fn name(&self) -> String {
        self.vertex.name()
    }

    /// Returns the earliest time that the vertex exists.
    ///
    /// Arguments:
    ///    None
    ///
    /// Returns:
    ///     The earliest time that the vertex exists as an integer.
    pub fn earliest_time(&self) -> Option<i64> {
        self.vertex.earliest_time()
    }

    /// Returns the earliest datetime that the vertex exists.
    ///
    /// Arguments:
    ///    None
    ///
    /// Returns:
    ///     The earliest datetime that the vertex exists as an integer.
    pub fn earliest_date_time(&self) -> Option<NaiveDateTime> {
        let earliest_time = self.vertex.earliest_time()?;
        Some(NaiveDateTime::from_timestamp_millis(earliest_time).unwrap())
    }

    /// Returns the latest time that the vertex exists.
    ///
    /// Returns:
    ///     The latest time that the vertex exists as an integer.
    pub fn latest_time(&self) -> Option<i64> {
        self.vertex.latest_time()
    }

    /// Returns the latest datetime that the vertex exists.
    ///
    /// Arguments:
    ///    None
    ///
    /// Returns:
    ///     The latest datetime that the vertex exists as an integer.
    pub fn latest_date_time(&self) -> Option<NaiveDateTime> {
        let latest_time = self.vertex.latest_time()?;
        Some(NaiveDateTime::from_timestamp_millis(latest_time).unwrap())
    }

    /// Gets the property value of this vertex given the name of the property.
    ///
    /// Arguments:
    ///     name: The name of the property.
    ///     include_static: Whether to include static properties. Defaults to true.
    ///
    /// Returns:
    ///    The property value as a `Prop` object.
    pub fn property(&self, name: String, include_static: Option<bool>) -> Option<Prop> {
        let include_static = include_static.unwrap_or(true);
        self.vertex.property(name, include_static)
    }

    /// Returns the history of a property value of a vertex at all times
    ///
    /// Arguments:
    ///    name: The name of the property.
    ///
    /// Returns:
    ///   A list of tuples of the form (time, value) where time is an integer and value is a `Prop` object.
    pub fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        self.vertex.property_history(name)
    }

    /// Returns all the properties of the vertex as a dictionary.
    ///
    /// Arguments:
    ///    include_static: Whether to include static properties. Defaults to true.
    ///
    /// Returns:
    ///   A dictionary of the form {name: value} where name is a string and value is a `Prop` object.
    pub fn properties(&self, include_static: Option<bool>) -> HashMap<String, Prop> {
        let include_static = include_static.unwrap_or(true);
        self.vertex.properties(include_static)
    }

    /// Returns all the properties of the vertex as a dictionary including the history of each property.
    ///
    /// Arguments:
    ///   include_static: Whether to include static properties. Defaults to true.
    ///
    /// Returns:
    ///  A dictionary of the form {name: [(time, value)]} where name is a string, time is an integer, and value is a `Prop` object.
    pub fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.vertex.property_histories()
    }

    /// Returns the names of all the properties of the vertex.
    ///
    /// Arguments:
    ///   include_static: Whether to include static properties. Defaults to true.
    ///
    /// Returns:
    ///  A list of strings of propert names.
    pub fn property_names(&self, include_static: Option<bool>) -> Vec<String> {
        let include_static = include_static.unwrap_or(true);
        self.vertex.property_names(include_static)
    }

    /// Checks if a property exists on this vertex.
    ///
    /// Arguments:
    ///  name: The name of the property.
    ///  include_static: Whether to include static properties. Defaults to true.
    ///
    /// Returns:
    ///     True if the property exists, false otherwise.
    pub fn has_property(&self, name: String, include_static: Option<bool>) -> bool {
        let include_static = include_static.unwrap_or(true);
        self.vertex.has_property(name, include_static)
    }

    /// Checks if a static property exists on this vertex.
    ///
    /// Arguments:
    ///   name: The name of the property.
    ///   
    /// Returns:
    ///   True if the property exists, false otherwise.
    pub fn has_static_property(&self, name: String) -> bool {
        self.vertex.has_static_property(name)
    }

    /// Returns the static property value of this vertex given the name of the property.
    ///
    /// Arguments:
    ///     name: The name of the property.
    ///
    /// Returns:
    ///     The property value as a `Prop` object or None if the property does not exist.
    pub fn static_property(&self, name: String) -> Option<Prop> {
        self.vertex.static_property(name)
    }

    /// Returns static properties of a vertex
    ///
    /// Arguments:
    ///
    /// Returns:
    ///     HashMap<String, Prop> - Returns static properties of a vertex identified by their names
    pub fn static_properties(&self) -> HashMap<String, Prop> {
        self.vertex.static_properties()
    }

    /// Get the degree of this vertex (i.e., the number of edges that are incident to it).
    ///
    /// Returns
    ///     The degree of this vertex.
    pub fn degree(&self) -> usize {
        self.vertex.degree()
    }

    /// Get the in-degree of this vertex (i.e., the number of edges that are incident to it from other vertices).
    ///
    /// Returns:
    ///    The in-degree of this vertex.
    pub fn in_degree(&self) -> usize {
        self.vertex.in_degree()
    }

    /// Get the out-degree of this vertex (i.e., the number of edges that are incident to it from this vertex).
    ///
    /// Returns:
    ///   The out-degree of this vertex.
    pub fn out_degree(&self) -> usize {
        self.vertex.out_degree()
    }

    /// Get the edges that are pointing to or from this vertex.
    ///
    /// Returns:
    ///     A list of `Edge` objects.
    pub fn edges(&self) -> PyEdges {
        let vertex = self.vertex.clone();
        (move || vertex.edges()).into()
    }

    /// Get the edges that are pointing to this vertex.
    ///
    /// Returns:
    ///     A list of `Edge` objects.
    pub fn in_edges(&self) -> PyEdges {
        let vertex = self.vertex.clone();
        (move || vertex.in_edges()).into()
    }

    /// Get the edges that are pointing from this vertex.
    ///
    /// Returns:
    ///    A list of `Edge` objects.
    pub fn out_edges(&self) -> PyEdges {
        let vertex = self.vertex.clone();
        (move || vertex.out_edges()).into()
    }

    /// Get the neighbours of this vertex.
    ///
    /// Returns:
    ///
    ///    A list of `Vertex` objects.
    pub fn neighbours(&self) -> PyPathFromVertex {
        self.vertex.neighbours().into()
    }

    /// Get the neighbours of this vertex that are pointing to it.
    ///
    /// Returns:
    ///   A list of `Vertex` objects.
    pub fn in_neighbours(&self) -> PyPathFromVertex {
        self.vertex.in_neighbours().into()
    }

    /// Get the neighbours of this vertex that are pointing from it.
    ///
    /// Returns:
    ///   A list of `Vertex` objects.
    pub fn out_neighbours(&self) -> PyPathFromVertex {
        self.vertex.out_neighbours().into()
    }

    //******  Perspective APIS  ******//

    /// Gets the earliest time that this vertex is valid.
    ///
    /// Returns:
    ///    The earliest time that this vertex is valid or None if the vertex is valid for all times.
    pub fn start(&self) -> Option<i64> {
        self.vertex.start()
    }

    /// Gets the earliest datetime that this vertex is valid
    ///
    /// Returns:
    ///     The earliest datetime that this vertex is valid or None if the vertex is valid for all times.
    pub fn start_date_time(&self) -> Option<NaiveDateTime> {
        let start_time = self.vertex.start()?;
        Some(NaiveDateTime::from_timestamp_millis(start_time).unwrap())
    }

    /// Gets the latest time that this vertex is valid.
    ///
    /// Returns:
    ///   The latest time that this vertex is valid or None if the vertex is valid for all times.
    pub fn end(&self) -> Option<i64> {
        self.vertex.end()
    }

    /// Gets the latest datetime that this vertex is valid
    ///
    /// Returns:
    ///     The latest datetime that this vertex is valid or None if the vertex is valid for all times.
    pub fn end_date_time(&self) -> Option<NaiveDateTime> {
        let end_time = self.vertex.end()?;
        Some(NaiveDateTime::from_timestamp_millis(end_time).unwrap())
    }

    /// Creates a `PyVertexWindowSet` with the given `step` size and optional `start` and `end` times,    
    /// using an expanding window.
    ///
    /// An expanding window is a window that grows by `step` size at each iteration.
    /// This will tell you whether a vertex exists at different points in the window and what
    /// its properties are at those points.
    ///
    /// Arguments:
    ///  step (int): The step size of the window.
    ///  start (int): The start time of the window. Defaults to the start time of the vertex.
    ///  end (int): The end time of the window. Defaults to the end time of the vertex.
    ///
    /// Returns:
    ///  A `PyVertexWindowSet` object.
    fn expanding(
        &self,
        step: PyInterval,
    ) -> Result<WindowSet<VertexView<DynamicGraph>>, ParseTimeError> {
        self.vertex.expanding(step)
    }

    /// Creates a `PyVertexWindowSet` with the given `window` size and optional `step`, `start` and `end` times,
    /// using a rolling window.
    ///
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    /// This will tell you whether a vertex exists at different points in the window and what
    /// its properties are at those points.
    ///
    /// Arguments:
    ///  window: The size of the window.
    ///  step: The step size of the window. Defaults to the window size.
    ///  start: The start time of the window. Defaults to the start time of the vertex.
    ///  end: The end time of the window. Defaults to the end time of the vertex.
    ///
    /// Returns:
    /// A `PyVertexWindowSet` object.
    fn rolling(
        &self,
        window: PyInterval,
        step: Option<PyInterval>,
    ) -> Result<WindowSet<VertexView<DynamicGraph>>, ParseTimeError> {
        self.vertex.rolling(window, step)
    }

    /// Create a view of the vertex including all events between `t_start` (inclusive) and `t_end` (exclusive)
    ///
    /// Arguments:
    ///     t_start (int): The start time of the window. Defaults to the start time of the vertex.
    ///     t_end (int): The end time of the window. Defaults to the end time of the vertex.
    ///
    /// Returns:
    ///    A `PyVertex` object.
    #[pyo3(signature = (t_start = None, t_end = None))]
    pub fn window(
        &self,
        t_start: Option<PyTime>,
        t_end: Option<PyTime>,
    ) -> VertexView<WindowedGraph<DynamicGraph>> {
        self.vertex
            .window(t_start.unwrap_or(PyTime::MIN), t_end.unwrap_or(PyTime::MAX))
    }

    /// Create a view of the vertex including all events at `t`.
    ///
    /// Arguments:
    ///     end (int): The time of the window.
    ///
    /// Returns:
    ///     A `PyVertex` object.
    #[pyo3(signature = (end))]
    pub fn at(&self, end: PyTime) -> VertexView<WindowedGraph<DynamicGraph>> {
        self.vertex.at(end)
    }

    #[doc = default_layer_doc_string!()]
    pub fn default_layer(&self) -> PyVertex {
        self.vertex.default_layer().into()
    }

    #[doc = layer_doc_string!()]
    #[pyo3(signature = (name))]
    pub fn layer(&self, name: &str) -> Option<PyVertex> {
        Some(self.vertex.layer(name)?.into())
    }

    /// Returns the history of a vertex, including vertex additions and changes made to vertex.
    ///
    /// Returns:
    ///     A list of timestamps of the event history of vertex.
    pub fn history(&self) -> Vec<i64> {
        self.vertex.history()
    }

    //******  Python  ******//
    pub fn __getitem__(&self, name: String) -> Option<Prop> {
        self.property(name, Some(true))
    }

    /// Display the vertex as a string.
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyVertex {
    fn repr(&self) -> String {
        let properties: String = self
            .properties(Some(true))
            .iter()
            .map(|(k, v)| format!("{}={}", k.to_string(), v.to_string()))
            .join(", ");

        if properties.is_empty() {
            format!("Vertex(name={})", self.name().trim_matches('"'))
        } else {
            let property_string: String = format!("{{{}}}", properties);
            format!(
                "Vertex(name={}, properties={})",
                self.name().trim_matches('"'),
                property_string
            )
        }
    }
}

/// A list of vertices that can be iterated over.
#[pyclass(name = "Vertices")]
pub struct PyVertices {
    pub(crate) vertices: Vertices<DynamicGraph>,
}

impl<G: GraphViewOps + IntoDynamic> From<Vertices<G>> for PyVertices {
    fn from(value: Vertices<G>) -> Self {
        Self {
            vertices: Vertices::new(value.graph.into_dynamic()),
        }
    }
}

impl<G: GraphViewOps + IntoDynamic> IntoPy<PyObject> for Vertices<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyVertices::from(self).into_py(py)
    }
}

/// Operations on a list of vertices.
/// These use all the same functions as a normal vertex except it returns a list of results.
#[pymethods]
impl PyVertices {
    /// checks if a list of vertices is equal to another list by their idd (ids are unique)
    ///
    /// Arguments:
    ///    other: The other vertices to compare to.
    ///
    /// Returns:
    ///   True if the vertices are equal, false otherwise.
    fn __eq__(&self, other: &PyVertices) -> bool {
        for (v1, v2) in self.vertices.iter().zip(other.vertices.iter()) {
            if v1.id() != v2.id() {
                return false;
            }
        }
        true
    }

    /// Returns an iterator over the vertices ids
    fn id(&self) -> U64Iterable {
        let vertices = self.vertices.clone();
        (move || vertices.id()).into()
    }

    /// Returns an iterator over the vertices name
    fn name(&self) -> StringIterable {
        let vertices = self.vertices.clone();
        (move || vertices.name()).into()
    }

    /// Returns an iterator over the vertices earliest time
    fn earliest_time(&self) -> OptionI64Iterable {
        let vertices = self.vertices.clone();
        (move || vertices.earliest_time()).into()
    }

    /// Returns an iterator over the vertices latest time
    fn latest_time(&self) -> OptionI64Iterable {
        let vertices = self.vertices.clone();
        (move || vertices.latest_time()).into()
    }

    /// Returns an iterator over the vertices properties
    /// If include_static is true, static properties are included
    ///
    /// Arguments:
    ///     `name` - The name of the property
    ///     `include_static` - If true, static properties are included
    ///
    /// Returns:
    ///     An iterator over the vertices properties
    fn property(&self, name: String, include_static: Option<bool>) -> OptionPropIterable {
        let vertices = self.vertices.clone();
        (move || vertices.property(name.clone(), include_static.unwrap_or(true))).into()
    }

    /// Returns an iterator over the vertices property with the complete history
    /// If include_static is true, static properties are included
    ///
    /// Arguments:
    ///     `name` - The name of the property
    ///
    /// Returns:
    ///     An iterator over the vertices property with the complete history as a
    ///     vector of tuples of the time and the property
    fn property_history(&self, name: String) -> PropHistoryIterable {
        let vertices = self.vertices.clone();
        (move || vertices.property_history(name.clone())).into()
    }

    /// Returns an iterator over the vertices and all their properties
    ///
    /// Arguments:
    ///     `include_static` - If true, static properties are included
    ///
    /// Returns:
    ///     An iterator over the vertices and all their properties
    fn properties(&self, include_static: Option<bool>) -> PropsIterable {
        let vertices = self.vertices.clone();
        (move || vertices.properties(include_static.unwrap_or(true))).into()
    }

    /// Returns an iterator over the vertices and all their properties at all times
    fn property_histories(&self) -> PropHistoriesIterable {
        let vertices = self.vertices.clone();
        (move || vertices.property_histories()).into()
    }

    /// Returns the names of all the properties of the vertices.
    ///
    /// Arguments:
    ///     `include_static` - If true, static properties are included
    fn property_names(&self, include_static: Option<bool>) -> StringVecIterable {
        let vertices = self.vertices.clone();
        (move || vertices.property_names(include_static.unwrap_or(true))).into()
    }

    /// Checks if a property exists on this vertices
    ///
    /// Arguments:
    ///     `name` - The name of the property
    ///     `include_static` - If true, static properties are included
    ///
    /// Returns:
    ///     A vector of booleans indicating if the property exists in each vertex
    fn has_property(&self, name: String, include_static: Option<bool>) -> BoolIterable {
        let vertices = self.vertices.clone();
        (move || vertices.has_property(name.clone(), include_static.unwrap_or(true))).into()
    }

    /// Checks if a static property exists on the vertices
    ///
    /// Arguments:
    ///     `name` - The name of the property
    ///
    /// Returns:
    ///     A vector of booleans indicating if the property exists in each vertex
    fn has_static_property(&self, name: String) -> BoolIterable {
        let vertices = self.vertices.clone();
        (move || vertices.has_static_property(name.clone())).into()
    }

    /// Returns the static property value of the vertices given the name of the property.
    ///
    /// Arguments:
    ///      `name` - The name of the property
    ///
    /// Returns:
    ///     An iterator of the static property of the vertices
    fn static_property(&self, name: String) -> OptionPropIterable {
        let vertices = self.vertices.clone();
        (move || vertices.static_property(name.clone())).into()
    }

    /// Returns static properties of the vertices
    ///
    /// Returns:
    ///     An iterator of the static properties of the vertices
    fn static_properties(&self) -> PropsIterable {
        let vertices = self.vertices.clone();
        (move || vertices.static_properties()).into()
    }

    /// Returns the number of edges of the vertices
    ///
    /// Returns:
    ///     An iterator of the number of edges of the vertices
    fn degree(&self) -> UsizeIterable {
        let vertices = self.vertices.clone();
        (move || vertices.degree()).into()
    }

    /// Returns the number of in edges of the vertices
    ///
    /// Returns:
    ///     An iterator of the number of in edges of the vertices
    fn in_degree(&self) -> UsizeIterable {
        let vertices = self.vertices.clone();
        (move || vertices.in_degree()).into()
    }

    /// Returns the number of out edges of the vertices
    ///
    /// Returns:
    ///     An iterator of the number of out edges of the vertices
    fn out_degree(&self) -> UsizeIterable {
        let vertices = self.vertices.clone();
        (move || vertices.out_degree()).into()
    }

    /// Returns the edges of the vertices
    ///
    /// Returns:
    ///     An iterator of edges of the vertices
    fn edges(&self) -> PyNestedEdges {
        let clone = self.vertices.clone();
        (move || clone.edges()).into()
    }

    /// Returns the in edges of the vertices
    ///
    /// Returns:
    ///     An iterator of in edges of the vertices
    fn in_edges(&self) -> PyNestedEdges {
        let clone = self.vertices.clone();
        (move || clone.in_edges()).into()
    }

    /// Returns the out edges of the vertices
    ///
    /// Returns:
    ///     An iterator of out edges of the vertices
    fn out_edges(&self) -> PyNestedEdges {
        let clone = self.vertices.clone();
        (move || clone.out_edges()).into()
    }

    /// Get the neighbours of the vertices
    ///
    /// Returns:
    ///     An iterator of the neighbours of the vertices
    fn neighbours(&self) -> PyPathFromGraph {
        self.vertices.neighbours().into()
    }

    /// Get the in neighbours of the vertices
    ///
    /// Returns:
    ///     An iterator of the in neighbours of the vertices
    fn in_neighbours(&self) -> PyPathFromGraph {
        self.vertices.in_neighbours().into()
    }

    /// Get the out neighbours of the vertices
    ///
    /// Returns:
    ///     An iterator of the out neighbours of the vertices
    fn out_neighbours(&self) -> PyPathFromGraph {
        self.vertices.out_neighbours().into()
    }

    /// Collects all vertices into a list
    fn collect(&self) -> Vec<PyVertex> {
        self.__iter__().into_iter().collect()
    }

    //*****     Perspective APIS  ******//
    /// Returns the start time of the vertices
    pub fn start(&self) -> Option<i64> {
        self.vertices.start()
    }

    /// Returns the end time of the vertices
    pub fn end(&self) -> Option<i64> {
        self.vertices.end()
    }

    #[doc = window_size_doc_string!()]
    pub fn window_size(&self) -> Option<u64> {
        self.vertices.window_size()
    }

    /// Creates a PyVertexWindowSet with the given step size using an expanding window.
    ///
    /// An expanding window is a window that grows by step size at each iteration.
    /// This will tell you whether a vertex exists at different points in the window
    /// and what its properties are at those points.
    ///
    /// Arguments:
    ///     `step` - The step size of the window
    ///
    /// Returns:
    ///     A PyVertexWindowSet with the given step size and optional start and end times or an error
    fn expanding(
        &self,
        step: PyInterval,
    ) -> Result<WindowSet<Vertices<DynamicGraph>>, ParseTimeError> {
        self.vertices.expanding(step)
    }


    /// Creates a PyVertexWindowSet with the given window size and optional step using a rolling window.
    ///
    /// A rolling window is a window that moves forward by step size at each iteration.
    /// This will tell you whether a vertex exists at different points in the window and
    /// what its properties are at those points.
    ///
    /// Arguments:
    ///     `window` - The window size of the window
    ///     `step` - The step size of the window
    ///
    /// Returns:
    ///     A PyVertexWindowSet with the given window size and optional step size or an error
    fn rolling(
        &self,
        window: PyInterval,
        step: Option<PyInterval>,
    ) -> Result<WindowSet<Vertices<DynamicGraph>>, ParseTimeError> {
        self.vertices.rolling(window, step)
    }

    /// Create a view of the vertices including all events between t_start (inclusive) and
    /// t_end (exclusive)
    ///
    /// Arguments:
    ///     `t_start` - The start time of the window
    ///     `t_end` - The end time of the window
    ///
    /// Returns:
    ///     A `PyVertices` object.
    #[pyo3(signature = (t_start = None, t_end = None))]
    pub fn window(
        &self,
        t_start: Option<PyTime>,
        t_end: Option<PyTime>,
    ) -> Vertices<WindowedGraph<DynamicGraph>> {
        self.vertices
            .window(t_start.unwrap_or(PyTime::MIN), t_end.unwrap_or(PyTime::MAX))
    }

    /// Create a view of the vertices including all events at `t`.
    ///
    /// Arguments:
    ///     end (int): The time of the window.
    ///
    /// Returns:
    ///     A `PyVertices` object.
    #[pyo3(signature = (end))]
    pub fn at(&self, end: PyTime) -> Vertices<WindowedGraph<DynamicGraph>> {
        self.vertices.at(end)
    }

    #[doc = default_layer_doc_string!()]
    pub fn default_layer(&self) -> PyVertices {
        self.vertices.default_layer().into()
    }

    #[doc = layer_doc_string!()]
    #[pyo3(signature = (name))]
    pub fn layer(&self, name: &str) -> Option<PyVertices> {
        Some(self.vertices.layer(name)?.into())
    }

    //****** Python *******
    pub fn __iter__(&self) -> PyVertexIterator {
        self.vertices.iter().into()
    }

    pub fn __len__(&self) -> usize {
        self.vertices.len()
    }

    pub fn __bool__(&self) -> bool {
        self.vertices.is_empty()
    }

    pub fn __getitem__(&self, vertex: VertexRef) -> PyResult<VertexView<DynamicGraph>> {
        self.vertices
            .get(vertex)
            .ok_or_else(|| PyIndexError::new_err("Vertex does not exist"))
    }

    pub fn __call__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyVertices {
    fn repr(&self) -> String {
        format!("Vertices({})", iterator_repr(self.__iter__().into_iter()))
    }
}

#[pyclass(name = "PathFromGraph")]
pub struct PyPathFromGraph {
    path: PathFromGraph<DynamicGraph>,
}

#[pymethods]
impl PyPathFromGraph {
    fn __iter__(&self) -> PathIterator {
        self.path.iter().into()
    }

    fn collect(&self) -> Vec<Vec<PyVertex>> {
        self.__iter__().into_iter().map(|it| it.collect()).collect()
    }
    fn id(&self) -> NestedU64Iterable {
        let path = self.path.clone();
        (move || path.id()).into()
    }

    fn name(&self) -> NestedStringIterable {
        let path = self.path.clone();
        (move || path.name()).into()
    }

    fn earliest_time(&self) -> NestedOptionI64Iterable {
        let path = self.path.clone();
        (move || path.earliest_time()).into()
    }

    fn latest_time(&self) -> NestedOptionI64Iterable {
        let path = self.path.clone();
        (move || path.latest_time()).into()
    }

    fn property(&self, name: String, include_static: Option<bool>) -> NestedOptionPropIterable {
        let path = self.path.clone();
        (move || path.property(name.clone(), include_static.unwrap_or(true))).into()
    }

    fn property_history(&self, name: String) -> NestedPropHistoryIterable {
        let path = self.path.clone();
        (move || path.property_history(name.clone())).into()
    }

    fn properties(&self, include_static: Option<bool>) -> NestedPropsIterable {
        let path = self.path.clone();
        (move || path.properties(include_static.unwrap_or(true))).into()
    }

    fn property_histories(&self) -> NestedPropHistoriesIterable {
        let path = self.path.clone();
        (move || path.property_histories()).into()
    }

    fn property_names(&self, include_static: Option<bool>) -> NestedStringVecIterable {
        let path = self.path.clone();
        (move || path.property_names(include_static.unwrap_or(true))).into()
    }

    fn has_property(&self, name: String, include_static: Option<bool>) -> NestedBoolIterable {
        let path = self.path.clone();
        (move || path.has_property(name.clone(), include_static.unwrap_or(true))).into()
    }

    fn has_static_property(&self, name: String) -> NestedBoolIterable {
        let path = self.path.clone();
        (move || path.has_static_property(name.clone())).into()
    }

    fn static_property(&self, name: String) -> NestedOptionPropIterable {
        let path = self.path.clone();
        (move || path.static_property(name.clone())).into()
    }

    fn degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.degree()).into()
    }

    fn in_degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.in_degree()).into()
    }

    fn out_degree(&self) -> NestedUsizeIterable {
        let path = self.path.clone();
        (move || path.out_degree()).into()
    }

    fn edges(&self) -> PyNestedEdges {
        let clone = self.path.clone();
        (move || clone.edges()).into()
    }

    fn in_edges(&self) -> PyNestedEdges {
        let clone = self.path.clone();
        (move || clone.in_edges()).into()
    }

    fn out_edges(&self) -> PyNestedEdges {
        let clone = self.path.clone();
        (move || clone.out_edges()).into()
    }

    fn out_neighbours(&self) -> Self {
        self.path.out_neighbours().into()
    }

    fn in_neighbours(&self) -> Self {
        self.path.in_neighbours().into()
    }

    fn neighbours(&self) -> Self {
        self.path.neighbours().into()
    }

    //******  Perspective APIS  ******//
    pub fn start(&self) -> Option<i64> {
        self.path.start()
    }

    pub fn end(&self) -> Option<i64> {
        self.path.end()
    }

    #[doc = window_size_doc_string!()]
    pub fn window_size(&self) -> Option<u64> {
        self.path.window_size()
    }

    fn expanding(
        &self,
        step: PyInterval,
    ) -> Result<WindowSet<PathFromGraph<DynamicGraph>>, ParseTimeError> {
        self.path.expanding(step)
    }

    fn rolling(
        &self,
        window: PyInterval,
        step: Option<PyInterval>,
    ) -> Result<WindowSet<PathFromGraph<DynamicGraph>>, ParseTimeError> {
        self.path.rolling(window, step)
    }

    #[pyo3(signature = (t_start = None, t_end = None))]
    pub fn window(
        &self,
        t_start: Option<PyTime>,
        t_end: Option<PyTime>,
    ) -> PathFromGraph<WindowedGraph<DynamicGraph>> {
        self.path
            .window(t_start.unwrap_or(PyTime::MIN), t_end.unwrap_or(PyTime::MAX))
    }

    /// Create a view of the vertex including all events at `t`.
    ///
    /// Arguments:
    ///     end (int): The time of the window.
    ///
    /// Returns:
    ///     A `PyVertex` object.
    #[pyo3(signature = (end))]
    pub fn at(&self, end: PyTime) -> PathFromGraph<WindowedGraph<DynamicGraph>> {
        self.path.at(end)
    }

    #[doc = default_layer_doc_string!()]
    pub fn default_layer(&self) -> Self {
        self.path.default_layer().into()
    }

    #[doc = layer_doc_string!()]
    #[pyo3(signature = (name))]
    pub fn layer(&self, name: &str) -> Option<Self> {
        Some(self.path.layer(name)?.into())
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyPathFromGraph {
    fn repr(&self) -> String {
        format!(
            "PathFromGraph({})",
            iterator_repr(self.__iter__().into_iter())
        )
    }
}

impl<G: GraphViewOps + IntoDynamic> From<PathFromGraph<G>> for PyPathFromGraph {
    fn from(value: PathFromGraph<G>) -> Self {
        Self {
            path: PathFromGraph {
                graph: value.graph.clone().into_dynamic(),
                operations: value.operations,
            },
        }
    }
}

impl<G: GraphViewOps + IntoDynamic> IntoPy<PyObject> for PathFromGraph<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyPathFromGraph::from(self).into_py(py)
    }
}

#[pyclass(name = "PathFromVertex")]
pub struct PyPathFromVertex {
    path: PathFromVertex<DynamicGraph>,
}

impl<G: GraphViewOps + IntoDynamic> From<PathFromVertex<G>> for PyPathFromVertex {
    fn from(value: PathFromVertex<G>) -> Self {
        Self {
            path: PathFromVertex {
                graph: value.graph.clone().into_dynamic(),
                vertex: value.vertex,
                operations: value.operations,
            },
        }
    }
}

impl<G: GraphViewOps + IntoDynamic> IntoPy<PyObject> for PathFromVertex<G> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyPathFromVertex::from(self).into_py(py)
    }
}

#[pymethods]
impl PyPathFromVertex {
    fn __iter__(&self) -> PyVertexIterator {
        self.path.iter().into()
    }

    fn collect(&self) -> Vec<PyVertex> {
        self.__iter__().into_iter().collect()
    }

    fn id(&self) -> U64Iterable {
        let path = self.path.clone();
        (move || path.id()).into()
    }

    fn name(&self) -> StringIterable {
        let path = self.path.clone();
        (move || path.name()).into()
    }

    fn earliest_time(&self) -> OptionI64Iterable {
        let path = self.path.clone();
        (move || path.earliest_time()).into()
    }

    fn latest_time(&self) -> OptionI64Iterable {
        let path = self.path.clone();
        (move || path.latest_time()).into()
    }

    fn property(&self, name: String, include_static: Option<bool>) -> OptionPropIterable {
        let path = self.path.clone();
        (move || path.property(name.clone(), include_static.unwrap_or(true))).into()
    }

    fn property_history(&self, name: String) -> PropHistoryIterable {
        let path = self.path.clone();
        (move || path.property_history(name.clone())).into()
    }

    fn properties(&self, include_static: Option<bool>) -> PropsIterable {
        let path = self.path.clone();
        (move || path.properties(include_static.unwrap_or(true))).into()
    }

    fn property_histories(&self) -> PropHistoriesIterable {
        let path = self.path.clone();
        (move || path.property_histories()).into()
    }

    fn property_names(&self, include_static: Option<bool>) -> StringVecIterable {
        let path = self.path.clone();
        (move || path.property_names(include_static.unwrap_or(true))).into()
    }

    fn has_property(&self, name: String, include_static: Option<bool>) -> BoolIterable {
        let path = self.path.clone();
        (move || path.has_property(name.clone(), include_static.unwrap_or(true))).into()
    }

    fn has_static_property(&self, name: String) -> BoolIterable {
        let path = self.path.clone();
        (move || path.has_static_property(name.clone())).into()
    }

    fn static_property(&self, name: String) -> OptionPropIterable {
        let path = self.path.clone();
        (move || path.static_property(name.clone())).into()
    }

    fn in_degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.in_degree()).into()
    }

    fn out_degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.out_degree()).into()
    }

    fn degree(&self) -> UsizeIterable {
        let path = self.path.clone();
        (move || path.degree()).into()
    }

    fn edges(&self) -> PyEdges {
        let path = self.path.clone();
        (move || path.edges()).into()
    }

    fn in_edges(&self) -> PyEdges {
        let path = self.path.clone();
        (move || path.in_edges()).into()
    }

    fn out_edges(&self) -> PyEdges {
        let path = self.path.clone();
        (move || path.out_edges()).into()
    }

    fn out_neighbours(&self) -> Self {
        self.path.out_neighbours().into()
    }

    fn in_neighbours(&self) -> Self {
        self.path.in_neighbours().into()
    }

    fn neighbours(&self) -> Self {
        self.path.neighbours().into()
    }

    //******  Perspective APIS  ******//
    pub fn start(&self) -> Option<i64> {
        self.path.start()
    }

    pub fn end(&self) -> Option<i64> {
        self.path.end()
    }

    #[doc = window_size_doc_string!()]
    pub fn window_size(&self) -> Option<u64> {
        self.path.window_size()
    }

    fn expanding(
        &self,
        step: PyInterval,
    ) -> Result<WindowSet<PathFromVertex<DynamicGraph>>, ParseTimeError> {
        self.path.expanding(step)
    }

    fn rolling(
        &self,
        window: PyInterval,
        step: Option<PyInterval>,
    ) -> Result<WindowSet<PathFromVertex<DynamicGraph>>, ParseTimeError> {
        self.path.rolling(window, step)
    }

    #[pyo3(signature = (t_start = None, t_end = None))]
    pub fn window(
        &self,
        t_start: Option<PyTime>,
        t_end: Option<PyTime>,
    ) -> PathFromVertex<WindowedGraph<DynamicGraph>> {
        self.path
            .window(t_start.unwrap_or(PyTime::MIN), t_end.unwrap_or(PyTime::MAX))
    }

    /// Create a view of the vertex including all events at `t`.
    ///
    /// Arguments:
    ///     end (int): The time of the window.
    ///
    /// Returns:
    ///     A `PyVertex` object.
    #[pyo3(signature = (end))]
    pub fn at(&self, end: PyTime) -> PathFromVertex<WindowedGraph<DynamicGraph>> {
        self.path.at(end)
    }

    pub fn default_layer(&self) -> Self {
        self.path.default_layer().into()
    }

    #[doc = layer_doc_string!()]
    #[pyo3(signature = (name))]
    pub fn layer(&self, name: &str) -> Option<Self> {
        Some(self.path.layer(name)?.into())
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyPathFromVertex {
    fn repr(&self) -> String {
        format!(
            "PathFromVertex({})",
            iterator_repr(self.__iter__().into_iter())
        )
    }
}

#[pyclass(name = "VertexIterator")]
pub struct PyVertexIterator {
    iter: Box<dyn Iterator<Item = PyVertex> + Send>,
}

impl From<Box<dyn Iterator<Item = VertexView<DynamicGraph>> + Send>> for PyVertexIterator {
    fn from(value: Box<dyn Iterator<Item = VertexView<DynamicGraph>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|v| v.into())),
        }
    }
}

impl IntoIterator for PyVertexIterator {
    type Item = PyVertex;
    type IntoIter = Box<dyn Iterator<Item = PyVertex> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

#[pymethods]
impl PyVertexIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyVertex> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = PyVertex> + Send>> for PyVertexIterator {
    fn from(value: Box<dyn Iterator<Item = PyVertex> + Send>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct PathIterator {
    pub(crate) iter: Box<dyn Iterator<Item = PyPathFromVertex> + Send>,
}

impl IntoIterator for PathIterator {
    type Item = PyPathFromVertex;
    type IntoIter = Box<dyn Iterator<Item = PyPathFromVertex> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

impl From<Box<dyn Iterator<Item = PathFromVertex<DynamicGraph>> + Send>> for PathIterator {
    fn from(value: Box<dyn Iterator<Item = PathFromVertex<DynamicGraph>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|path| path.into())),
        }
    }
}

#[pymethods]
impl PathIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyPathFromVertex> {
        slf.iter.next()
    }
}

py_iterable!(
    PyVertexIterable,
    VertexView<DynamicGraph>,
    PyVertex,
    PyVertexIterator
);

#[pymethods]
impl PyVertexIterable {
    fn id(&self) -> U64Iterable {
        let builder = self.builder.clone();
        (move || builder().id()).into()
    }

    fn name(&self) -> StringIterable {
        let vertices = self.builder.clone();
        (move || vertices().name()).into()
    }

    fn earliest_time(&self) -> OptionI64Iterable {
        let vertices = self.builder.clone();
        (move || vertices().earliest_time()).into()
    }

    fn latest_time(&self) -> OptionI64Iterable {
        let vertices = self.builder.clone();
        (move || vertices().latest_time()).into()
    }

    fn property(&self, name: String, include_static: Option<bool>) -> OptionPropIterable {
        let vertices = self.builder.clone();
        (move || vertices().property(name.clone(), include_static.unwrap_or(true))).into()
    }

    fn property_history(&self, name: String) -> PropHistoryIterable {
        let vertices = self.builder.clone();
        (move || vertices().property_history(name.clone())).into()
    }

    fn properties(&self, include_static: Option<bool>) -> PropsIterable {
        let vertices = self.builder.clone();
        (move || vertices().properties(include_static.unwrap_or(true))).into()
    }

    fn property_histories(&self) -> PropHistoriesIterable {
        let vertices = self.builder.clone();
        (move || vertices().property_histories()).into()
    }

    fn property_names(&self, include_static: Option<bool>) -> StringVecIterable {
        let vertices = self.builder.clone();
        (move || vertices().property_names(include_static.unwrap_or(true))).into()
    }

    fn has_property(&self, name: String, include_static: Option<bool>) -> BoolIterable {
        let vertices = self.builder.clone();
        (move || vertices().has_property(name.clone(), include_static.unwrap_or(true))).into()
    }

    fn has_static_property(&self, name: String) -> BoolIterable {
        let vertices = self.builder.clone();
        (move || vertices().has_static_property(name.clone())).into()
    }

    fn static_property(&self, name: String) -> OptionPropIterable {
        let vertices = self.builder.clone();
        (move || vertices().static_property(name.clone())).into()
    }

    fn degree(&self) -> UsizeIterable {
        let vertices = self.builder.clone();
        (move || vertices().degree()).into()
    }

    fn in_degree(&self) -> UsizeIterable {
        let vertices = self.builder.clone();
        (move || vertices().in_degree()).into()
    }

    fn out_degree(&self) -> UsizeIterable {
        let vertices = self.builder.clone();
        (move || vertices().out_degree()).into()
    }

    fn edges(&self) -> PyEdges {
        let clone = self.builder.clone();
        (move || clone().edges()).into()
    }

    fn in_edges(&self) -> PyEdges {
        let clone = self.builder.clone();
        (move || clone().in_edges()).into()
    }

    fn out_edges(&self) -> PyEdges {
        let clone = self.builder.clone();
        (move || clone().out_edges()).into()
    }

    fn out_neighbours(&self) -> Self {
        let builder = self.builder.clone();
        (move || builder().out_neighbours()).into()
    }

    fn in_neighbours(&self) -> Self {
        let builder = self.builder.clone();
        (move || builder().in_neighbours()).into()
    }

    fn neighbours(&self) -> Self {
        let builder = self.builder.clone();
        (move || builder().neighbours()).into()
    }
}
