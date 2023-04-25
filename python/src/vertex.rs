//! Defines the `Vertex`, which represents a vertex in the graph.
//! A vertex is a node in the graph, and can have properties and edges.
//! It can also be used to navigate the graph.
use crate::dynamic::{DynamicGraph, IntoDynamic};
use crate::edge::{PyEdges, PyNestedEdges};
use crate::types::repr::{iterator_repr, Repr};
use crate::utils::{expanding_impl, extract_vertex_ref, rolling_impl, window_impl};
use crate::wrappers::iterators::*;
use crate::wrappers::prop::Prop;
use itertools::Itertools;
use pyo3::exceptions::PyIndexError;
use pyo3::{pyclass, pymethods, PyAny, PyRef, PyRefMut, PyResult};
use raphtory::core::tgraph::VertexRef;
use raphtory::db::path::{PathFromGraph, PathFromVertex};
use raphtory::db::vertex::VertexView;
use raphtory::db::vertices::Vertices;
use raphtory::db::view_api::time::WindowSet;
use raphtory::db::view_api::*;
use std::collections::HashMap;

/// A vertex (or node) in the graph.
#[pyclass(name = "Vertex")]
#[derive(Clone)]
pub struct PyVertex {
    vertex: VertexView<DynamicGraph>,
}

/// Converts a rust vertex into a python vertex.
impl From<VertexView<DynamicGraph>> for PyVertex {
    fn from(value: VertexView<DynamicGraph>) -> Self {
        PyVertex { vertex: value }
    }
}

impl From<VertexView<WindowedGraph<DynamicGraph>>> for PyVertex {
    fn from(value: VertexView<WindowedGraph<DynamicGraph>>) -> Self {
        Self {
            vertex: VertexView {
                graph: value.graph.into_dynamic(),
                vertex: value.vertex,
            },
        }
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

    /// Returns the latest time that the vertex exists.
    ///
    /// Returns:
    ///     The latest time that the vertex exists as an integer.
    pub fn latest_time(&self) -> Option<i64> {
        self.vertex.latest_time()
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
        self.vertex
            .property(name, include_static)
            .map(|prop| prop.into())
    }

    /// Returns the history of a property value of a vertex at all times
    ///
    /// Arguments:
    ///    name: The name of the property.
    ///
    /// Returns:
    ///   A list of tuples of the form (time, value) where time is an integer and value is a `Prop` object.
    pub fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        self.vertex
            .property_history(name)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
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
        self.vertex
            .properties(include_static)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    /// Returns all the properties of the vertex as a dictionary including the history of each property.
    ///
    /// Arguments:
    ///   include_static: Whether to include static properties. Defaults to true.
    ///
    /// Returns:
    ///  A dictionary of the form {name: [(time, value)]} where name is a string, time is an integer, and value is a `Prop` object.
    pub fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.vertex
            .property_histories()
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(|(t, p)| (t, p.into())).collect()))
            .collect()
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
        self.vertex.static_property(name).map(|prop| prop.into())
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

    /// Gets the latest time that this vertex is valid.
    ///
    /// Returns:
    ///   The latest time that this vertex is valid or None if the vertex is valid for all times.
    pub fn end(&self) -> Option<i64> {
        self.vertex.end()
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
    fn expanding(&self, step: &PyAny) -> PyResult<PyVertexWindowSet> {
        expanding_impl(&self.vertex, step)
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
    fn rolling(&self, window: &PyAny, step: Option<&PyAny>) -> PyResult<PyVertexWindowSet> {
        rolling_impl(&self.vertex, window, step)
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
    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyVertex {
        window_impl(&self.vertex, t_start, t_end).into()
    }

    /// Create a view of the vertex including all events at `t`.
    ///
    /// Arguments:
    ///     end (int): The time of the window.
    ///
    /// Returns:
    ///     A `PyVertex` object.
    #[pyo3(signature = (end))]
    pub fn at(&self, end: i64) -> PyVertex {
        self.vertex.at(end).into()
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
            .map(|(k, v)| k.to_string() + " : " + &v.to_string())
            .join(", ");

        if properties.is_empty() {
            format!("Vertex(name={})", self.name().trim_matches('"'))
        } else {
            let property_string: String = "{".to_owned() + &properties + "}";
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

impl From<Vertices<DynamicGraph>> for PyVertices {
    fn from(value: Vertices<DynamicGraph>) -> Self {
        Self { vertices: value }
    }
}

impl From<Vertices<WindowedGraph<DynamicGraph>>> for PyVertices {
    fn from(value: Vertices<WindowedGraph<DynamicGraph>>) -> Self {
        Self {
            vertices: Vertices::new(value.graph.into_dynamic()),
        }
    }
}

/// Operations on a list of vertices.
/// These use all the same functions as a normal vertex except it returns a list of results.
#[pymethods]
impl PyVertices {
    fn id(&self) -> U64Iterable {
        let vertices = self.vertices.clone();
        (move || vertices.id()).into()
    }

    fn name(&self) -> StringIterable {
        let vertices = self.vertices.clone();
        (move || vertices.name()).into()
    }

    fn earliest_time(&self) -> OptionI64Iterable {
        let vertices = self.vertices.clone();
        (move || vertices.earliest_time()).into()
    }

    fn latest_time(&self) -> OptionI64Iterable {
        let vertices = self.vertices.clone();
        (move || vertices.latest_time()).into()
    }

    fn property(&self, name: String, include_static: Option<bool>) -> OptionPropIterable {
        let vertices = self.vertices.clone();
        (move || vertices.property(name.clone(), include_static.unwrap_or(true))).into()
    }

    fn property_history(&self, name: String) -> PropHistoryIterable {
        let vertices = self.vertices.clone();
        (move || vertices.property_history(name.clone())).into()
    }

    fn properties(&self, include_static: Option<bool>) -> PropsIterable {
        let vertices = self.vertices.clone();
        (move || vertices.properties(include_static.unwrap_or(true))).into()
    }

    fn property_histories(&self) -> PropHistoriesIterable {
        let vertices = self.vertices.clone();
        (move || vertices.property_histories()).into()
    }

    fn property_names(&self, include_static: Option<bool>) -> StringVecIterable {
        let vertices = self.vertices.clone();
        (move || vertices.property_names(include_static.unwrap_or(true))).into()
    }

    fn has_property(&self, name: String, include_static: Option<bool>) -> BoolIterable {
        let vertices = self.vertices.clone();
        (move || vertices.has_property(name.clone(), include_static.unwrap_or(true))).into()
    }

    fn has_static_property(&self, name: String) -> BoolIterable {
        let vertices = self.vertices.clone();
        (move || vertices.has_static_property(name.clone())).into()
    }

    fn static_property(&self, name: String) -> OptionPropIterable {
        let vertices = self.vertices.clone();
        (move || vertices.static_property(name.clone())).into()
    }

    fn degree(&self) -> UsizeIterable {
        let vertices = self.vertices.clone();
        (move || vertices.degree()).into()
    }

    fn in_degree(&self) -> UsizeIterable {
        let vertices = self.vertices.clone();
        (move || vertices.in_degree()).into()
    }

    fn out_degree(&self) -> UsizeIterable {
        let vertices = self.vertices.clone();
        (move || vertices.out_degree()).into()
    }

    fn edges(&self) -> PyNestedEdges {
        let clone = self.vertices.clone();
        (move || clone.edges()).into()
    }

    fn in_edges(&self) -> PyNestedEdges {
        let clone = self.vertices.clone();
        (move || clone.in_edges()).into()
    }

    fn out_edges(&self) -> PyNestedEdges {
        let clone = self.vertices.clone();
        (move || clone.out_edges()).into()
    }

    fn out_neighbours(&self) -> PyPathFromGraph {
        self.vertices.out_neighbours().into()
    }

    fn in_neighbours(&self) -> PyPathFromGraph {
        self.vertices.in_neighbours().into()
    }

    fn neighbours(&self) -> PyPathFromGraph {
        self.vertices.neighbours().into()
    }

    fn collect(&self) -> Vec<PyVertex> {
        self.__iter__().into_iter().collect()
    }

    //******  Perspective APIS  ******//
    pub fn start(&self) -> Option<i64> {
        self.vertices.start()
    }

    pub fn end(&self) -> Option<i64> {
        self.vertices.end()
    }

    fn expanding(&self, step: &PyAny) -> PyResult<PyVerticesWindowSet> {
        expanding_impl(&self.vertices, step)
    }

    fn rolling(&self, window: &PyAny, step: Option<&PyAny>) -> PyResult<PyVerticesWindowSet> {
        rolling_impl(&self.vertices, window, step)
    }

    #[pyo3(signature = (t_start = None, t_end = None))]
    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyVertices {
        window_impl(&self.vertices, t_start, t_end).into()
    }

    /// Create a view of the vertices including all events at `t`.
    ///
    /// Arguments:
    ///     end (int): The time of the window.
    ///
    /// Returns:
    ///     A `PyVertices` object.
    #[pyo3(signature = (end))]
    pub fn at(&self, end: i64) -> PyVertices {
        self.vertices.at(end).into()
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

    pub fn __getitem__(&self, vertex: &PyAny) -> PyResult<PyVertex> {
        let vref = extract_vertex_ref(vertex)?;
        self.vertices.get(vref).map_or_else(
            || Err(PyIndexError::new_err("Vertex does not exist")),
            |v| Ok(v.into()),
        )
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

    fn expanding(&self, step: &PyAny) -> PyResult<PyPathFromGraphWindowSet> {
        expanding_impl(&self.path, step)
    }

    fn rolling(&self, window: &PyAny, step: Option<&PyAny>) -> PyResult<PyPathFromGraphWindowSet> {
        rolling_impl(&self.path, window, step)
    }

    #[pyo3(signature = (t_start = None, t_end = None))]
    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> Self {
        window_impl(&self.path, t_start, t_end).into()
    }

    /// Create a view of the vertex including all events at `t`.
    ///
    /// Arguments:
    ///     end (int): The time of the window.
    ///
    /// Returns:
    ///     A `PyVertex` object.
    #[pyo3(signature = (end))]
    pub fn at(&self, end: i64) -> Self {
        self.path.at(end).into()
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

impl From<PathFromGraph<DynamicGraph>> for PyPathFromGraph {
    fn from(value: PathFromGraph<DynamicGraph>) -> Self {
        Self { path: value }
    }
}

impl From<PathFromGraph<WindowedGraph<DynamicGraph>>> for PyPathFromGraph {
    fn from(value: PathFromGraph<WindowedGraph<DynamicGraph>>) -> Self {
        Self {
            path: PathFromGraph {
                graph: value.graph.into_dynamic(),
                operations: value.operations,
            },
        }
    }
}

#[pyclass(name = "PathFromVertex")]
pub struct PyPathFromVertex {
    path: PathFromVertex<DynamicGraph>,
}

impl From<PathFromVertex<DynamicGraph>> for PyPathFromVertex {
    fn from(value: PathFromVertex<DynamicGraph>) -> Self {
        Self { path: value }
    }
}

impl From<PathFromVertex<WindowedGraph<DynamicGraph>>> for PyPathFromVertex {
    fn from(value: PathFromVertex<WindowedGraph<DynamicGraph>>) -> Self {
        Self {
            path: PathFromVertex {
                graph: value.graph.into_dynamic(),
                vertex: value.vertex,
                operations: value.operations,
            },
        }
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

    fn expanding(&self, step: &PyAny) -> PyResult<PyPathFromVertexWindowSet> {
        expanding_impl(&self.path, step)
    }

    fn rolling(&self, window: &PyAny, step: Option<&PyAny>) -> PyResult<PyPathFromVertexWindowSet> {
        rolling_impl(&self.path, window, step)
    }

    #[pyo3(signature = (t_start = None, t_end = None))]
    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> Self {
        window_impl(&self.path, t_start, t_end).into()
    }

    /// Create a view of the vertex including all events at `t`.
    ///
    /// Arguments:
    ///     end (int): The time of the window.
    ///
    /// Returns:
    ///     A `PyVertex` object.
    #[pyo3(signature = (end))]
    pub fn at(&self, end: i64) -> Self {
        self.path.at(end).into()
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

#[pyclass(name = "VertexWindowSet")]
pub struct PyVertexWindowSet {
    window_set: WindowSet<VertexView<DynamicGraph>>,
}

impl From<WindowSet<VertexView<DynamicGraph>>> for PyVertexWindowSet {
    fn from(value: WindowSet<VertexView<DynamicGraph>>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyVertexWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyVertex> {
        slf.window_set.next().map(|g| g.into())
    }
}

#[pyclass(name = "VerticesWindowSet")]
pub struct PyVerticesWindowSet {
    window_set: WindowSet<Vertices<DynamicGraph>>,
}

impl From<WindowSet<Vertices<DynamicGraph>>> for PyVerticesWindowSet {
    fn from(value: WindowSet<Vertices<DynamicGraph>>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyVerticesWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyVertices> {
        slf.window_set.next().map(|g| g.into())
    }
}

#[pyclass(name = "PathFromGraphWindowSet")]
pub struct PyPathFromGraphWindowSet {
    window_set: WindowSet<PathFromGraph<DynamicGraph>>,
}

impl From<WindowSet<PathFromGraph<DynamicGraph>>> for PyPathFromGraphWindowSet {
    fn from(value: WindowSet<PathFromGraph<DynamicGraph>>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyPathFromGraphWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyPathFromGraph> {
        slf.window_set.next().map(|g| g.into())
    }
}

#[pyclass(name = "PathFromVertexWindowSet")]
pub struct PyPathFromVertexWindowSet {
    window_set: WindowSet<PathFromVertex<DynamicGraph>>,
}

impl From<WindowSet<PathFromVertex<DynamicGraph>>> for PyPathFromVertexWindowSet {
    fn from(value: WindowSet<PathFromVertex<DynamicGraph>>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyPathFromVertexWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyPathFromVertex> {
        slf.window_set.next().map(|g| g.into())
    }
}
