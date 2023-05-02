//! The edge module contains the PyEdge class, which is used to represent edges in the graph and
//! provides access to the edge's properties and vertices.
//!
//! The PyEdge class also provides access to the perspective APIs, which allow the user to view the
//! edge as it existed at a particular point in time, or as it existed over a particular time range.
//!
use crate::dynamic::{DynamicGraph, IntoDynamic};
use crate::types::repr::{iterator_repr, Repr};
use crate::utils::*;
use crate::vertex::PyVertex;
use crate::wrappers::prop::Prop;
use itertools::Itertools;
use pyo3::{pyclass, pymethods, PyAny, PyRef, PyRefMut, PyResult};
use raphtory::db::edge::EdgeView;
use raphtory::db::graph_window::WindowedGraph;
use raphtory::db::view_api::time::WindowSet;
use raphtory::db::view_api::*;
use std::collections::HashMap;
use std::sync::Arc;

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
                graph: value.graph.into_dynamic(),
                edge: value.edge,
            },
        }
    }
}

/// PyEdge is a Python class that represents an edge in the graph.
/// An edge is a directed connection between two vertices.
#[pymethods]
impl PyEdge {
    pub fn __getitem__(&self, name: String) -> Option<Prop> {
        self.property(name, Some(true))
    }

    /// Returns the value of the property with the given name.
    /// If the property is not found, None is returned.
    /// If the property is found, the value of the property is returned.
    ///
    /// Arguments:
    ///    name (str): The name of the property to retrieve.
    ///
    /// Returns:
    ///   The value of the property with the given name.
    #[pyo3(signature = (name, include_static = true))]
    pub fn property(&self, name: String, include_static: Option<bool>) -> Option<Prop> {
        let include_static = include_static.unwrap_or(true);
        self.edge
            .property(name, include_static)
            .map(|prop| prop.into())
    }

    /// Returns the value of the property with the given name all times.
    /// If the property is not found, None is returned.
    /// If the property is found, the value of the property is returned.
    ///
    /// Arguments:
    ///   name (str): The name of the property to retrieve.
    ///
    /// Returns:
    ///  The value of the property with the given name.
    #[pyo3(signature = (name))]
    pub fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        self.edge
            .property_history(name)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    /// Returns a list of timestamps of when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///     A list of timestamps.
    ///

    pub fn history(&self) -> Vec<i64> {
        self.edge.history()
    }

    /// Returns a dictionary of all properties on the edge.
    ///
    /// Arguments:
    ///  include_static (bool): Whether to include static properties in the result.
    ///
    /// Returns:
    ///   A dictionary of all properties on the edge.
    #[pyo3(signature = (include_static = true))]
    pub fn properties(&self, include_static: Option<bool>) -> HashMap<String, Prop> {
        let include_static = include_static.unwrap_or(true);
        self.edge
            .properties(include_static)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    /// Returns a dictionary of all properties on the edge at all times.
    ///
    /// Returns:
    ///   A dictionary of all properties on the edge at all times.
    pub fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.edge
            .property_histories()
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(|(t, p)| (t, p.into())).collect()))
            .collect()
    }

    /// Returns a list of all property names on the edge.
    ///
    /// Arguments:
    ///   include_static (bool): Whether to include static properties in the result.
    ///
    /// Returns:
    ///   A list of all property names on the edge.
    #[pyo3(signature = (include_static = true))]
    pub fn property_names(&self, include_static: Option<bool>) -> Vec<String> {
        let include_static = include_static.unwrap_or(true);
        self.edge.property_names(include_static)
    }

    /// Check if a property exists with the given name.
    ///
    /// Arguments:
    ///  name (str): The name of the property to check.
    ///  include_static (bool): Whether to include static properties in the result.
    ///
    /// Returns:
    /// True if a property exists with the given name, False otherwise.
    #[pyo3(signature = (name, include_static = true))]
    pub fn has_property(&self, name: String, include_static: Option<bool>) -> bool {
        let include_static = include_static.unwrap_or(true);
        self.edge.has_property(name, include_static)
    }

    /// Check if a static property exists with the given name.
    ///
    /// Arguments:
    ///   name (str): The name of the property to check.
    ///
    /// Returns:
    ///   True if a static property exists with the given name, False otherwise.
    pub fn has_static_property(&self, name: String) -> bool {
        self.edge.has_static_property(name)
    }

    pub fn static_property(&self, name: String) -> Option<Prop> {
        self.edge.static_property(name).map(|prop| prop.into())
    }

    /// Get the id of the Edge.
    ///
    /// Returns:
    ///   The id of the Edge.
    pub fn id(&self) -> usize {
        self.edge.id()
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

    /// Get the end time of the Edge.
    ///
    /// Returns:
    ///   The end time of the Edge.
    pub fn end(&self) -> Option<i64> {
        self.edge.end()
    }

    /// Get the duration of the Edge.
    ///
    /// Arguments:
    ///   step (int): The step size to use when calculating the duration.
    ///
    /// Returns:
    ///   A set of windows containing edges that fall in the time period
    #[pyo3(signature = (step))]
    fn expanding(&self, step: &PyAny) -> PyResult<PyEdgeWindowSet> {
        expanding_impl(&self.edge, step)
    }

    /// Get a set of Edge windows for a given window size, step, start time
    /// and end time using rolling window.
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    ///
    /// Arguments:
    ///   window (int): The size of the window.
    ///   step (int): The step size to use when calculating the duration.
    ///   start (int): The start time to use when calculating the duration.
    ///   end (int): The end time to use when calculating the duration.
    ///
    /// Returns:
    ///   A set of windows containing edges that fall in the time period
    fn rolling(&self, window: &PyAny, step: Option<&PyAny>) -> PyResult<PyEdgeWindowSet> {
        rolling_impl(&self.edge, window, step)
    }

    /// Get a new Edge with the properties of this Edge within the specified time window.
    ///
    /// Arguments:
    ///   t_start (int): The start time of the window.
    ///   t_end (int): The end time of the window.
    ///
    /// Returns:
    ///   A new Edge with the properties of this Edge within the specified time window.
    #[pyo3(signature = (t_start = None, t_end = None))]
    pub fn window(&self, t_start: Option<&PyAny>, t_end: Option<&PyAny>) -> PyResult<PyEdge> {
        window_impl(&self.edge, t_start, t_end).map(|e| e.into())
    }

    /// Get a new Edge with the properties of this Edge at a specified time.
    ///
    /// Arguments:
    ///   end (int): The time to get the properties at.
    ///
    /// Returns:
    ///   A new Edge with the properties of this Edge at a specified time.
    #[pyo3(signature = (end))]
    pub fn at(&self, end: &PyAny) -> PyResult<PyEdge> {
        at_impl(&self.edge, end).map(|e| e.into())
    }

    pub fn default_layer(&self) -> PyEdge {
        self.edge.default_layer().into()
    }

    pub fn layer(&self, name: &str) -> Option<PyEdge> {
        Some(self.edge.layer(name)?.into())
    }

    /// Explodes an Edge into a list of PyEdges. This is useful when you want to iterate over
    /// the properties of an Edge at every single point in time. This will return a seperate edge
    /// each time a property had been changed.
    ///
    /// Returns:
    ///     A list of PyEdges
    pub fn explode(&self) -> Vec<PyEdge> {
        self.edge
            .explode()
            .into_iter()
            .map(|e| e.into())
            .collect::<Vec<PyEdge>>()
    }

    /// Gets the earliest time of an edge.
    ///
    /// Returns:
    ///     (int) The earliest time of an edge
    pub fn earliest_time(&self) -> Option<i64> {
        self.edge.earliest_time()
    }

    /// Gets the latest time of an edge.
    ///
    /// Returns:
    ///     (int) The latest time of an edge
    pub fn latest_time(&self) -> Option<i64> {
        self.edge.latest_time()
    }

    /// Gets the time of an exploded edge.
    ///
    /// Returns:
    ///     (int) The time of an exploded edge
    pub fn time(&self) -> Option<i64> {
        self.edge.time()
    }

    /// Displays the Edge as a string.
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyEdge {
    fn repr(&self) -> String {
        let properties = &self
            .properties(Some(true))
            .iter()
            .map(|(k, v)| k.to_string() + " : " + &v.to_string())
            .join(", ");

        let source = self.edge.src().name();
        let target = self.edge.dst().name();
        let earliest_time = self.edge.earliest_time();
        let latest_time = self.edge.latest_time();
        if properties.is_empty() {
            format!(
                "Edge(source={}, target={}, earliest_time={}, latest_time={})",
                source.trim_matches('"'),
                target.trim_matches('"'),
                earliest_time.unwrap_or(0),
                latest_time.unwrap_or(0),
            )
        } else {
            let property_string: String = "{".to_string() + &properties + "}";
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

py_iterator!(PyEdgeIter, EdgeView<DynamicGraph>, PyEdge, "EdgeIter");

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
    fn __iter__(&self) -> PyEdgeIter {
        PyEdgeIter {
            iter: Box::new(self.py_iter()),
        }
    }

    fn __len__(&self) -> usize {
        self.py_iter().count()
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
    fn earliest_time(&self) -> Vec<Option<i64>> {
        self.py_iter().map(|e| e.earliest_time()).collect()
    }

    /// Returns the latest time of the edges.
    fn latest_time(&self) -> Vec<Option<i64>> {
        self.py_iter().map(|e| e.latest_time()).collect()
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyEdges {
    fn repr(&self) -> String {
        format!("Edges({})", iterator_repr(self.__iter__().into_iter()))
    }
}

impl<F: Fn() -> BoxedIter<EdgeView<DynamicGraph>> + Send + Sync + 'static> From<F> for PyEdges {
    fn from(value: F) -> Self {
        Self {
            builder: Arc::new(value),
        }
    }
}

py_iterator!(
    PyNestedEdgeIter,
    BoxedIter<EdgeView<DynamicGraph>>,
    PyEdgeIter,
    "NestedEdgeIter"
);

#[pyclass(name = "EdgeWindowSet")]
pub struct PyEdgeWindowSet {
    window_set: WindowSet<EdgeView<DynamicGraph>>,
}

impl From<WindowSet<EdgeView<DynamicGraph>>> for PyEdgeWindowSet {
    fn from(value: WindowSet<EdgeView<DynamicGraph>>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyEdgeWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyEdge> {
        slf.window_set.next().map(|g| g.into())
    }
}

#[pyclass(name = "NestedEdges")]
pub struct PyNestedEdges {
    builder: Arc<dyn Fn() -> BoxedIter<BoxedIter<EdgeView<DynamicGraph>>> + Send + Sync + 'static>,
}

impl PyNestedEdges {
    fn iter(&self) -> BoxedIter<BoxedIter<EdgeView<DynamicGraph>>> {
        (self.builder)()
    }
}

#[pymethods]
impl PyNestedEdges {
    fn __iter__(&self) -> PyNestedEdgeIter {
        self.iter().into()
    }

    fn collect(&self) -> Vec<Vec<PyEdge>> {
        self.iter()
            .map(|e| e.map(|ee| ee.into()).collect())
            .collect()
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

impl<F: Fn() -> BoxedIter<BoxedIter<EdgeView<DynamicGraph>>> + Send + Sync + 'static> From<F>
    for PyNestedEdges
{
    fn from(value: F) -> Self {
        Self {
            builder: Arc::new(value),
        }
    }
}
