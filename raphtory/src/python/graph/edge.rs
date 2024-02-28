//! The edge module contains the PyEdge class, which is used to represent edges in the graph and
//! provides access to the edge's properties and nodes.
//!
//! The PyEdge class also provides access to the perspective APIs, which allow the user to view the
//! edge as it existed at a particular point in time, or as it existed over a particular time range.
//!
use crate::{
    core::{utils::errors::GraphError, ArcStr, Direction},
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{DynamicGraph, Immutable, IntoDynamic, MaterializedGraph, Static},
                StaticGraphViewOps,
            },
        },
        graph::{edge::EdgeView, views::deletion_graph::GraphWithDeletions},
    },
    prelude::*,
    python::{types::repr::Repr, utils::PyTime},
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use pyo3::{prelude::*, pyclass::CompareOp};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    ops::Deref,
};

/// PyEdge is a Python class that represents an edge in the graph.
/// An edge is a directed connection between two nodes.
#[pyclass(name = "Edge", subclass)]
#[derive(Clone)]
pub struct PyEdge {
    pub(crate) edge: EdgeView<DynamicGraph, DynamicGraph>,
}

#[pyclass(name="MutableEdge", extends=PyEdge)]
pub struct PyMutableEdge {
    edge: EdgeView<MaterializedGraph, MaterializedGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<EdgeView<G, GH>> for PyEdge
{
    fn from(value: EdgeView<G, GH>) -> Self {
        let base_graph = value.base_graph.into_dynamic();
        let graph = value.graph.into_dynamic();
        let edge = value.edge;
        Self {
            edge: EdgeView {
                base_graph,
                graph,
                edge,
            },
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic + Static>
    From<EdgeView<G, GH>> for EdgeView<DynamicGraph, DynamicGraph>
{
    fn from(value: EdgeView<G, GH>) -> Self {
        EdgeView {
            base_graph: value.base_graph.into_dynamic(),
            graph: value.graph.into_dynamic(),
            edge: value.edge,
        }
    }
}

impl<G: Into<MaterializedGraph> + StaticGraphViewOps> From<EdgeView<G, G>> for PyMutableEdge {
    fn from(value: EdgeView<G, G>) -> Self {
        let edge = EdgeView {
            edge: value.edge,
            graph: value.graph.into(),
            base_graph: value.base_graph.into(),
        };

        Self { edge }
    }
}

impl<
        G: StaticGraphViewOps + IntoDynamic + Immutable,
        GH: StaticGraphViewOps + IntoDynamic + Immutable,
    > IntoPy<PyObject> for EdgeView<G, GH>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        let py_version: PyEdge = self.into();
        py_version.into_py(py)
    }
}

impl IntoPy<PyObject> for EdgeView<Graph, Graph> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let graph: MaterializedGraph = self.graph.into();
        let base_graph: MaterializedGraph = self.base_graph.into();
        let edge = self.edge;
        EdgeView {
            graph,
            base_graph,
            edge,
        }
        .into_py(py)
    }
}

impl IntoPy<PyObject> for EdgeView<GraphWithDeletions, GraphWithDeletions> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let graph: MaterializedGraph = self.graph.into();
        let base_graph: MaterializedGraph = self.base_graph.into();
        let edge = self.edge;
        EdgeView {
            graph,
            base_graph,
            edge,
        }
        .into_py(py)
    }
}

impl IntoPy<PyObject> for EdgeView<MaterializedGraph, MaterializedGraph> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, (PyMutableEdge::from(self.clone()), PyEdge::from(self)))
            .unwrap() // I think this only fails if we are out of memory? Seems to be unavoidable!
            .into_py(py)
    }
}
impl_edgeviewops!(PyEdge, edge, EdgeView<DynamicGraph>, "Edge");

/// PyEdge is a Python class that represents an edge in the graph.
/// An edge is a directed connection between two nodes.
#[pymethods]
impl PyEdge {
    /// Rich Comparison for Node objects
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
    ///     A list of unix timestamps.
    ///
    pub fn history(&self) -> Vec<i64> {
        self.edge.history()
    }

    /// Returns a list of timestamps of when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///     A list of timestamps.
    ///
    pub fn history_date_time(&self) -> Option<Vec<DateTime<Utc>>> {
        self.edge.history_date_time()
    }

    /// Returns a list of timestamps of when an edge is deleted
    ///
    /// Returns:
    ///     A list of unix timestamps
    pub fn deletions(&self) -> Vec<i64> {
        self.edge.deletions()
    }

    /// Returns a list of timestamps of when an edge is deleted
    ///
    /// Returns:
    ///     A list of DateTime objects
    pub fn deletions_data_time(&self) -> Option<Vec<DateTime<Utc>>> {
        self.edge.deletions_date_time()
    }

    /// Check if the edge is currently valid (i.e., not deleted)
    pub fn is_valid(&self) -> bool {
        self.edge.is_valid()
    }

    /// Check if the edge is currently deleted
    pub fn is_deleted(&self) -> bool {
        self.edge.is_deleted()
    }

    /// Check if the edge is on the same node
    pub fn is_self_loop(&self) -> bool {
        self.edge.is_self_loop()
    }

    /// Returns a view of the properties of the edge.
    ///
    /// Returns:
    ///   Properties on the Edge.
    #[getter]
    pub fn properties(&self) -> Properties<EdgeView<DynamicGraph, DynamicGraph>> {
        self.edge.properties()
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
    pub fn earliest_date_time(&self) -> Option<DateTime<Utc>> {
        self.edge.earliest_date_time()
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
    pub fn latest_date_time(&self) -> Option<DateTime<Utc>> {
        self.edge.latest_date_time()
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
    pub fn date_time(&self) -> Option<DateTime<Utc>> {
        self.edge.date_time()
    }
}

impl Repr for PyEdge {
    fn repr(&self) -> String {
        self.edge.repr()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Repr for EdgeView<G, GH> {
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
