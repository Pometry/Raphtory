//! The edge module contains the PyEdge class, which is used to represent edges in the graph and
//! provides access to the edge's properties and nodes.
//!
//! The PyEdge class also provides access to the perspective APIs, which allow the user to view the
//! edge as it existed at a particular point in time, or as it existed over a particular time range.
//!
use crate::{
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{DynamicGraph, Immutable, IntoDynamic, MaterializedGraph, Static},
                StaticGraphViewOps,
            },
        },
        graph::{edge::EdgeView, views::deletion_graph::PersistentGraph},
    },
    errors::GraphError,
    prelude::*,
    python::{types::repr::Repr, utils::PyTime},
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use numpy::{IntoPyArray, Ix1, PyArray};
use pyo3::prelude::*;
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    ops::Deref,
};

/// PyEdge is a Python class that represents an edge in the graph.
/// An edge is a directed connection between two nodes.
#[pyclass(name = "Edge", subclass, module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyEdge {
    pub edge: EdgeView<DynamicGraph>,
}

#[pyclass(name="MutableEdge", extends=PyEdge, module="raphtory", frozen)]
pub struct PyMutableEdge {
    pub edge: EdgeView<MaterializedGraph>,
}

impl PyMutableEdge {
    fn new_bound<G: Into<MaterializedGraph> + StaticGraphViewOps + Static>(
        edge: EdgeView<G>,
        py: Python,
    ) -> PyResult<Bound<Self>> {
        Bound::new(py, (PyMutableEdge::from(edge.clone()), PyEdge::from(edge)))
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> From<EdgeView<G>> for PyEdge {
    fn from(value: EdgeView<G>) -> Self {
        let graph = value.graph.into_dynamic();
        let edge = value.edge;
        Self {
            edge: EdgeView { graph, edge },
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic + Static> From<EdgeView<G>> for EdgeView<DynamicGraph> {
    fn from(value: EdgeView<G>) -> Self {
        EdgeView {
            graph: value.graph.into_dynamic(),
            edge: value.edge,
        }
    }
}

impl<'py> IntoPyObject<'py> for EdgeView<&DynamicGraph> {
    type Target = PyEdge;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.cloned().into_pyobject(py)
    }
}

impl<G: Into<MaterializedGraph> + StaticGraphViewOps> From<EdgeView<G>> for PyMutableEdge {
    fn from(value: EdgeView<G>) -> Self {
        let edge = EdgeView {
            graph: value.graph.into(),
            edge: value.edge,
        };

        Self { edge }
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic + Immutable> IntoPyObject<'py> for EdgeView<G> {
    type Target = PyEdge;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyEdge::from(self).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for EdgeView<Graph> {
    type Target = PyMutableEdge;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyMutableEdge::new_bound(self, py)
    }
}

impl<'py> IntoPyObject<'py> for EdgeView<PersistentGraph> {
    type Target = PyMutableEdge;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyMutableEdge::new_bound(self, py)
    }
}

impl<'py> IntoPyObject<'py> for EdgeView<MaterializedGraph> {
    type Target = PyMutableEdge;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(py, (PyMutableEdge::from(self.clone()), PyEdge::from(self)))
    }
}
impl_edgeviewops!(PyEdge, edge, EdgeView<DynamicGraph>, "Edge");

/// PyEdge is a Python class that represents an edge in the graph.
/// An edge is a directed connection between two nodes.
#[pymethods]
impl PyEdge {
    fn __eq__(&self, other: Bound<PyEdge>) -> bool {
        self.edge == other.get().edge
    }

    fn __ne__(&self, other: Bound<PyEdge>) -> bool {
        self.edge != other.get().edge
    }

    fn __lt__(&self, other: Bound<PyEdge>) -> bool {
        self.edge < other.get().edge
    }

    fn __le__(&self, other: Bound<PyEdge>) -> bool {
        self.edge <= other.get().edge
    }

    fn __gt__(&self, other: Bound<PyEdge>) -> bool {
        self.edge > other.get().edge
    }

    fn __ge__(&self, other: Bound<PyEdge>) -> bool {
        self.edge >= other.get().edge
    }

    /// Returns the hash of the edge and edge properties.
    ///
    /// Returns:
    ///   int: A hash of the edge.
    pub fn __hash__(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.edge.hash(&mut s);
        s.finish()
    }

    /// The id of the edge.
    #[getter]
    pub fn id(&self) -> (GID, GID) {
        self.edge.id()
    }

    pub fn __getitem__(&self, name: &str) -> Option<Prop> {
        self.edge.properties().get(name)
    }

    /// Returns a list of timestamps of when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///    List[int]:  A list of unix timestamps.
    ///
    pub fn history<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray<i64, Ix1>> {
        let history = self.edge.history();
        history.into_pyarray(py)
    }

    /// Returns the number of times an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///    int: The number of times an edge is added or change to an edge is made.
    ///
    pub fn history_counts(&self) -> usize {
        self.edge.history_counts()
    }

    /// Returns a list of timestamps of when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///     List[datetime]
    ///
    pub fn history_date_time(&self) -> Option<Vec<DateTime<Utc>>> {
        self.edge.history_date_time()
    }

    /// Returns a list of timestamps of when an edge is deleted
    ///
    /// Returns:
    ///     List[int]: A list of unix timestamps
    pub fn deletions(&self) -> Vec<i64> {
        self.edge.deletions()
    }

    /// Returns a list of timestamps of when an edge is deleted
    ///
    /// Returns:
    ///     List[datetime]
    pub fn deletions_data_time(&self) -> Option<Vec<DateTime<Utc>>> {
        self.edge.deletions_date_time()
    }

    /// Check if the edge is currently valid (i.e., not deleted)
    /// Returns:
    ///     bool:
    pub fn is_valid(&self) -> bool {
        self.edge.is_valid()
    }

    /// Check if the edge is currently active (i.e., has at least one update within this period)
    /// Returns:
    ///     bool:
    pub fn is_active(&self) -> bool {
        self.edge.is_active()
    }

    /// Check if the edge is currently deleted
    /// Returns:
    ///     bool:
    pub fn is_deleted(&self) -> bool {
        self.edge.is_deleted()
    }

    /// Check if the edge is on the same node
    /// Returns:
    ///     bool:
    pub fn is_self_loop(&self) -> bool {
        self.edge.is_self_loop()
    }

    /// Returns a view of the properties of the edge.
    ///
    /// Returns:
    ///   Properties on the Edge.
    #[getter]
    pub fn properties(&self) -> Properties<EdgeView<DynamicGraph>> {
        self.edge.properties()
    }

    /// Gets the earliest time of an edge.
    ///
    /// Returns:
    ///     int: The earliest time of an edge
    #[getter]
    pub fn earliest_time(&self) -> Option<i64> {
        self.edge.earliest_time()
    }

    /// Gets of earliest datetime of an edge.
    ///
    /// Returns:
    ///     datetime: the earliest datetime of an edge
    #[getter]
    pub fn earliest_date_time(&self) -> Option<DateTime<Utc>> {
        self.edge.earliest_date_time()
    }

    /// Gets the latest time of an edge.
    ///
    /// Returns:
    ///     int: The latest time of an edge
    #[getter]
    pub fn latest_time(&self) -> Option<i64> {
        self.edge.latest_time()
    }

    /// Gets of latest datetime of an edge.
    ///
    /// Returns:
    ///     datetime: the latest datetime of an edge
    #[getter]
    pub fn latest_date_time(&self) -> Option<DateTime<Utc>> {
        self.edge.latest_date_time()
    }

    /// Gets the time of an exploded edge.
    ///
    /// Returns:
    ///     int: The time of an exploded edge
    #[getter]
    pub fn time(&self) -> Result<i64, GraphError> {
        self.edge.time()
    }

    /// Gets the names of the layers this edge belongs to
    ///
    /// Returns:
    ///     List[str]-  The name of the layer
    #[getter]
    pub fn layer_names(&self) -> Vec<ArcStr> {
        self.edge.layer_names()
    }

    /// Gets the name of the layer this edge belongs to - assuming it only belongs to one layer
    ///
    /// Returns:
    ///     str: The name of the layer
    #[getter]
    pub fn layer_name(&self) -> Result<ArcStr, GraphError> {
        self.edge.layer_name()
    }

    /// Gets the datetime of an exploded edge.
    ///
    /// Returns:
    ///     datetime: the datetime of an exploded edge
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

impl<'graph, G: GraphViewOps<'graph>> Repr for EdgeView<G> {
    fn repr(&self) -> String {
        let properties: String = self
            .properties()
            .iter()
            .map(|(k, v)| format!("{}: {}", k.deref(), v.repr()))
            .join(", ");

        let source = self.src().name();
        let target = self.dst().name();
        let earliest_time = self.earliest_time().repr();
        let latest_time = self.latest_time().repr();
        let layer_names = self.layer_names().into_iter().take(11).collect_vec();
        let layer_names_prev = if layer_names.len() < 11 {
            layer_names.join(", ")
        } else {
            layer_names[0..10].join(",") + ", ..."
        };

        if properties.is_empty() {
            format!(
                "Edge(source={}, target={}, earliest_time={}, latest_time={}, layer(s)=[{}])",
                source.trim_matches('"'),
                target.trim_matches('"'),
                earliest_time,
                latest_time,
                layer_names_prev
            )
        } else {
            format!(
                "Edge(source={}, target={}, earliest_time={}, latest_time={}, properties={{{}}}, layer(s)=[{}])",
                source.trim_matches('"'),
                target.trim_matches('"'),
                earliest_time,
                latest_time,
                properties,
                layer_names_prev
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
    /// Add updates to an edge in the graph at a specified time.
    /// This function allows for the addition of property updates to an edge within the graph. The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Parameters:
    ///    t (TimeInput): The timestamp at which the updates should be applied.
    ///    properties (PropInput, optional): A dictionary of properties to update.
    ///    layer (str, optional): The layer you want these properties to be added on to.
    ///    secondary_index (int, optional): The optional integer which will be used as a secondary index
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (t, properties=None, layer=None, secondary_index=None))]
    fn add_updates(
        &self,
        t: PyTime,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        secondary_index: Option<usize>,
    ) -> Result<(), GraphError> {
        match secondary_index {
            None => self
                .edge
                .add_updates(t, properties.unwrap_or_default(), layer),
            Some(secondary_index) => {
                self.edge
                    .add_updates((t, secondary_index), properties.unwrap_or_default(), layer)
            }
        }
    }

    /// Mark the edge as deleted at the specified time.
    ///
    /// Parameters:
    ///     t (TimeInput): The timestamp at which the deletion should be applied.
    ///     layer (str, optional): The layer you want the deletion applied to .
    #[pyo3(signature = (t, layer=None))]
    fn delete(&self, t: PyTime, layer: Option<&str>) -> Result<(), GraphError> {
        self.edge.delete(t, layer)
    }

    /// Add constant properties to an edge in the graph.
    /// This function is used to add properties to an edge that remain constant and do not
    /// change over time. These properties are fundamental attributes of the edge.
    ///
    /// Parameters:
    ///     properties (PropInput): A dictionary of properties to be added to the edge.
    ///     layer (str, optional): The layer you want these properties to be added on to.
    #[pyo3(signature = (properties, layer=None))]
    fn add_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.edge.add_constant_properties(properties, layer)
    }

    /// Update constant properties of an edge in the graph overwriting existing values.
    /// This function is used to add properties to an edge that remains constant and does not
    /// change over time. These properties are fundamental attributes of the edge.
    ///
    /// Parameters:
    ///     properties (PropInput): A dictionary of properties to be added to the edge.
    ///     layer (str, optional): The layer you want these properties to be added on to.
    #[pyo3(signature = (properties, layer=None))]
    pub fn update_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.edge.update_constant_properties(properties, layer)
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}
