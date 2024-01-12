use crate::{
    db::{
        api::view::{BoxedIter, DynamicGraph, IntoDynBoxed, IntoDynamic, StaticGraphViewOps},
        graph::{edge::EdgeView, edges::Edges},
    },
    prelude::{EdgeViewOps, LayerOps, TimeOps},
    python::{
        graph::{edge::PyEdge, properties::PyPropsList},
        types::{
            repr::{iterator_repr, Repr},
            wrappers::iterators::{
                ArcStringVecIterable, BoolIterable, I64VecIterable, OptionArcStringIterable,
                OptionI64Iterable, OptionUtcDateTimeIterable, OptionVecUtcDateTimeIterable,
                U64U64Iterable,
            },
        },
        utils::{PyGenericIterator, PyTime},
    },
};
use itertools::Itertools;
use pyo3::{pyclass, pymethods, IntoPy, PyObject, Python};

/// A list of edges that can be iterated over.
#[pyclass(name = "Edges")]
pub struct PyEdges {
    edges: Edges<'static, DynamicGraph>,
}

impl_edgeviewops!(PyEdges, edges, Edges<'static, DynamicGraph>, "Edges");

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic> IntoPy<PyObject>
    for Edges<'static, G, GH>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        let graph = self.graph.into_dynamic();
        let base_graph = self.base_graph.into_dynamic();
        let edges = self.edges;
        PyEdges {
            edges: Edges {
                base_graph,
                graph,
                edges,
            },
        }
        .into_py(py)
    }
}

impl PyEdges {
    /// an iterable that can be used in rust
    fn iter(&self) -> BoxedIter<EdgeView<DynamicGraph, DynamicGraph>> {
        self.edges.iter().into_dyn_boxed()
    }

    /// returns an iterable used in python
    fn py_iter(&self) -> BoxedIter<PyEdge> {
        Box::new(self.iter().map(|e| e.into()))
    }
}

#[pymethods]
impl PyEdges {
    fn __iter__(&self) -> PyGenericIterator {
        self.edges.iter().into()
    }

    /// Returns all edges as a list
    fn collect(&self) -> Vec<PyEdge> {
        self.py_iter().collect()
    }

    /// Returns the number of edges
    fn count(&self) -> usize {
        self.iter().count()
    }

    /// Returns the earliest time of the edges.
    ///
    /// Returns:
    /// Earliest time of the edges.
    #[getter]
    fn earliest_time(&self) -> OptionI64Iterable {
        let edges = self.edges.clone();
        (move || edges.earliest_time()).into()
    }

    /// Returns the earliest date time of the edges.
    ///
    /// Returns:
    ///  Earliest date time of the edges.
    #[getter]
    fn earliest_date_time(&self) -> OptionUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.earliest_date_time()).into()
    }

    /// Returns the latest time of the edges.
    ///
    /// Returns:
    ///  Latest time of the edges.
    #[getter]
    fn latest_time(&self) -> OptionI64Iterable {
        let edges = self.edges.clone();
        (move || edges.latest_time()).into()
    }

    /// Returns the latest date time of the edges.
    ///
    /// Returns:
    ///   Latest date time of the edges.
    #[getter]
    fn latest_date_time(&self) -> OptionUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.latest_date_time()).into()
    }

    /// Returns the date times of exploded edges
    ///
    /// Returns:
    ///    A list of date times.
    #[getter]
    fn date_time(&self) -> OptionUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.date_time()).into()
    }

    /// Returns the times of exploded edges
    ///
    /// Returns:
    ///   Time of edge
    #[getter]
    fn time(&self) -> OptionI64Iterable {
        let edges = self.edges.clone();
        (move || edges.time()).into()
    }

    /// Returns all properties of the edges
    #[getter]
    fn properties(&self) -> PyPropsList {
        let edges = self.edges.clone();
        (move || edges.properties()).into()
    }

    /// Returns all ids of the edges.
    #[getter]
    fn id(&self) -> U64U64Iterable {
        let edges = self.edges.clone();
        (move || edges.id()).into()
    }

    /// Returns all timestamps of edges, when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///    A list of lists unix timestamps.
    ///
    fn history(&self) -> I64VecIterable {
        let edges = self.edges.clone();
        (move || edges.history()).into()
    }

    /// Returns all timestamps of edges, when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///    A list of lists of timestamps.
    ///
    fn history_date_time(&self) -> OptionVecUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.history_date_time()).into()
    }

    /// Returns all timestamps of edges where an edge is deleted
    ///
    /// Returns:
    ///     A list of lists of unix timestamps
    fn deletions(&self) -> I64VecIterable {
        let edges = self.edges.clone();
        (move || edges.deletions()).into()
    }

    /// Returns all timestamps of edges where an edge is deleted
    ///
    /// Returns:
    ///     A list of lists of DateTime objects
    fn deletions_date_time(&self) -> OptionVecUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.deletions_date_time()).into()
    }

    /// Check if the edges are valid (i.e. not deleted)
    fn is_valid(&self) -> BoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_valid()).into()
    }

    /// Check if the edges are deleted
    fn is_deleted(&self) -> BoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_deleted()).into()
    }

    /// Get the layer name that all edges belong to - assuming they only belong to one layer
    ///
    /// Returns:
    ///  The name of the layer
    #[getter]
    fn layer_name(&self) -> OptionArcStringIterable {
        let edges = self.edges.clone();
        (move || edges.layer_name()).into()
    }

    /// Get the layer names that all edges belong to - assuming they only belong to one layer
    ///
    /// Returns:
    ///   A list of layer names
    #[getter]
    fn layer_names(&self) -> ArcStringVecIterable {
        let edges = self.edges.clone();
        (move || edges.layer_names().map(|e| e.collect_vec())).into()
    }

    fn __repr__(&self) -> String {
        self.repr()
    }

    fn __len__(&self) -> usize {
        self.count()
    }
}

impl Repr for PyEdges {
    fn repr(&self) -> String {
        format!("Edges({})", iterator_repr(self.iter()))
    }
}
