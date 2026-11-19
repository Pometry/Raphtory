use crate::{
    db::{
        api::view::{
            DynamicGraph, IntoDynBoxed, IntoDynHop, IntoDynamic, Select, StaticGraphViewOps,
        },
        graph::{
            edge::EdgeView,
            edges::{DynEdgeItem, EdgeItem, Edges, NestedEdges},
        },
    },
    errors::GraphError,
    export::edge_updates,
    prelude::*,
    python::{
        filter::filter_expr::PyFilterExpr,
        graph::{
            history::{HistoryIterable, NestedHistoryIterable},
            properties::{MetadataListList, MetadataView, PropertiesView, PyNestedPropsIterable},
            sorting::PyEdgeSortBy,
        },
        types::{
            repr::{iterator_repr, Repr},
            wrappers::iterables::{
                ArcStringIterable, ArcStringVecIterable, BoolIterable, EventTimeIterable,
                GIDGIDIterable, NestedArcStringIterable, NestedArcStringVecIterable,
                NestedBoolIterable, NestedEventTimeIterable, NestedGIDGIDIterable,
                NestedOptionEventTimeIterable, OptionEventTimeIterable,
            },
        },
    },
};
use pyo3::{prelude::*, types::PyDict, PyAny};
use pyo3_arrow::PyTable;

/// A list of edges that can be iterated over.
#[pyclass(name = "Edges", module = "raphtory", frozen)]
pub struct PyEdges {
    edges: Edges<'static, DynamicGraph, DynEdgeItem>,
}

impl_edgeviewops!(
    PyEdges,
    edges,
    Edges<'static, DynamicGraph, DynEdgeItem>,
    "Edges",
    "Edges"
);
impl_iterable_mixin!(
    PyEdges,
    edges,
    Vec<EdgeView<DynamicGraph>>,
    "list[Edge]",
    "edge",
    |edges: &Edges<'static, DynamicGraph, DynEdgeItem>| edges.clone().into_iter()
);

impl<'graph, G: GraphViewOps<'graph>, K: EdgeItem> Repr for Edges<'graph, G, K> {
    fn repr(&self) -> String {
        format!("Edges({})", iterator_repr(self.iter()))
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic, K: EdgeItem> IntoPyObject<'py>
    for Edges<'static, G, K>
where
    Edges<'static, DynamicGraph, DynEdgeItem>: From<Edges<'static, DynamicGraph, K>>,
{
    type Target = PyEdges;
    type Output = Bound<'py, PyEdges>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyEdges::from(self).into_pyobject(py)
    }
}

impl<G: StaticGraphViewOps + IntoDynamic, K: EdgeItem> From<Edges<'static, G, K>> for PyEdges
where
    Edges<'static, DynamicGraph, DynEdgeItem>: From<Edges<'static, DynamicGraph, K>>,
{
    fn from(value: Edges<'static, G, K>) -> Self {
        Self {
            edges: value.into_dyn().into(),
        }
    }
}

impl PyEdges {
    /// The edge update table of these edges as a pyarrow Table.
    fn update_table<'py>(
        &self,
        py: Python<'py>,
        include_property_history: bool,
        convert_datetime: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let updates =
            py.detach(|| edge_updates(&self.edges, !include_property_history, convert_datetime))?;
        PyTable::try_new(updates.batches, updates.schema)?.into_pyarrow(py)
    }
}

#[pymethods]
impl PyEdges {
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyEdges> {
        let r = self.edges.select(filter)?;
        Ok(PyEdges::from(r))
    }

    /// Reorder this collection by an ordered list of sort keys. Multi-key
    /// sort is lexicographic (ties on key 1 break to key 2).
    ///
    /// Arguments:
    ///     sort_bys (list[EdgeSortBy]): the ordered sort keys.
    ///
    /// Returns:
    ///     Edges: a new collection in the sorted order.
    fn sorted(&self, sort_bys: Vec<PyEdgeSortBy>) -> PyEdges {
        let sort_bys: Vec<_> = sort_bys.into_iter().map(|s| s.inner).collect();
        PyEdges::from(self.edges.sorted(&sort_bys))
    }

    /// Returns the earliest time of the edges.
    ///
    /// Returns:
    ///     OptionEventTimeIterable: Iterable of `EventTime`s.
    #[getter]
    fn earliest_time(&self) -> OptionEventTimeIterable {
        let edges = self.edges.clone();
        (move || edges.earliest_time()).into()
    }

    /// Returns the latest times of the edges.
    ///
    /// Returns:
    ///     OptionEventTimeIterable: Iterable of `EventTime`s.
    #[getter]
    fn latest_time(&self) -> OptionEventTimeIterable {
        let edges = self.edges.clone();
        (move || edges.latest_time()).into()
    }

    /// Returns the times of exploded edges
    ///
    /// Returns:
    ///   EventTimeIterable: Iterable of `EventTime`s.
    #[getter]
    fn time(&self) -> Result<EventTimeIterable, GraphError> {
        match self.edges.time().next() {
            Some(Err(err)) => Err(err),
            _ => {
                let edges = self.edges.clone();
                Ok((move || edges.time().map(|t| t.unwrap())).into())
            }
        }
    }

    /// Returns all properties of the edges
    ///
    /// Returns:
    ///     PropertiesView:
    #[getter]
    fn properties(&self) -> PropertiesView {
        let edges = self.edges.clone();
        (move || edges.properties()).into()
    }

    /// Returns all the metadata of the edges
    ///
    /// Returns:
    ///     MetadataView:
    #[getter]
    fn metadata(&self) -> MetadataView {
        let edges = self.edges.clone();
        (move || edges.metadata()).into()
    }

    /// Returns all ids of the edges.
    ///
    /// Returns:
    ///     GIDGIDIterable:
    #[getter]
    fn id(&self) -> GIDGIDIterable {
        let edges = self.edges.clone();
        (move || edges.id()).into()
    }

    /// Returns a history object for each edge containing time entries for when the edge is added or change to the edge is made.
    ///
    /// Returns:
    ///    HistoryIterable: An iterable of history objects, one for each edge.
    #[getter]
    fn history(&self) -> HistoryIterable {
        let edges = self.edges.clone();
        (move || edges.history().map(|history| history.into_arc_dyn())).into()
    }

    /// Returns a history object for each edge containing their deletion times.
    ///
    /// Returns:
    ///    HistoryIterable: An iterable of history objects, one for each edge.
    #[getter]
    fn deletions(&self) -> HistoryIterable {
        let edges = self.edges.clone();
        (move || edges.deletions().map(|history| history.into_arc_dyn())).into()
    }

    /// Check if the edges are valid (i.e. not deleted).
    ///
    /// Returns:
    ///     BoolIterable:
    fn is_valid(&self) -> BoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_valid()).into()
    }

    /// Check if the edges are active (there is at least one update during this time).
    ///
    /// Returns:
    ///     BoolIterable:
    fn is_active(&self) -> BoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_active()).into()
    }

    /// Check if the edges are on the same node.
    ///
    /// Returns:
    ///     BoolIterable:
    fn is_self_loop(&self) -> BoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_self_loop()).into()
    }

    /// Check if the edges are deleted.
    ///
    /// Returns:
    ///     BoolIterable:
    fn is_deleted(&self) -> BoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_deleted()).into()
    }

    /// Get the layer name that all edges belong to - assuming they only belong to one layer
    ///
    /// Returns:
    ///  ArcStringIterable:
    #[getter]
    fn layer_name(&self) -> Result<ArcStringIterable, GraphError> {
        match self.edges.layer_name().next() {
            Some(Err(err)) => Err(err),
            _ => {
                let edges = self.edges.clone();
                Ok((move || edges.layer_name().map(|layer| layer.unwrap())).into())
            }
        }
    }

    /// Get the layer names that all edges belong to - assuming they only belong to one layer.
    ///
    /// Returns:
    ///   ArcStringVecIterable:
    #[getter]
    fn layer_names(&self) -> ArcStringVecIterable {
        let edges = self.edges.clone();
        (move || edges.layer_names()).into()
    }

    /// Converts the edges into a pandas DataFrame of their updates, with one row per update.
    ///
    /// The DataFrame has the following columns:
    /// - "rap_time": the time of the update, in milliseconds since the epoch.
    /// - "rap_secondary_index": the event id of the update, which orders updates made at the same timestamp.
    /// - "rap_src_gid": the id of the source node, an integer or a string depending on the graph.
    /// - "rap_dst_gid": the id of the destination node, an integer or a string depending on the graph.
    /// - "rap_layer": the layer of the update.
    ///
    /// These are followed by one column per temporal property. A cell is empty when the update did
    /// not set that property. By default, only the updates that hold the latest value of a property are included.
    ///
    /// Columns use pyarrow-backed dtypes, so integer columns with empty cells keep their type.
    ///
    /// Args:
    ///     include_property_history (bool): If True, include every update instead of only the updates that hold a property's latest value. Defaults to False.
    ///     convert_datetime (bool): If True, the "rap_time" column holds UTC datetimes instead of milliseconds. Defaults to False.
    ///
    /// Returns:
    ///     DataFrame: the edge updates as a pandas DataFrame.
    #[pyo3(signature = (include_property_history = false, convert_datetime = false))]
    pub fn to_df<'py>(
        &self,
        py: Python<'py>,
        include_property_history: bool,
        convert_datetime: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let updates = self.update_table(py, include_property_history, convert_datetime)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("types_mapper", py.import("pandas")?.getattr("ArrowDtype")?)?;
        updates.call_method("to_pandas", (), Some(&kwargs))
    }

    /// Converts the edges into a pyarrow Table of their updates, with one row per update.
    ///
    /// The Table has the following columns:
    /// - "rap_time": the time of the update, in milliseconds since the epoch.
    /// - "rap_secondary_index": the event id of the update, which orders updates made at the same time.
    /// - "rap_src_gid": the id of the source node, an integer or a string depending on the graph.
    /// - "rap_dst_gid": the id of the destination node, an integer or a string depending on the graph.
    /// - "rap_layer": the layer of the update.
    ///
    /// These are followed by one column per temporal property. A cell is empty when the update did
    /// not set that property. By default, only the updates that hold the latest value of a property are included.
    ///
    /// Args:
    ///     include_property_history (bool): If True, include every update instead of only the updates that hold a property's latest value. Defaults to False.
    ///     convert_datetime (bool): If True, the "rap_time" column holds UTC datetimes instead of milliseconds. Defaults to False.
    ///
    /// Returns:
    ///     pyarrow.Table: the edge updates as a pyarrow Table.
    #[pyo3(signature = (include_property_history = false, convert_datetime = false))]
    pub fn to_arrow<'py>(
        &self,
        py: Python<'py>,
        include_property_history: bool,
        convert_datetime: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.update_table(py, include_property_history, convert_datetime)
    }
}

impl Repr for PyEdges {
    fn repr(&self) -> String {
        self.edges.repr()
    }
}

/// A list of edges per node.
#[pyclass(name = "NestedEdges", module = "raphtory")]
pub struct PyNestedEdges {
    edges: NestedEdges<'static, DynamicGraph, DynEdgeItem>,
}

impl_edgeviewops!(
    PyNestedEdges,
    edges,
    NestedEdges<'static, DynamicGraph, DynEdgeItem>,
    "NestedEdges",
    "NestedEdges"
);
impl_iterable_mixin!(
    PyNestedEdges,
    edges,
    Vec<Vec<EdgeView<DynamicGraph>>>,
    "list[list[Edges]]",
    "edge"
);

impl<'py, G: StaticGraphViewOps + IntoDynamic, K: EdgeItem> IntoPyObject<'py>
    for NestedEdges<'static, G, K>
where
    NestedEdges<'static, DynamicGraph, DynEdgeItem>: From<NestedEdges<'static, DynamicGraph, K>>,
{
    type Target = PyNestedEdges;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let edges = self.into_dyn_hop().into();
        PyNestedEdges { edges }.into_pyobject(py)
    }
}

impl<'graph, G: GraphViewOps<'graph>, K: EdgeItem> Repr for NestedEdges<'graph, G, K> {
    fn repr(&self) -> String {
        format!("NestedEdges({})", iterator_repr(self.iter()))
    }
}

impl<G: StaticGraphViewOps + IntoDynamic, K: EdgeItem> From<NestedEdges<'static, G, K>>
    for PyNestedEdges
where
    NestedEdges<'static, DynamicGraph, DynEdgeItem>: From<NestedEdges<'static, DynamicGraph, K>>,
{
    fn from(value: NestedEdges<'static, G, K>) -> Self {
        Self {
            edges: value.into_dyn().into(),
        }
    }
}

#[pymethods]
impl PyNestedEdges {
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyNestedEdges> {
        let r = self.edges.select(filter)?;
        Ok(PyNestedEdges::from(r))
    }

    /// Returns the earliest time of the edges.
    ///
    /// Returns:
    ///     NestedOptionEventTimeIterable: A nested iterable of `EventTime`s.
    #[getter]
    fn earliest_time(&self) -> NestedOptionEventTimeIterable {
        let edges = self.edges.clone();
        (move || edges.earliest_time()).into()
    }

    /// Returns the latest time of the edges.
    ///
    /// Returns:
    ///     NestedOptionEventTimeIterable: A nested iterable of `EventTime`s.
    #[getter]
    fn latest_time(&self) -> NestedOptionEventTimeIterable {
        let edges = self.edges.clone();
        (move || edges.latest_time()).into()
    }

    /// Returns the times of exploded edges.
    ///
    /// Returns:
    ///     NestedEventTimeIterable: A nested iterable of `EventTime`s.
    ///
    /// Raises:
    ///     GraphError: If a graph error occurs (e.g. the edges are not exploded).
    #[getter]
    fn time(&self) -> Result<NestedEventTimeIterable, GraphError> {
        match self.edges.time().flatten().next() {
            Some(Err(err)) => Err(err),
            _ => {
                let edges = self.edges.clone();
                Ok((move || {
                    edges
                        .time()
                        .map(|t_iter| t_iter.map(|t| t.unwrap()).into_dyn_boxed())
                        .into_dyn_boxed()
                })
                .into())
            }
        }
    }

    /// Returns the name of the layer the edges belong to - assuming they only belong to one layer.
    ///
    /// Returns:
    ///     NestedArcStringIterable:
    #[getter]
    fn layer_name(&self) -> Result<NestedArcStringIterable, GraphError> {
        match self.edges.layer_name().flatten().next() {
            Some(Err(err)) => Err(err),
            _ => {
                let edges = self.edges.clone();
                Ok((move || {
                    edges
                        .layer_name()
                        .map(|layer_name_iter| {
                            layer_name_iter
                                .map(|layer_name| layer_name.unwrap())
                                .into_dyn_boxed()
                        })
                        .into_dyn_boxed()
                })
                .into())
            }
        }
    }

    /// Returns the names of the layers the edges belong to.
    ///
    /// Returns:
    ///     NestedArcStringVecIterable:
    #[getter]
    fn layer_names(&self) -> NestedArcStringVecIterable {
        let edges = self.edges.clone();
        (move || edges.layer_names()).into()
    }

    // FIXME: needs a view that allows indexing into the properties
    /// Returns all properties of the edges
    ///
    /// Returns:
    ///     PyNestedPropsIterable:
    #[getter]
    fn properties(&self) -> PyNestedPropsIterable {
        let edges = self.edges.clone();
        (move || edges.properties()).into()
    }

    /// Get a view of the metadata only.
    ///
    /// Returns:
    ///     MetadataListList:
    #[getter]
    pub fn metadata(&self) -> MetadataListList {
        let edges = self.edges.clone();
        (move || edges.metadata()).into()
    }

    /// Returns all ids of the edges.
    ///
    /// Returns:
    ///     NestedGIDGIDIterable:
    #[getter]
    fn id(&self) -> NestedGIDGIDIterable {
        let edges = self.edges.clone();
        (move || edges.id()).into()
    }

    /// Returns a history object for each edge containing time entries for when the edge is added or change to the edge is made.
    ///
    /// Returns:
    ///     NestedHistoryIterable: A nested iterable of history objects, one for each edge.
    #[getter]
    fn history(&self) -> NestedHistoryIterable {
        let edges = self.edges.clone();
        (move || {
            edges
                .history()
                .map(|history_iter| history_iter.map(|history| history.into_arc_dyn()))
        })
        .into()
    }

    /// Returns a history object for each edge containing their deletion times.
    ///
    /// Returns:
    ///     NestedHistoryIterable: A nested iterable of history objects, one for each edge.
    #[getter]
    fn deletions(&self) -> NestedHistoryIterable {
        let edges = self.edges.clone();
        (move || {
            edges
                .deletions()
                .map(|history_iter| history_iter.map(|history| history.into_arc_dyn()))
        })
        .into()
    }

    /// Check if edges are valid (i.e., not deleted).
    ///
    /// Returns:
    ///     NestedBoolIterable:
    fn is_valid(&self) -> NestedBoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_valid()).into()
    }

    /// Check if the edges are active (there is at least one update during this time).
    ///
    /// Returns:
    ///     NestedBoolIterable:
    fn is_active(&self) -> NestedBoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_active()).into()
    }

    /// Check if the edges are on the same node.
    ///
    /// Returns:
    ///     NestedBoolIterable:
    fn is_self_loop(&self) -> NestedBoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_self_loop()).into()
    }

    /// Check if edges are deleted.
    ///
    /// Returns:
    ///     NestedBoolIterable:
    fn is_deleted(&self) -> NestedBoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_deleted()).into()
    }
}
