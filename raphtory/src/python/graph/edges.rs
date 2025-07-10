use crate::{
    api::core::storage::timeindex::TimeError,
    db::{
        api::view::{DynamicGraph, IntoDynBoxed, IntoDynamic, StaticGraphViewOps},
        graph::{
            edge::EdgeView,
            edges::{Edges, NestedEdges},
        },
    },
    errors::GraphError,
    prelude::*,
    python::{
        graph::properties::{PropertiesView, PyNestedPropsIterable},
        types::{
            repr::{iterator_repr, Repr},
            result_option_iterable::{
                NestedResultOptionUtcDateTimeIterable, ResultOptionUtcDateTimeIterable,
            },
            result_vec_iterable::{
                NestedResultVecUtcDateTimeIterable, ResultVecUtcDateTimeIterable,
            },
            wrappers::iterables::{
                ArcStringIterable, ArcStringVecIterable, BoolIterable, GIDGIDIterable,
                HistoryIterable, I64Iterable, NestedArcStringIterable, NestedArcStringVecIterable,
                NestedBoolIterable, NestedGIDGIDIterable, NestedHistoryIterable,
                NestedI64VecIterable, NestedOptionI64Iterable, NestedOptionRaphtoryTimeIterable,
                OptionRaphtoryTimeIterable,
            },
        },
        utils::{
            export::{create_row, extract_properties, get_column_names_from_props},
            NumpyArray, PyGenericIterable,
        },
    },
};
use pyo3::{prelude::*, types::PyDict};
use raphtory_api::core::storage::arc_str::ArcStr;
use raphtory_storage::core_ops::CoreGraphOps;
use rayon::{iter::IntoParallelIterator, prelude::*};
use std::collections::HashMap;

/// A list of edges that can be iterated over.
#[pyclass(name = "Edges", module = "raphtory", frozen)]
pub struct PyEdges {
    edges: Edges<'static, DynamicGraph>,
}

impl_edgeviewops!(PyEdges, edges, Edges<'static, DynamicGraph>, "Edges");
impl_iterable_mixin!(
    PyEdges,
    edges,
    Vec<EdgeView<DynamicGraph>>,
    "list[Edge]",
    "edge",
    |edges: &Edges<'static, DynamicGraph>| edges.clone().into_iter()
);

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Repr for Edges<'graph, G, GH> {
    fn repr(&self) -> String {
        format!("Edges({})", iterator_repr(self.iter()))
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    IntoPyObject<'py> for Edges<'static, G, GH>
{
    type Target = PyEdges;
    type Output = Bound<'py, PyEdges>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
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
        .into_pyobject(py)
    }
}

#[pymethods]
impl PyEdges {
    /// Returns the number of edges
    fn count(&self) -> usize {
        self.edges.len()
    }

    /// Returns the earliest time of the edges.
    ///
    /// Returns:
    /// Earliest time of the edges as an iterable of RaphtoryTime items.
    #[getter]
    fn earliest_time(&self) -> OptionRaphtoryTimeIterable {
        let edges = self.edges.clone();
        (move || edges.earliest_time()).into()
    }

    /// Returns the latest time of the edges.
    ///
    /// Returns:
    ///  Latest time of the edges as an iterable of RaphtoryTime items.
    #[getter]
    fn latest_time(&self) -> OptionRaphtoryTimeIterable {
        let edges = self.edges.clone();
        (move || edges.latest_time()).into()
    }

    /// Returns the date times of exploded edges
    ///
    /// Returns:
    ///    A list of date times.
    #[getter]
    fn date_time(&self) -> ResultOptionUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.date_time()).into()
    }

    /// Returns the times of exploded edges
    ///
    /// Returns:
    ///   Time of edge
    #[getter]
    fn time(&self) -> Result<I64Iterable, GraphError> {
        match self.edges.time().next() {
            Some(Err(err)) => Err(err),
            _ => {
                let edges = self.edges.clone();
                Ok((move || edges.time().map(|t| t.unwrap())).into())
            }
        }
    }

    /// Returns all properties of the edges
    #[getter]
    fn properties(&self) -> PropertiesView {
        let edges = self.edges.clone();
        (move || edges.properties()).into()
    }

    /// Returns all ids of the edges.
    #[getter]
    fn id(&self) -> GIDGIDIterable {
        let edges = self.edges.clone();
        (move || edges.id()).into()
    }

    /// Returns history objects for edges containing their timestamps, when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///    An iterable of history objects, one for each edge.
    ///
    fn history(&self) -> HistoryIterable {
        let edges = self.edges.clone();
        (move || edges.history().map(|history| history.into_arc_static())).into()
    }

    /// Returns all timestamps of edges where an edge is deleted
    ///
    /// Returns:
    ///     A list of lists of unix timestamps
    fn deletions(&self) -> PyGenericIterable {
        let edges = self.edges.clone();
        (move || edges.deletions().map(NumpyArray::I64)).into()
    }

    /// Returns all timestamps of edges where an edge is deleted
    ///
    /// Returns:
    ///     A list of lists of DateTime objects
    fn deletions_date_time(&self) -> ResultVecUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.deletions_date_time()).into()
    }

    /// Check if the edges are valid (i.e. not deleted)
    fn is_valid(&self) -> BoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_valid()).into()
    }

    ////Check if the edges are active (i.e. there is at least one update during this time)
    fn is_active(&self) -> BoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_active()).into()
    }

    /// Check if the edges are on the same node
    fn is_self_loop(&self) -> BoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_self_loop()).into()
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
    fn layer_name(&self) -> Result<ArcStringIterable, GraphError> {
        match self.edges.layer_name().next() {
            Some(Err(err)) => Err(err),
            _ => {
                let edges = self.edges.clone();
                Ok((move || edges.layer_name().map(|layer| layer.unwrap())).into())
            }
        }
    }

    /// Get the layer names that all edges belong to - assuming they only belong to one layer
    ///
    /// Returns:
    ///   A list of layer names
    #[getter]
    fn layer_names(&self) -> ArcStringVecIterable {
        let edges = self.edges.clone();
        (move || edges.layer_names()).into()
    }

    /// Converts the graph's edges into a Pandas DataFrame.
    ///
    /// This method will create a DataFrame with the following columns:
    /// - "src": The source node of the edge.
    /// - "dst": The destination node of the edge.
    /// - "layer": The layer of the edge.
    /// - "properties": The properties of the edge.
    /// - "update_history": The update history of the edge. This column will be included if `include_update_history` is set to `true`.
    ///
    /// Args:
    ///     include_property_history (bool): A boolean, if set to `True`, the history of each property is included, if `False`, only the latest value is shown. Ignored if exploded. Defaults to True.
    ///     convert_datetime (bool): A boolean, if set to `True` will convert the timestamp to python datetimes. Defaults to False.
    ///     explode (bool): A boolean, if set to `True`, will explode each edge update into its own row. Defaults to False.
    ///
    /// Returns:
    ///     DataFrame: If successful, this PyObject will be a Pandas DataFrame.
    #[pyo3(signature = (include_property_history = true, convert_datetime = false, explode = false))]
    pub fn to_df(
        &self,
        include_property_history: bool,
        convert_datetime: bool,
        mut explode: bool,
    ) -> PyResult<PyObject> {
        let mut column_names = vec![
            String::from("src"),
            String::from("dst"),
            String::from("layer"),
        ];
        let edge_meta = self.edges.graph.edge_meta();
        let is_prop_both_temp_and_const = get_column_names_from_props(&mut column_names, edge_meta);

        let mut edges = self.edges.explode_layers();
        if explode {
            edges = self.edges.explode_layers().explode();
        }

        explode = explode
            || edges
                .iter()
                .next()
                .filter(|e| e.edge.time().is_some())
                .is_some();

        let edge_tuples: Vec<_> = edges
            .collect()
            .into_par_iter()
            .flat_map(|item| {
                let mut properties_map: HashMap<String, Prop> = HashMap::new();
                let mut prop_time_dict: HashMap<i64, HashMap<String, Prop>> = HashMap::new();

                extract_properties(
                    include_property_history,
                    convert_datetime,
                    explode,
                    &column_names,
                    &is_prop_both_temp_and_const,
                    &item.properties(),
                    &mut properties_map,
                    &mut prop_time_dict,
                    item.start().unwrap_or(0),
                );

                let row_header: Vec<Prop> = vec![
                    Prop::from(item.src().name()),
                    Prop::from(item.dst().name()),
                    Prop::from(item.layer_name().unwrap_or(ArcStr::from(""))),
                ];

                let start_point = 3;
                let history = item.history().t().collect();

                create_row(
                    convert_datetime,
                    explode,
                    &column_names,
                    properties_map,
                    prop_time_dict,
                    row_header,
                    start_point,
                    history,
                )
            })
            .collect();

        Python::with_gil(|py| {
            let pandas = PyModule::import(py, "pandas")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("columns", column_names)?;
            Ok(pandas
                .call_method("DataFrame", (edge_tuples,), Some(&kwargs))?
                .unbind())
        })
    }
}

impl Repr for PyEdges {
    fn repr(&self) -> String {
        format!("Edges({})", iterator_repr(self.edges.iter()))
    }
}

#[pyclass(name = "NestedEdges", module = "raphtory")]
pub struct PyNestedEdges {
    edges: NestedEdges<'static, DynamicGraph>,
}

impl_edgeviewops!(
    PyNestedEdges,
    edges,
    NestedEdges<'static, DynamicGraph>,
    "NestedEdges"
);
impl_iterable_mixin!(
    PyNestedEdges,
    edges,
    Vec<Vec<EdgeView<DynamicGraph>>>,
    "list[list[Edges]]",
    "edge"
);

impl<'py, G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    IntoPyObject<'py> for NestedEdges<'static, G, GH>
{
    type Target = PyNestedEdges;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let edges = NestedEdges {
            graph: self.graph.into_dynamic(),
            nodes: self.nodes,
            base_graph: self.base_graph.into_dynamic(),
            edges: self.edges,
        };
        PyNestedEdges { edges }.into_pyobject(py)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Repr
    for NestedEdges<'graph, G, GH>
{
    fn repr(&self) -> String {
        format!("NestedEdges({})", iterator_repr(self.iter()))
    }
}

#[pymethods]
impl PyNestedEdges {
    /// Returns the earliest time of the edges as RaphtoryTime.
    #[getter]
    fn earliest_time(&self) -> NestedOptionRaphtoryTimeIterable {
        let edges = self.edges.clone();
        (move || edges.earliest_time()).into()
    }

    /// Returns the latest time of the edges.
    #[getter]
    fn latest_time(&self) -> NestedOptionRaphtoryTimeIterable {
        let edges = self.edges.clone();
        (move || edges.latest_time()).into()
    }

    /// Returns the times of exploded edges
    #[getter]
    fn time(&self) -> Result<NestedOptionI64Iterable, GraphError> {
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

    /// Returns the name of the layer the edges belong to - assuming they only belong to one layer
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

    /// Returns the names of the layers the edges belong to
    #[getter]
    fn layer_names(&self) -> NestedArcStringVecIterable {
        let edges = self.edges.clone();
        (move || edges.layer_names()).into()
    }

    // FIXME: needs a view that allows indexing into the properties
    /// Returns all properties of the edges
    #[getter]
    fn properties(&self) -> PyNestedPropsIterable {
        let edges = self.edges.clone();
        (move || edges.properties()).into()
    }

    /// Returns all ids of the edges.
    #[getter]
    fn id(&self) -> NestedGIDGIDIterable {
        let edges = self.edges.clone();
        (move || edges.id()).into()
    }

    /// Returns history objects for edges, containing information about when an edge is added or change to an edge is made.
    fn history(&self) -> NestedHistoryIterable {
        let edges = self.edges.clone();
        (move || {
            edges
                .history()
                .map(|history_iter| history_iter.map(|history| history.into_arc_static()))
        })
        .into()
    }

    /// Returns all timestamps of edges, where an edge is deleted
    ///
    /// Returns:
    ///     A list of lists of lists of unix timestamps
    fn deletions(&self) -> NestedI64VecIterable {
        let edges = self.edges.clone();
        (move || edges.deletions()).into()
    }

    /// Returns all timestamps of edges, where an edge is deleted
    ///
    /// Returns:
    ///     A list of lists of lists of DateTime objects
    fn deletions_date_time(&self) -> NestedResultVecUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.deletions_date_time()).into()
    }

    /// Check if edges are valid (i.e., not deleted)
    fn is_valid(&self) -> NestedBoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_valid()).into()
    }

    ////Check if the edges are active (i.e. there is at least one update during this time)
    fn is_active(&self) -> NestedBoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_active()).into()
    }

    /// Check if the edges are on the same node
    fn is_self_loop(&self) -> NestedBoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_self_loop()).into()
    }

    /// Check if edges are deleted
    fn is_deleted(&self) -> NestedBoolIterable {
        let edges = self.edges.clone();
        (move || edges.is_deleted()).into()
    }

    /// Get the date times of exploded edges
    #[getter]
    fn date_time(&self) -> NestedResultOptionUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.date_time()).into()
    }
}
