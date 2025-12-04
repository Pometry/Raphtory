use crate::{
    db::{
        api::view::{DynamicGraph, IntoDynBoxed, IntoDynamic, Select, StaticGraphViewOps},
        graph::{
            edge::EdgeView,
            edges::{Edges, NestedEdges},
        },
    },
    errors::GraphError,
    prelude::*,
    python::{
        filter::filter_expr::PyFilterExpr,
        graph::properties::{
            MetadataListList, MetadataView, PropertiesView, PyNestedPropsIterable,
        },
        types::{
            repr::{iterator_repr, Repr},
            wrappers::iterables::{
                ArcStringIterable, ArcStringVecIterable, BoolIterable, GIDGIDIterable, I64Iterable,
                NestedArcStringIterable, NestedArcStringVecIterable, NestedBoolIterable,
                NestedGIDGIDIterable, NestedI64VecIterable, NestedOptionI64Iterable,
                NestedUtcDateTimeIterable, NestedVecUtcDateTimeIterable, OptionI64Iterable,
                OptionUtcDateTimeIterable, OptionVecUtcDateTimeIterable, U64Iterable,
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

impl<'graph, G: GraphViewOps<'graph>> Repr for Edges<'graph, G> {
    fn repr(&self) -> String {
        format!("Edges({})", iterator_repr(self.iter()))
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for Edges<'static, G> {
    type Target = PyEdges;
    type Output = Bound<'py, PyEdges>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let base_graph = self.base_graph.into_dynamic();
        let edges = self.edges;
        PyEdges {
            edges: Edges { base_graph, edges },
        }
        .into_pyobject(py)
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> From<Edges<'static, G>> for PyEdges {
    fn from(value: Edges<'static, G>) -> Self {
        let base_graph = value.base_graph.into_dynamic();
        Self {
            edges: Edges::new(base_graph, value.edges),
        }
    }
}

#[pymethods]
impl PyEdges {
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyEdges> {
        let r = self.edges.select(filter)?;
        Ok(PyEdges::from(r))
    }

    /// Returns the number of edges.
    ///
    /// Returns:
    ///     int:
    fn count(&self) -> usize {
        self.edges.len()
    }

    /// Returns the earliest time of the edges.
    ///
    /// Returns:
    ///     OptionI64Iterable:
    #[getter]
    fn earliest_time(&self) -> OptionI64Iterable {
        let edges = self.edges.clone();
        (move || edges.earliest_time()).into()
    }

    /// Returns the earliest date time of the edges.
    ///
    /// Returns:
    ///     OptionUtcDateTimeIterable:
    #[getter]
    fn earliest_date_time(&self) -> OptionUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.earliest_date_time()).into()
    }

    /// Returns the latest time of the edges.
    ///
    /// Returns:
    ///     OptionI64Iterable:
    #[getter]
    fn latest_time(&self) -> OptionI64Iterable {
        let edges = self.edges.clone();
        (move || edges.latest_time()).into()
    }

    /// Returns the latest date time of the edges.
    ///
    /// Returns:
    ///     OptionUtcDateTimeIterable:
    #[getter]
    fn latest_date_time(&self) -> OptionUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.latest_date_time()).into()
    }

    /// Returns the date times of exploded edges
    ///
    /// Returns:
    ///    OptionUtcDateTimeIterable:
    #[getter]
    fn date_time(&self) -> OptionUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.date_time()).into()
    }

    /// Returns the times of exploded edges.
    ///
    /// Returns:
    ///     I64Iterable:
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

    /// Returns all timestamps of edges, when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///     PyGenericIterable:
    ///
    fn history(&self) -> PyGenericIterable {
        let edges = self.edges.clone();
        (move || edges.history().map(NumpyArray::I64)).into()
    }

    /// Returns the number of times any edge was added or change to an edge was been made.
    ///
    /// Returns:
    ///     U64Iterable:
    fn history_counts(&self) -> U64Iterable {
        let edges = self.edges.clone();
        (move || edges.history_counts().map(|count| count as u64)).into()
    }

    /// Returns all timestamps of edges, when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///     OptionVecUtcDateTimeIterable:
    fn history_date_time(&self) -> OptionVecUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.history_date_time()).into()
    }

    /// Returns all timestamps of edges where an edge is deleted
    ///
    /// Returns:
    ///     PyGenericIterable:
    fn deletions(&self) -> PyGenericIterable {
        let edges = self.edges.clone();
        (move || edges.deletions().map(NumpyArray::I64)).into()
    }

    /// Returns all timestamps of edges where an edge is deleted
    ///
    /// Returns:
    ///     OptionVecUtcDateTimeIterable:
    fn deletions_date_time(&self) -> OptionVecUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.deletions_date_time()).into()
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
        let edge_meta = self.edges.base_graph.edge_meta();
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
                    &item.metadata(),
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
                let history = item.history();

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

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for NestedEdges<'static, G> {
    type Target = PyNestedEdges;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let edges = NestedEdges {
            nodes: self.nodes,
            graph: self.graph.into_dynamic(),
            edges: self.edges,
        };
        PyNestedEdges { edges }.into_pyobject(py)
    }
}

impl<'graph, G: GraphViewOps<'graph>> Repr for NestedEdges<'graph, G> {
    fn repr(&self) -> String {
        format!("NestedEdges({})", iterator_repr(self.iter()))
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> From<NestedEdges<'static, G>> for PyNestedEdges {
    fn from(value: NestedEdges<'static, G>) -> Self {
        let base_graph = value.graph.into_dynamic();
        Self {
            edges: NestedEdges::new(base_graph, value.nodes, value.edges),
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
    ///     NestedOptionI64Iterable:
    #[getter]
    fn earliest_time(&self) -> NestedOptionI64Iterable {
        let edges = self.edges.clone();
        (move || edges.earliest_time()).into()
    }

    /// Returns the earliest date time of the edges.
    ///
    /// Returns:
    ///     NestedUtcDateTimeIterable:
    #[getter]
    fn earliest_date_time(&self) -> NestedUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.earliest_date_time()).into()
    }

    /// Returns the latest time of the edges.
    ///
    /// Returns:
    ///     NestedOptionI64Iterable:
    #[getter]
    fn latest_time(&self) -> NestedOptionI64Iterable {
        let edges = self.edges.clone();
        (move || edges.latest_time()).into()
    }

    /// Returns the latest date time of the edges.
    ///
    /// Returns:
    ///     NestedUtcDateTimeIterable:
    #[getter]
    fn latest_date_time(&self) -> NestedUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.latest_date_time()).into()
    }

    /// Returns the times of exploded edges.
    ///
    /// Returns:
    ///     NestedOptionI64Iterable:
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

    /// Returns all timestamps of edges, when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///     NestedI64VecIterable:
    fn history(&self) -> NestedI64VecIterable {
        let edges = self.edges.clone();
        (move || edges.history()).into()
    }

    /// Returns all timestamps of edges, when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///     NestedVecUtcDateTimeIterable:
    fn history_date_time(&self) -> NestedVecUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.history_date_time()).into()
    }

    /// Returns all timestamps of edges, where an edge is deleted.
    ///
    /// Returns:
    ///     NestedI64VecIterable: A list of lists of lists of unix timestamps
    fn deletions(&self) -> NestedI64VecIterable {
        let edges = self.edges.clone();
        (move || edges.deletions()).into()
    }

    /// Returns all timestamps of edges, where an edge is deleted.
    ///
    /// Returns:
    ///     NestedVecUtcDateTimeIterable: A list of lists of lists of DateTime objects
    fn deletions_date_time(&self) -> NestedVecUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.deletions_date_time()).into()
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

    /// Get the date times of exploded edges.
    ///
    /// Returns:
    ///     NestedUtcDateTimeIterable:
    #[getter]
    fn date_time(&self) -> NestedUtcDateTimeIterable {
        let edges = self.edges.clone();
        (move || edges.date_time()).into()
    }
}
