use crate::{
    core::{ArcStr, Prop},
    db::api::view::node::BaseNodeViewOps,
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps},
    python::graph::views::graph_view::PyGraphView,
};
use pyo3::{
    prelude::PyModule,
    pymethods,
    types::{PyDict, PyList, PyTuple},
    IntoPy, PyObject, PyResult, Python, ToPyObject,
};
use std::collections::HashMap;
#[pymethods]
impl PyGraphView {
    /// Converts the graph's nodes into a Pandas DataFrame.
    ///
    /// This method will create a DataFrame with the following columns:
    /// - "name": The name of the node.
    /// - "properties": The properties of the node. This column will be included if `include_node_properties` is set to `true`.
    /// - "property_history": The history of the node's properties. This column will be included if both `include_node_properties` and `include_property_histories` are set to `true`.
    /// - "update_history": The update history of the node. This column will be included if `include_update_history` is set to `true`.
    ///
    /// Args:
    ///     include_node_properties (bool): A boolean wrapped in an Option. If set to `true`, the "properties" and "property_history" columns will be included in the DataFrame. Defaults to `true`.
    ///     include_update_history (bool): A boolean wrapped in an Option. If set to `true`, the "update_history" column will be included in the DataFrame. Defaults to `true`.
    ///     include_property_histories (bool): A boolean wrapped in an Option. If set to `true`, the "property_history" column will be included in the DataFrame. Defaults to `true`.
    ///
    /// Returns:
    ///     If successful, this PyObject will be a Pandas DataFrame.
    #[pyo3(signature = (include_node_properties=true, include_update_history=true, include_property_histories=true))]
    pub fn to_node_df(
        &self,
        include_node_properties: Option<bool>,
        include_update_history: Option<bool>,
        include_property_histories: Option<bool>,
    ) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let pandas = PyModule::import(py, "pandas")?;
            let column_names = vec!["name", "properties", "property_history", "update_history"];
            let node_tuples: Vec<_> = self
                .graph
                .nodes()
                .map(|g, b| {
                    let v = g.node(b).unwrap();
                    let mut properties: Option<HashMap<ArcStr, Prop>> = None;
                    let mut temporal_properties: Option<Vec<(ArcStr, (i64, Prop))>> = None;
                    let mut update_history: Option<Vec<_>> = None;
                    if include_node_properties == Some(true) {
                        if include_property_histories == Some(true) {
                            properties = Some(v.properties().constant().as_map());
                            temporal_properties = Some(v.properties().temporal().histories());
                        } else {
                            properties = Some(v.properties().as_map());
                        }
                    }
                    if include_update_history == Some(true) {
                        update_history = Some(v.history());
                    }
                    (v.name(), properties, temporal_properties, update_history)
                })
                .collect();
            let kwargs = PyDict::new(py);
            kwargs.set_item("columns", column_names)?;
            let df = pandas.call_method("DataFrame", (node_tuples,), Some(kwargs))?;
            let kwargs_drop = PyDict::new(py);
            kwargs_drop.set_item("how", "all")?;
            kwargs_drop.set_item("axis", 1)?;
            kwargs_drop.set_item("inplace", true)?;
            df.call_method("dropna", (), Some(kwargs_drop))?;
            Ok(df.to_object(py))
        })
    }

    /// Converts the graph's edges into a Pandas DataFrame.
    ///
    /// This method will create a DataFrame with the following columns:
    /// - "src": The source node of the edge.
    /// - "dst": The destination node of the edge.
    /// - "layer": The layer of the edge.
    /// - "properties": The properties of the edge. This column will be included if `include_edge_properties` is set to `true`.
    /// - "property_histories": The history of the edge's properties. This column will be included if both `include_edge_properties` and `include_property_histories` are set to `true`.
    /// - "update_history": The update history of the edge. This column will be included if `include_update_history` is set to `true`.
    /// - "update_history_exploded": The exploded update history of the edge. This column will be included if `explode_edges` is set to `true`.
    ///
    /// Args:
    ///     explode_edges (bool): A boolean wrapped in an Option. If set to `true`, the "update_history_exploded" column will be included in the DataFrame. Defaults to `false`.
    ///     include_edge_properties (bool): A boolean wrapped in an Option. If set to `true`, the "properties" and "property_histories" columns will be included in the DataFrame. Defaults to `true`.
    ///     include_update_history (bool): A boolean wrapped in an Option. If set to `true`, the "update_history" column will be included in the DataFrame. Defaults to `true`.
    ///     include_property_histories (bool): A boolean wrapped in an Option. If set to `true`, the "property_histories" column will be included in the DataFrame. Defaults to `true`.
    ///
    /// Returns:
    ///     If successful, this PyObject will be a Pandas DataFrame.
    #[pyo3(signature = (explode_edges=false, include_edge_properties=true, include_update_history=true, include_property_histories=true))]
    pub fn to_edge_df(
        &self,
        explode_edges: Option<bool>,
        include_edge_properties: Option<bool>,
        include_update_history: Option<bool>,
        include_property_histories: Option<bool>,
    ) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let pandas = PyModule::import(py, "pandas")?;
            let column_names = vec![
                "src",
                "dst",
                "layer",
                "properties",
                "property_histories",
                "update_history",
                "update_history_exploded",
            ];
            let mut edges = self.graph.edges();
            if explode_edges == Some(true) {
                edges = self.graph.edges().explode_layers().explode();
            }
            let edge_tuples: Vec<_> = edges
                .iter()
                .map(|e| {
                    let mut properties: Option<HashMap<ArcStr, Prop>> = None;
                    let mut temporal_properties: Option<Vec<(ArcStr, (i64, Prop))>> = None;
                    if include_edge_properties == Some(true) {
                        if include_property_histories == Some(true) {
                            properties = Some(e.properties().constant().as_map());
                            temporal_properties = Some(e.properties().temporal().histories());
                        } else {
                            properties = Some(e.properties().as_map());
                        }
                    }
                    let mut update_history_exploded: Option<i64> = None;
                    let mut update_history: Option<Vec<_>> = None;
                    if include_update_history == Some(true) {
                        if explode_edges == Some(true) {
                            update_history_exploded = e.time();
                        } else {
                            update_history = Some(e.history());
                        }
                    }
                    (
                        e.src().name(),
                        e.dst().name(),
                        e.layer_name(),
                        properties,
                        temporal_properties,
                        update_history,
                        update_history_exploded,
                    )
                })
                .collect();
            let kwargs = PyDict::new(py);
            kwargs.set_item("columns", column_names)?;
            let df = pandas.call_method("DataFrame", (edge_tuples,), Some(kwargs))?;
            let kwargs_drop = PyDict::new(py);
            kwargs_drop.set_item("how", "all")?;
            kwargs_drop.set_item("axis", 1)?;
            kwargs_drop.set_item("inplace", true)?;
            df.call_method("dropna", (), Some(kwargs_drop))?;
            Ok(df.to_object(py))
        })
    }

    ///Returns a graph with NetworkX.
    ///
    ///     Network X is a required dependency.
    ///     If you intend to use this function make sure that
    ///     you install Network X with ``pip install networkx``
    ///
    ///     Args:
    ///         explode_edges (bool): A boolean that is set to True if you want to explode the edges in the graph. By default this is set to False.
    ///         include_node_properties (bool): A boolean that is set to True if you want to include the node properties in the graph. By default this is set to True.
    ///         include_edge_properties (bool): A boolean that is set to True if you want to include the edge properties in the graph. By default this is set to True.
    ///         include_update_history (bool): A boolean that is set to True if you want to include the update histories in the graph. By default this is set to True.
    ///         include_property_histories (bool): A boolean that is set to True if you want to include the histories in the graph. By default this is set to True.
    ///
    ///     Returns:
    ///         A Networkx MultiDiGraph.
    #[pyo3(signature = (explode_edges=false, include_node_properties=true, include_edge_properties=true,include_update_history=true,include_property_histories=true))]
    pub fn to_networkx(
        &self,
        explode_edges: Option<bool>,
        include_node_properties: Option<bool>,
        include_edge_properties: Option<bool>,
        include_update_history: Option<bool>,
        include_property_histories: Option<bool>,
    ) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let networkx = py.import("networkx")?.getattr("MultiDiGraph")?.call0()?;

            let mut node_tuples = Vec::new();
            for v in self.graph.nodes().iter() {
                let properties = PyDict::new(py);
                if include_node_properties.unwrap_or(true) {
                    if include_property_histories.unwrap_or(true) {
                        let const_props = v.properties().constant().as_map();
                        let const_props_py = PyDict::new(py);
                        for (key, value) in const_props {
                            const_props_py.set_item(key, value.into_py(py))?;
                        }
                        properties.set_item("constant", const_props_py)?;
                        properties.set_item(
                            "temporal",
                            PyList::new(py, v.properties().temporal().histories()),
                        )?;
                    } else {
                        for (key, value) in v.properties().as_map() {
                            properties.set_item(key, value.into_py(py))?;
                        }
                    }
                }
                if include_update_history.unwrap_or(true) {
                    properties.set_item("update_history", v.history().to_object(py))?;
                }
                if v.node_type().is_some() {
                    properties.set_item("node_type", v.node_type().unwrap())?;
                }
                let node_tuple =
                    PyTuple::new(py, &[v.name().to_object(py), properties.to_object(py)]);
                node_tuples.push(node_tuple);
            }
            networkx.call_method1("add_nodes_from", (node_tuples,))?;

            let mut edge_tuples = Vec::new();
            let edges = if explode_edges.unwrap_or(false) {
                self.graph.edges().explode()
            } else {
                self.graph.edges().explode_layers()
            };

            for e in edges.iter() {
                let properties = PyDict::new(py);
                let src = e.src().name();
                let dst = e.dst().name();
                if include_edge_properties.unwrap_or(true) {
                    if include_property_histories.unwrap_or(true) {
                        let const_props = e.properties().constant().as_map();
                        let const_props_py = PyDict::new(py);
                        for (key, value) in const_props {
                            const_props_py.set_item(key, value.into_py(py))?;
                        }
                        properties.set_item("constant", const_props_py)?;
                        let prop_hist = e.properties().temporal().histories();
                        let mut prop_hist_map: HashMap<ArcStr, Vec<(i64, Prop)>> = HashMap::new();
                        for (key, value) in prop_hist {
                            prop_hist_map
                                .entry(key)
                                .or_insert_with(Vec::new)
                                .push(value);
                        }
                        let output: Vec<(ArcStr, Vec<(i64, Prop)>)> =
                            prop_hist_map.into_iter().collect();
                        properties.set_item("temporal", PyList::new(py, output))?;
                    } else {
                        for (key, value) in e.properties().as_map() {
                            properties.set_item(key, value.into_py(py))?;
                        }
                    }
                }
                let layer = e.layer_name();
                if layer.is_some() {
                    properties.set_item("layer", layer)?;
                }
                if include_update_history.unwrap_or(true) {
                    if explode_edges.unwrap_or(true) {
                        properties.set_item("update_history", e.time())?;
                    } else {
                        properties.set_item("update_history", e.history())?;
                    }
                }
                let edge_tuple = PyTuple::new(
                    py,
                    &[
                        src.to_object(py),
                        dst.to_object(py),
                        properties.to_object(py),
                    ],
                );
                edge_tuples.push(edge_tuple);
            }
            networkx.call_method1("add_edges_from", (edge_tuples,))?;

            Ok(networkx.to_object(py))
        })
    }
}
