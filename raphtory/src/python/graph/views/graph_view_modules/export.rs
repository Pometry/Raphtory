use crate::{
    core::{ArcStr, Prop},
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps, PropUnwrap},
    python::graph::views::graph_view::PyGraphView,
};
use itertools::Itertools;
use pyo3::{
    prelude::*,
    pymethods,
    types::{PyDict, PyList, PyTuple},
    IntoPy, PyObject, PyResult, Python, ToPyObject,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use chrono::NaiveDateTime;
use crate::db::api::view::internal::CoreGraphOps;
use crate::prelude::TimeOps;
use rayon::prelude::*;
use std::sync::Mutex;

#[pymethods]
impl PyGraphView {
    /// Converts the graph's nodes into a Pandas DataFrame.
    ///
    /// This method will create a DataFrame with the following columns:
    /// - "name": The name of the node.
    /// - "properties": The properties of the node. This column will be included if `include_node_properties` is set to `true`.
    /// - "update_history": The update history of the node.
    ///
    /// Args:
    ///     include_property_histories (bool): A boolean, if set to `true`, the history of each property is included, if `false`, only the latest value is shown.  Defaults to `true`.
    ///     convert_datetime (bool): A boolean, if set to `true` will convert the timestamp to python datetimes, defaults to `false`
    ///     explode (bool): A boolean, if set to `true`, will explode each node update into its own row. Defaults to `false`
    ///
    /// Returns:
    ///     If successful, this PyObject will be a Pandas DataFrame.
    #[pyo3(signature = (include_property_histories=true, convert_datetime=false, explode=false))]
    pub fn to_node_df(&self, include_property_histories: bool, convert_datetime: bool, explode: bool) -> PyResult<PyObject> {
        let mut column_names = vec![String::from("name"), String::from("type")];
        let mut is_prop_both_temp_and_const: HashSet<String> = HashSet::new();

        // Adjusted to check both temporal and constant properties
        let temporal_properties : HashSet<ArcStr> = self.graph.node_meta().temporal_prop_meta().get_keys().iter().cloned().collect();
        let constant_properties : HashSet<ArcStr> = self.graph.node_meta().const_prop_meta().get_keys().iter().cloned().collect();

        constant_properties.intersection(&temporal_properties).into_iter().for_each(|name| {
            column_names.push(format!("{}_constant", name));
            column_names.push(format!("{}_temporal", name));
            is_prop_both_temp_and_const.insert(name.to_string());
        });
        constant_properties.symmetric_difference(&temporal_properties).into_iter().for_each(|name| {
            column_names.push(name.to_string());
        });
        column_names.push("update_histories".parse().unwrap());

        let mut node_tuples: Vec<Vec<Prop>> = vec![];
        
        self.graph.nodes().iter().for_each(|v| {
            let properties = v.properties().constant().as_map();
            let properties_collected: Vec<(String, Prop)> = properties.par_iter()
                .map(|(name, prop)| {
                    let column_name = if is_prop_both_temp_and_const.contains(name.as_ref()) {
                        format!("{}_constant", name)
                    } else {
                        name.to_string()
                    };
                    (column_name, prop.clone())
                })
                .collect();
            let mut properties_map: HashMap<String, Prop> = properties_collected.into_iter().collect();
            
            let mut prop_time_dict: HashMap<i64, HashMap<String, Prop>> = HashMap::new();
            if explode {
                let mut empty_dict = HashMap::new();
                column_names.clone().iter().for_each(|name| {
                    let _  = empty_dict.insert(name.clone(), Prop::from(""));
                });
                
                if v.properties().temporal().iter().count() == 0 {
                    let first_time = v.start().unwrap_or(0);
                    if v.properties().constant().iter().count() == 0 {
                        // node is empty so add as empty time
                        let _ = prop_time_dict.insert(first_time, empty_dict.clone());
                    } else {
                        v.properties().constant().iter().for_each(|(name, prop_val)| {
                            if !prop_time_dict.contains_key(&first_time) {
                                let _ = prop_time_dict.insert(first_time, empty_dict.clone());
                            }
                            let data_dict = prop_time_dict.get_mut(&0i64).unwrap();
                            let _ = data_dict.insert(name.to_string(), prop_val);
                        })
                    }
                }
                v.properties()
                    .temporal()
                    .histories()
                    .iter()
                    .for_each(|(prop_name, (time, prop_val))| {
                        let column_name = if is_prop_both_temp_and_const.contains(prop_name.as_ref()) {
                            format!("{}_temporal", prop_name)
                        } else {
                            prop_name.to_string()
                        };
                        if !prop_time_dict.contains_key(time) {
                            prop_time_dict.insert(*time, empty_dict.clone());
                        }
                        let data_dict = prop_time_dict.get_mut(&time).unwrap();
                        let _ = data_dict.insert(column_name.clone(), prop_val.clone());
                    });
            }
            else if include_property_histories {
                v.properties()
                    .temporal()
                    .iter()
                    .for_each(|(name, prop_view)| {
                        let column_name = if is_prop_both_temp_and_const.contains(name.as_ref()) {
                            format!("{}_temporal", name)
                        } else {
                            name.to_string()
                        };
                        if convert_datetime {
                            let mut prop_vec = vec![];
                            prop_view.iter().for_each(|(time, prop)| {
                                let new_time = NaiveDateTime::from_timestamp_opt(time, 0).unwrap();
                                let prop_time = Prop::DTime(new_time);
                                prop_vec.push(Prop::List(Arc::from(vec![prop_time, prop])))
                            });
                            let wrapped = Prop::from(prop_vec);
                            let _ = properties_map.insert(column_name, wrapped);
                        } else {
                            let vec_props = prop_view.iter().map(|(k, v)| Prop::from(vec![Prop::from(k), v])).collect_vec();
                            let wrapped = Prop::List(Arc::from(vec_props));
                            let _ = properties_map.insert(column_name, wrapped);
                        }
                    });
            } else {
                v.properties()
                    .temporal()
                    .iter()
                    .for_each(|(name, t_prop)| {
                        let column_name = if is_prop_both_temp_and_const.contains(name.as_ref()) {
                            format!("{}_temporal", name)
                        } else {
                            name.to_string()
                        };
                        let _ = properties_map.insert(column_name, t_prop.latest().unwrap_or(Prop::from("")));
                    });

            }

            if explode {
                let new_rows: Vec<Vec<Prop>> = prop_time_dict.par_iter().map(|(time, item_dict)| {
                    let mut row: Vec<Prop> = vec![
                        Prop::from(v.name()),
                        Prop::from(v.node_type().unwrap_or_else(|| ArcStr::from("")))
                    ];

                    for prop_name in &column_names[2..column_names.len() - 1] {
                        if let Some(prop_val) = properties_map.get(prop_name) {
                            row.push(prop_val.clone());
                        } else if let Some(prop_val) = item_dict.get(prop_name) {
                            row.push(prop_val.clone());
                        } else {
                            row.push(Prop::from(""));
                        }
                    }

                    if convert_datetime {
                        let new_time = NaiveDateTime::from_timestamp_opt(*time, 0).unwrap();
                        row.push(Prop::DTime(new_time));
                    } else {
                        row.push(Prop::from(*time));
                    }

                    row
                }).collect();
                node_tuples.extend(new_rows);
            } else {
                let mut row: Vec<Prop> = vec![Prop::from(v.name()), Prop::from(v.node_type().unwrap_or(ArcStr::from("")))];
                // Flatten properties into the row
                for prop_name in &column_names[2..column_names.len() - 1] {
                    // Skip the first column (name)
                    let blank_prop = Prop::from("");
                    let prop_value = properties_map
                        .get(prop_name)
                        .unwrap_or(&blank_prop);
                    let _ = row.push(prop_value.clone()); // Append property value as string
                }

                if convert_datetime {
                    let update_list = v.history().iter().map(|val| {
                        let new_time = NaiveDateTime::from_timestamp_opt(*val, 0).unwrap();
                        Prop::DTime(new_time)
                    }).collect_vec();
                    let _ = row.push(Prop::from(update_list));
                } else {
                    let update_list = Prop::from(v.history().iter().map(|&val| Prop::from(val)).collect_vec());
                    let _ = row.push(update_list);
                }
                node_tuples.push(row);
            }
        });

        Python::with_gil(|py| {
            let kwargs = PyDict::new(py);
            kwargs.set_item("columns", column_names.clone())?;
            let pandas = PyModule::import(py, "pandas")?;
            let df_data = pandas.call_method("DataFrame", (node_tuples,), Some(kwargs))?;
            Ok(df_data.to_object(py))
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

    /// Draw a graph with PyVis.
    /// Pyvis is a required dependency. If you intend to use this function make sure that you install Pyvis
    /// with ``pip install pyvis``
    ///
    ///     Args:
    ///         graph (graph): A Raphtory graph.
    ///         explode_edges (bool): A boolean that is set to True if you want to explode the edges in the graph. By default this is set to False.
    ///         edge_color (str): A string defining the colour of the edges in the graph. By default ``#000000`` (black) is set.
    ///         shape (str): An optional string defining what the node looks like.
    ///             There are two types of nodes. One type has the label inside of it and the other type has the label underneath it.
    ///             The types with the label inside of it are: ellipse, circle, database, box, text.
    ///             The ones with the label outside of it are: image, circularImage, diamond, dot, star, triangle, triangleDown, square and icon.
    ///             By default ``"dot"`` is set.
    ///         node_image (str): An optional string defining the url of a custom node image. By default an image of a circle is set.
    ///         edge_weight (str): An optional string defining the name of the property where edge weight is set on your Raphtory graph. By default ``1`` is set.
    ///         edge_label (str): An optional string defining the name of the property where edge label is set on your Raphtory graph. By default, an empty string as the label is set.
    ///         notebook (bool): A boolean that is set to True if using jupyter notebook. By default this is set to True.
    ///         kwargs: Additional keyword arguments that are passed to the pyvis Network class.
    ///
    ///     Returns:
    ///         A pyvis network
    #[pyo3(signature = (explode_edges=false, edge_color="#000000", shape=None, node_image=None, edge_weight=None, edge_label=None, colour_nodes_by_type=false, notebook=true, **kwargs))]
    pub fn to_pyvis(
        &self,
        explode_edges: Option<bool>,
        edge_color: Option<&str>,
        shape: Option<&str>,
        node_image: Option<&str>,
        edge_weight: Option<&str>,
        edge_label: Option<&str>,
        colour_nodes_by_type: Option<bool>,
        notebook: Option<bool>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let pyvis = PyModule::import(py, "pyvis.network")?;
            let network = pyvis.getattr("Network")?;
            let vis_graph = network.call(("notebook", notebook.unwrap_or(true)), kwargs)?;
            let mut groups = HashMap::new();
            if colour_nodes_by_type.unwrap_or(false) {
                let mut index = 1;
                for node in self.graph.nodes() {
                    let value = node.node_type().unwrap_or(ArcStr::from("_default"));
                    groups.insert(value, index);
                    index += 1;
                }
            }

            let mut colours = HashMap::new();
            let mut colour_index = 1;
            colours.insert(ArcStr::from("_default"), 0);

            for v in self.graph.nodes() {
                let image = match node_image {
                    Some(image) => v.properties().get(image).unwrap_or(Prop::from(
                        "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
                    )),
                    None => Prop::from("https://cdn-icons-png.flaticon.com/512/7584/7584620.png"),
                };
                let shape = shape.unwrap_or("dot");
                let kwargs_node = PyDict::new(py);
                kwargs_node.set_item("label", v.name())?;
                kwargs_node.set_item("shape", shape)?;
                kwargs_node.set_item("image", image)?;
                if colour_nodes_by_type.unwrap_or(false) {
                    let node_type = v.node_type().unwrap_or(ArcStr::from("_default"));
                    let group = match colours.get(&node_type) {
                        None => {
                            colours.insert(node_type, colour_index);
                            let to_return = colour_index;
                            colour_index += 1;
                            to_return
                        }
                        Some(colour) => *colour,
                    };
                    kwargs_node.set_item("group", group)?;
                    vis_graph.call_method("add_node", (v.id(),), Some(kwargs_node))?;
                } else {
                    vis_graph.call_method("add_node", (v.id(),), Some(kwargs_node))?;
                }
            }
            let edges = if explode_edges.unwrap_or(false) {
                self.graph.edges().explode()
            } else {
                self.graph.edges().explode_layers()
            };
            for edge in edges {
                let weight = match edge_weight {
                    Some(weight) => {
                        let w = edge.properties().get(weight).unwrap_or(Prop::from(0.0f64));
                        w.unwrap_f64()
                    }
                    None => 0.0f64,
                };
                let label = match edge_label {
                    Some(label) => {
                        let l = edge.properties().get(label).unwrap_or(Prop::from(""));
                        l.unwrap_str()
                    }
                    None => ArcStr::from(""),
                };
                let kwargs = PyDict::new(py);
                kwargs.set_item("value", weight)?;
                let edge_col = edge_color.unwrap_or("#000000");
                kwargs.set_item("color", edge_col)?;
                kwargs.set_item("title", label)?;
                kwargs.set_item("arrowStrikethrough", false)?;
                vis_graph.call_method(
                    "add_edge",
                    (edge.src().id(), edge.dst().id()),
                    Some(kwargs),
                )?;
            }
            Ok(vis_graph.to_object(py))
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
                match v.node_type() {
                    None => {}
                    Some(n_type) => {
                        properties
                            .set_item("node_type", n_type)
                            .expect("Failed to add property");
                    }
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
