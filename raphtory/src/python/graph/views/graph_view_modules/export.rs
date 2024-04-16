use crate::{
    core::{ArcStr, Prop},
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps, PropUnwrap},
    python::graph::views::graph_view::PyGraphView,
};
use pyo3::{
    prelude::*,
    pymethods,
    types::{PyDict, PyList, PyTuple},
    IntoPy, PyObject, PyResult, Python, ToPyObject,
};
use std::collections::HashMap;

#[pymethods]
impl PyGraphView {
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
    #[pyo3(signature = (explode_edges=false, edge_color="#000000", shape=None, node_image=None, edge_weight=None, edge_label=None, colour_nodes_by_type=false, notebook=false, **kwargs))]
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
    ///         include_property_history (bool): A boolean that is set to True if you want to include the histories in the graph. By default this is set to True.
    ///
    ///     Returns:
    ///         A Networkx MultiDiGraph.
    #[pyo3(signature = (explode_edges=false, include_node_properties=true, include_edge_properties=true,include_update_history=true,include_property_history=true))]
    pub fn to_networkx(
        &self,
        explode_edges: Option<bool>,
        include_node_properties: Option<bool>,
        include_edge_properties: Option<bool>,
        include_update_history: Option<bool>,
        include_property_history: Option<bool>,
    ) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let networkx = py.import("networkx")?.getattr("MultiDiGraph")?.call0()?;

            let mut node_tuples = Vec::new();
            for v in self.graph.nodes().iter() {
                let properties = PyDict::new(py);
                if include_node_properties.unwrap_or(true) {
                    if include_property_history.unwrap_or(true) {
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
                    if include_property_history.unwrap_or(true) {
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
