use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::input_node::InputNode, LayerIds},
        storage::timeindex::TimeIndexEntry,
        utils::{
            errors::{
                GraphError,
                GraphError::{EdgeExistsError, NodeExistsError},
            },
            time::IntoTimeWithFormat,
        },
        Prop,
    },
    db::{
        api::{
            mutation::{
                internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                CollectProperties, TryIntoInputTime,
            },
            view::{IntoDynamic, StaticGraphViewOps},
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
};

pub trait AdditionOps: StaticGraphViewOps {
    // TODO: Probably add vector reference here like add
    /// Add a node to the graph
    ///
    /// # Arguments
    ///
    /// * `t` - The time
    /// * `v` - The node (can be a string or integer)
    /// * `props` - The properties of the node
    ///
    /// Returns:
    ///
    /// A result containing the node id
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::prelude::*;
    /// let g = Graph::new();
    /// let v = g.add_node(0, "Alice", NO_PROPS);
    /// let v = g.add_node(0, 5, NO_PROPS);
    /// ```
    fn add_node<V: InputNode, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        v: V,
        props: PI,
    ) -> Result<NodeView<Self, Self>, GraphError>;

    fn add_node_with_custom_time_format<V: InputNode, PI: CollectProperties>(
        &self,
        t: &str,
        fmt: &str,
        v: V,
        props: PI,
    ) -> Result<NodeView<Self, Self>, GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_node(time, v, props)
    }

    // TODO: Node.name which gets ._id property else numba as string
    /// Adds an edge between the source and destination nodes with the given timestamp and properties.
    ///
    /// # Arguments
    ///
    /// * `t` - The timestamp of the edge.
    /// * `src` - An instance of `T` that implements the `InputNode` trait representing the source node.
    /// * `dst` - An instance of `T` that implements the `InputNode` trait representing the destination node.
    /// * `props` - A vector of tuples containing the property name and value pairs to add to the edge.
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::prelude::*;
    ///
    /// let graph = Graph::new();
    /// graph.add_node(1, "Alice", NO_PROPS).unwrap();
    /// graph.add_node(2, "Bob", NO_PROPS).unwrap();
    /// graph.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
    /// ```    
    fn add_edge<V: InputNode, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<EdgeView<Self, Self>, GraphError>;

    fn add_edge_with_custom_time_format<V: InputNode, PI: CollectProperties>(
        &self,
        t: &str,
        fmt: &str,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<EdgeView<Self, Self>, GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_edge(time, src, dst, props, layer)
    }

    /// Imports a single node into the graph.
    ///
    /// This function takes a reference to a node and an optional boolean flag `force`.
    /// If `force` is `Some(false)` or `None`, the function will return an error if the node already exists in the graph.
    /// If `force` is `Some(true)`, the function will overwrite the existing node in the graph.
    ///
    /// # Arguments
    ///
    /// * `node` - A reference to the node to be imported.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing node.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the node was successfully imported, and `Err` otherwise.
    fn import_node<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        node: &NodeView<GHH, GH>,
        force: Option<bool>,
    ) -> Result<NodeView<Self, Self>, GraphError>;

    /// Imports multiple nodes into the graph.
    ///
    /// This function takes a vector of references to nodes and an optional boolean flag `force`.
    /// If `force` is `Some(false)` or `None`, the function will return an error if any of the nodes already exist in the graph.
    /// If `force` is `Some(true)`, the function will overwrite the existing nodes in the graph.
    ///
    /// # Arguments
    ///
    /// * `nodes` - A vector of references to the nodes to be imported.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing nodes.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the nodes were successfully imported, and `Err` otherwise.
    fn import_nodes<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        node: Vec<&NodeView<GHH, GH>>,
        force: Option<bool>,
    ) -> Result<Vec<NodeView<Self, Self>>, GraphError>;

    fn import_edge<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        edge: &EdgeView<GHH, GH>,
        force: Option<bool>,
    ) -> Result<EdgeView<Self, Self>, GraphError>;
}

impl<G: InternalAdditionOps + StaticGraphViewOps + InternalPropertyAdditionOps> AdditionOps for G {
    fn add_node<V: InputNode, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        v: V,
        props: PI,
    ) -> Result<NodeView<G, G>, GraphError> {
        let properties = props.collect_properties(
            |name, dtype| self.resolve_node_property(name, dtype, false),
            |prop| self.process_prop_value(prop),
        )?;
        let ti = TimeIndexEntry::from_input(self, t)?;
        let v_id = self.resolve_node(v.id(), v.id_str());
        self.internal_add_node(ti, v_id, properties)?;
        Ok(NodeView::new_internal(self.clone(), v_id))
    }

    fn add_edge<V: InputNode, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<EdgeView<G, G>, GraphError> {
        let ti = TimeIndexEntry::from_input(self, t)?;
        let src_id = self.resolve_node(src.id(), src.id_str());
        let dst_id = self.resolve_node(dst.id(), dst.id_str());
        let layer_id = self.resolve_layer(layer);

        let properties: Vec<(usize, Prop)> = props.collect_properties(
            |name, dtype| self.resolve_edge_property(name, dtype, false),
            |prop| self.process_prop_value(prop),
        )?;
        let eid = self.internal_add_edge(ti, src_id, dst_id, properties, layer_id)?;
        Ok(EdgeView::new(
            self.clone(),
            EdgeRef::new_outgoing(eid, src_id, dst_id).at_layer(layer_id),
        ))
    }

    fn import_node<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        node: &NodeView<GHH, GH>,
        force: Option<bool>,
    ) -> Result<NodeView<G, G>, GraphError> {
        if force == Some(false) || force.is_none() {
            if self.node(node.id()).is_some() {
                return Err(NodeExistsError(node.id()));
            }
        }

        for h in node.history() {
            self.add_node(h, node.name(), NO_PROPS)?;
        }
        for (name, prop_view) in node.properties().temporal().iter() {
            for (t, prop) in prop_view.iter() {
                self.add_node(t, node.name(), [(name.clone(), prop)])?;
            }
        }
        self.node(node.id())
            .expect("node added")
            .add_constant_properties(node.properties().constant())?;

        Ok(self.node(node.id()).unwrap())
    }

    fn import_nodes<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        nodes: Vec<&NodeView<GHH, GH>>,
        force: Option<bool>,
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
        let mut added_nodes = vec![];
        for node in nodes {
            let res = self.import_node(node, force);
            added_nodes.push(res.unwrap())
        }
        Ok(added_nodes)
    }

    fn import_edge<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        edge: &EdgeView<GHH, GH>,
        force: Option<bool>,
    ) -> Result<EdgeView<Self, Self>, GraphError> {
        // make sure we preserve all layers even if they are empty
        // skip default layer
        for layer in edge.graph.unique_layers().skip(1) {
            self.resolve_layer(Some(&layer));
        }
        if force == Some(false) || force.is_none() {
            if self.edge(edge.src(), edge.dst()).is_some() {
                return Err(EdgeExistsError(edge.src().id(), edge.dst().id()));
            }
        }

        // Add edges first so we definitely have all associated nodes (important in case of persistent edges)
        for ee in edge.explode_layers() {
            let layer_id = *ee.edge.layer().expect("exploded layers");
            let layer_ids = LayerIds::One(layer_id);
            let layer_name = self.get_layer_name(layer_id);
            let layer_name: Option<&str> = if layer_id == 0 {
                None
            } else {
                Some(&layer_name)
            };

            for ee in ee.explode() {
                self.add_edge(
                    ee.time().expect("exploded edge"),
                    ee.src().name(),
                    ee.dst().name(),
                    ee.properties().temporal().collect_properties(),
                    layer_name,
                )?;
            }

            self.edge(ee.src().id(), ee.dst().id())
                .expect("edge added")
                .add_constant_properties(ee.properties().constant(), layer_name)?;
        }
        Ok(self.edge(edge.src(), edge.dst()).unwrap())
    }
}
