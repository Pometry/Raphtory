use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::input_node::InputNode},
        storage::timeindex::TimeIndexEntry,
        utils::{errors::GraphError, time::IntoTimeWithFormat},
        Prop,
    },
    db::{
        api::{
            mutation::{internal::InternalAdditionOps, CollectProperties, TryIntoInputTime},
            view::StaticGraphViewOps,
        },
        graph::{edge::EdgeView, node::NodeView},
    },
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
}

impl<G: InternalAdditionOps + StaticGraphViewOps> AdditionOps for G {
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
}
