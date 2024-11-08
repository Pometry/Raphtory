use super::time_from_input;
use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_ref::AsNodeRef},
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
    prelude::NodeViewOps,
};
use raphtory_api::core::storage::dict_mapper::MaybeNew::{Existing, New};

pub trait AdditionOps: StaticGraphViewOps {
    // TODO: Probably add vector reference here like add
    /// Add a node to the graph
    ///
    /// # Arguments
    ///
    /// * `t` - The time
    /// * `v` - The node (can be a string or integer)
    /// * `props` - The properties of the node
    /// * `node_type` - The optional string which will be used as a node type
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
    /// let v = g.add_node(0, "Alice", NO_PROPS, None);
    /// let v = g.add_node(0, 5, NO_PROPS, None);
    /// ```
    fn add_node<V: AsNodeRef, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        v: V,
        props: PI,
        node_type: Option<&str>,
    ) -> Result<NodeView<Self, Self>, GraphError>;

    fn create_node<V: AsNodeRef, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        v: V,
        props: PI,
        node_type: Option<&str>,
    ) -> Result<NodeView<Self, Self>, GraphError>;

    fn add_node_with_custom_time_format<V: AsNodeRef, PI: CollectProperties>(
        &self,
        t: &str,
        fmt: &str,
        v: V,
        props: PI,
        node_type: Option<&str>,
    ) -> Result<NodeView<Self, Self>, GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_node(time, v, props, node_type)
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
    /// graph.add_node(1, "Alice", NO_PROPS, None).unwrap();
    /// graph.add_node(2, "Bob", NO_PROPS, None).unwrap();
    /// graph.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
    /// ```    
    fn add_edge<V: AsNodeRef, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<EdgeView<Self, Self>, GraphError>;

    fn add_edge_with_custom_time_format<V: AsNodeRef, PI: CollectProperties>(
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
    fn add_node<V: AsNodeRef, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        v: V,
        props: PI,
        node_type: Option<&str>,
    ) -> Result<NodeView<G, G>, GraphError> {
        let ti = time_from_input(self, t)?;
        let properties = props.collect_properties(|name, dtype| {
            Ok(self.resolve_node_property(name, dtype, false)?.inner())
        })?;
        let v_id = match node_type {
            None => self.resolve_node(v)?.inner(),
            Some(node_type) => {
                let (v_id, _) = self.resolve_node_and_type(v, node_type)?.inner();
                v_id.inner()
            }
        };
        self.internal_add_node(ti, v_id, &properties)?;
        Ok(NodeView::new_internal(self.clone(), v_id))
    }

    fn create_node<V: AsNodeRef, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        v: V,
        props: PI,
        node_type: Option<&str>,
    ) -> Result<NodeView<G, G>, GraphError> {
        let ti = time_from_input(self, t)?;
        let v_id = match node_type {
            None => self.resolve_node(v)?,
            Some(node_type) => {
                let (v_id, _) = self.resolve_node_and_type(v, node_type)?.inner();
                v_id
            }
        };
        match v_id {
            New(id) => {
                let properties = props.collect_properties(|name, dtype| {
                    Ok(self.resolve_node_property(name, dtype, false)?.inner())
                })?;
                self.internal_add_node(ti, id, &properties)?;
                Ok(NodeView::new_internal(self.clone(), id))
            }
            Existing(id) => {
                let node_id = self.node(id).unwrap().id();
                Err(GraphError::NodeExistsError(node_id))
            }
        }
    }

    fn add_edge<V: AsNodeRef, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<EdgeView<G, G>, GraphError> {
        let ti = time_from_input(self, t)?;
        let src_id = self.resolve_node(src)?.inner();
        let dst_id = self.resolve_node(dst)?.inner();
        let layer_id = self.resolve_layer(layer)?.inner();

        let properties: Vec<(usize, Prop)> = props.collect_properties(|name, dtype| {
            Ok(self.resolve_edge_property(name, dtype, false)?.inner())
        })?;
        let eid = self
            .internal_add_edge(ti, src_id, dst_id, &properties, layer_id)?
            .inner();
        Ok(EdgeView::new(
            self.clone(),
            EdgeRef::new_outgoing(eid, src_id, dst_id).at_layer(layer_id),
        ))
    }
}
