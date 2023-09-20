use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::input_vertex::InputVertex},
        storage::timeindex::TimeIndexEntry,
        utils::{errors::GraphError, time::IntoTimeWithFormat},
        Prop,
    },
    db::{
        api::mutation::{internal::InternalAdditionOps, CollectProperties, TryIntoInputTime},
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::GraphViewOps,
};

pub trait AdditionOps: GraphViewOps {
    // TODO: Probably add vector reference here like add
    /// Add a vertex to the graph
    ///
    /// # Arguments
    ///
    /// * `t` - The time
    /// * `v` - The vertex (can be a string or integer)
    /// * `props` - The properties of the vertex
    ///
    /// # Returns
    ///
    /// A result containing the vertex id
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::prelude::*;
    /// let g = Graph::new();
    /// let v = g.add_vertex(0, "Alice", NO_PROPS);
    /// let v = g.add_vertex(0, 5, NO_PROPS);
    /// ```
    fn add_vertex<V: InputVertex, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        v: V,
        props: PI,
    ) -> Result<VertexView<Self>, GraphError>;

    fn add_vertex_with_custom_time_format<V: InputVertex, PI: CollectProperties>(
        &self,
        t: &str,
        fmt: &str,
        v: V,
        props: PI,
    ) -> Result<VertexView<Self>, GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_vertex(time, v, props)
    }

    // TODO: Vertex.name which gets ._id property else numba as string
    /// Adds an edge between the source and destination vertices with the given timestamp and properties.
    ///
    /// # Arguments
    ///
    /// * `t` - The timestamp of the edge.
    /// * `src` - An instance of `T` that implements the `InputVertex` trait representing the source vertex.
    /// * `dst` - An instance of `T` that implements the `InputVertex` trait representing the destination vertex.
    /// * `props` - A vector of tuples containing the property name and value pairs to add to the edge.
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::prelude::*;
    ///
    /// let graph = Graph::new();
    /// graph.add_vertex(1, "Alice", NO_PROPS).unwrap();
    /// graph.add_vertex(2, "Bob", NO_PROPS).unwrap();
    /// graph.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
    /// ```    
    fn add_edge<V: InputVertex, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<EdgeView<Self>, GraphError>;

    fn add_edge_with_custom_time_format<V: InputVertex, PI: CollectProperties>(
        &self,
        t: &str,
        fmt: &str,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<EdgeView<Self>, GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_edge(time, src, dst, props, layer)
    }
}

impl<G: InternalAdditionOps + GraphViewOps> AdditionOps for G {
    fn add_vertex<V: InputVertex, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        v: V,
        props: PI,
    ) -> Result<VertexView<G>, GraphError> {
        let properties = props.collect_properties(
            |name, dtype| self.resolve_vertex_property(name, dtype, false),
            |prop| self.process_prop_value(prop),
        )?;
        let ti = TimeIndexEntry::from_input(self, t)?;
        let v_id = self.resolve_vertex(v.id(), v.id_str());
        self.internal_add_vertex(ti, v_id, properties)?;
        Ok(VertexView::new_internal(self.clone(), v_id))
    }

    fn add_edge<V: InputVertex, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<EdgeView<G>, GraphError> {
        let ti = TimeIndexEntry::from_input(self, t)?;
        let src_id = self.resolve_vertex(src.id(), src.id_str());
        let dst_id = self.resolve_vertex(dst.id(), dst.id_str());
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
