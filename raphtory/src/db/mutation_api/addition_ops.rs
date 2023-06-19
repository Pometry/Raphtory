use crate::core::time::TryIntoTime;
use crate::core::vertex::InputVertex;

pub trait AdditionOps {
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
    /// use raphtory::db::graph::InternalGraph;
    /// let g = InternalGraph::new(1);
    /// let v = g.add_vertex(0, "Alice", &vec![]);
    /// let v = g.add_vertex(0, 5, &vec![]);
    /// ```
    fn add_vertex<V: InputVertex, T: TryIntoTime>(
        &self,
        t: T,
        v: V,
        props: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        let shard_id = utils::get_shard_id_from_global_vid(v.id(), self.nr_shards);
        self.shards[shard_id].add_vertex(t.try_into_time()?, v, props)
    }

    pub fn add_vertex_with_custom_time_format<V: InputVertex>(
        &self,
        t: &str,
        fmt: &str,
        v: V,
        props: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_vertex(time, v, props)
    }

    /// Adds properties to the given input vertex.
    ///
    /// # Arguments
    ///
    /// * `v` - A vertex
    /// * `data` - A vector of tuples containing the property name and value pairs to add to the vertex.
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::db::graph::InternalGraph;
    /// use raphtory::core::Prop;
    /// let graph = InternalGraph::new(1);
    /// graph.add_vertex(0, "Alice", &vec![]);
    /// let properties = vec![("color".to_owned(), Prop::Str("blue".to_owned())), ("weight".to_owned(), Prop::I64(11))];
    /// let result = graph.add_vertex_properties("Alice", &properties);
    /// ```
    pub fn add_vertex_properties<V: InputVertex>(
        &self,
        v: V,
        data: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        let shard_id = utils::get_shard_id_from_global_vid(v.id(), self.nr_shards);
        self.shards[shard_id].add_vertex_properties(v.id(), data)
    }

    pub fn add_property<T: TryIntoTime>(
        &self,
        t: T,
        props: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        let shard_id = utils::get_shard_id_from_global_vid(0, self.nr_shards);
        self.shards[shard_id].add_property(t.try_into_time()?, props)
    }

    pub fn add_static_property(&self, props: &Vec<(String, Prop)>) -> Result<(), GraphError> {
        let shard_id = utils::get_shard_id_from_global_vid(0, self.nr_shards);
        self.shards[shard_id].add_static_property(props)
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
    /// use raphtory::db::graph::InternalGraph;
    ///
    /// let graph = InternalGraph::new(1);
    /// graph.add_vertex(1, "Alice", &vec![]).unwrap();
    /// graph.add_vertex(2, "Bob", &vec![]).unwrap();
    /// graph.add_edge(3, "Alice", "Bob", &vec![], None).unwrap();
    /// ```    
    pub fn add_edge<V: InputVertex, T: TryIntoTime>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: &Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let time = t.try_into_time()?;
        let src_shard_id = utils::get_shard_id_from_global_vid(src.id(), self.nr_shards);
        let dst_shard_id = utils::get_shard_id_from_global_vid(dst.id(), self.nr_shards);

        let layer_id = self.get_or_allocate_layer(layer);

        if src_shard_id == dst_shard_id {
            self.shards[src_shard_id].add_edge(time, src, dst, props, layer_id)
        } else {
            // FIXME these are sort of connected, we need to hold both locks for
            // the src partition and dst partition to add a remote edge between both
            self.shards[src_shard_id].add_edge_remote_out(
                time,
                src.clone(),
                dst.clone(),
                props,
                layer_id,
            )?;
            self.shards[dst_shard_id].add_edge_remote_into(time, src, dst, props, layer_id)?;
            Ok(())
        }
    }

    pub fn delete_edge<V: InputVertex, T: TryIntoTime>(
        &self,
        t: T,
        src: V,
        dst: V,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let time = t.try_into_time()?;
        let src_shard_id = utils::get_shard_id_from_global_vid(src.id(), self.nr_shards);
        let dst_shard_id = utils::get_shard_id_from_global_vid(dst.id(), self.nr_shards);

        let layer_id = self.get_or_allocate_layer(layer);

        if src_shard_id == dst_shard_id {
            self.shards[src_shard_id].delete_edge(time, src, dst, layer_id)
        } else {
            // FIXME these are sort of connected, we need to hold both locks for
            // the src partition and dst partition to add a remote edge between both
            self.shards[src_shard_id].delete_edge_remote_out(
                time,
                src.clone(),
                dst.clone(),
                layer_id,
            )?;
            self.shards[dst_shard_id].delete_edge_remote_into(time, src, dst, layer_id)?;
            Ok(())
        }
    }

    pub fn add_edge_with_custom_time_format<V: InputVertex>(
        &self,
        t: &str,
        fmt: &str,
        src: V,
        dst: V,
        props: &Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_edge(time, src, dst, props, layer)
    }

    pub fn delete_edge_with_custom_time_format<V: InputVertex>(
        &self,
        t: &str,
        fmt: &str,
        src: V,
        dst: V,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.delete_edge(time, src, dst, layer)
    }

    /// Adds properties to an existing edge between a source and destination vertices
    ///
    /// # Arguments
    ///
    /// * `src` - An instance of `T` that implements the `InputVertex` trait representing the source vertex.
    /// * `dst` - An instance of `T` that implements the `InputVertex` trait representing the destination vertex.
    /// * `props` - A vector of tuples containing the property name and value pairs to add to the edge.
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::db::graph::InternalGraph;
    /// use raphtory::core::Prop;
    /// let graph = InternalGraph::new(1);
    /// graph.add_vertex(1, "Alice", &vec![]);
    /// graph.add_vertex(2, "Bob", &vec![]);
    /// graph.add_edge(3, "Alice", "Bob", &vec![], None);
    /// let properties = vec![("price".to_owned(), Prop::I64(100))];
    /// let result = graph.add_edge_properties("Alice", "Bob", &properties, None);
    /// ```
    pub fn add_edge_properties<V: InputVertex>(
        &self,
        src: V,
        dst: V,
        props: &Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let layer_id = self.get_layer_id(layer).unwrap(); // FIXME: bubble up instead

        // TODO: we don't add properties to dst shard, but may need to depending on the plans
        self.get_shard_from_id(src.id())
            .add_edge_properties(src.id(), dst.id(), props, layer_id)
    }
}
