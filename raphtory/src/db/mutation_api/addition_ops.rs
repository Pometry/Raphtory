use crate::core::tgraph_shard::errors::GraphError;
use crate::core::time::{IntoTimeWithFormat, TryIntoTime};
use crate::core::vertex::InputVertex;
use crate::core::Prop;
use crate::db::mutation_api::internal::InternalAdditionOps;
use crate::db::mutation_api::Properties;

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
    /// use raphtory::prelude::*;
    /// let g = Graph::new(1);
    /// let v = g.add_vertex(0, "Alice", []);
    /// let v = g.add_vertex(0, 5, []);
    /// ```
    fn add_vertex<V: InputVertex, T: TryIntoTime, P: Properties>(
        &self,
        t: T,
        v: V,
        props: P,
    ) -> Result<(), GraphError>;

    fn add_vertex_with_custom_time_format<V: InputVertex, P: Properties>(
        &self,
        t: &str,
        fmt: &str,
        v: V,
        props: P,
    ) -> Result<(), GraphError> {
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
    /// let graph = Graph::new(1);
    /// graph.add_vertex(1, "Alice", []).unwrap();
    /// graph.add_vertex(2, "Bob", []).unwrap();
    /// graph.add_edge(3, "Alice", "Bob", [], None).unwrap();
    /// ```    
    fn add_edge<V: InputVertex, T: TryIntoTime, P: Properties>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: P,
        layer: Option<&str>,
    ) -> Result<(), GraphError>;

    fn add_edge_with_custom_time_format<V: InputVertex, P: Properties>(
        &self,
        t: &str,
        fmt: &str,
        src: V,
        dst: V,
        props: P,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_edge(time, src, dst, props, layer)
    }
}

impl<G: InternalAdditionOps> AdditionOps for G {
    fn add_vertex<V: InputVertex, T: TryIntoTime, P: Properties>(
        &self,
        t: T,
        v: V,
        props: P,
    ) -> Result<(), GraphError> {
        let mut props: Vec<(String, Prop)> = props.collect_properties();
        self.internal_add_vertex(
            t.try_into_time()?,
            v.id(),
            v.id_str(),
            props.collect_properties(),
        )
    }

    fn add_edge<V: InputVertex, T: TryIntoTime, P: Properties>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: P,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let t = t.try_into_time()?;
        let src_id = src.id();
        let dst_id = dst.id();
        self.add_vertex(t, src, [])?;
        self.add_vertex(t, dst, [])?;
        self.internal_add_edge(t, src_id, dst_id, props.collect_properties(), layer)
    }
}
