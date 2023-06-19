use crate::core::tgraph_shard::errors::GraphError;
use crate::core::time::{IntoTimeWithFormat, TryIntoTime};
use crate::core::vertex::InputVertex;
use crate::core::Prop;

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
    ) -> Result<(), GraphError>;

    fn add_vertex_with_custom_time_format<V: InputVertex>(
        &self,
        t: &str,
        fmt: &str,
        v: V,
        props: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError>;

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
    fn add_edge<V: InputVertex, T: TryIntoTime>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: &Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError>;

    fn add_edge_with_custom_time_format<V: InputVertex>(
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
}
