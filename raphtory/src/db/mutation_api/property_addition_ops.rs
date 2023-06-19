use crate::core::tgraph_shard::errors::GraphError;
use crate::core::time::TryIntoTime;
use crate::core::vertex::InputVertex;
use crate::core::Prop;

pub trait PropertyAdditionOps {
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
    fn add_vertex_properties<V: InputVertex>(
        &self,
        v: V,
        data: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError>;

    fn add_property<T: TryIntoTime>(
        &self,
        t: T,
        props: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError>;

    fn add_static_property(&self, props: &Vec<(String, Prop)>) -> Result<(), GraphError>;

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
    fn add_edge_properties<V: InputVertex>(
        &self,
        src: V,
        dst: V,
        props: &Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError>;
}
