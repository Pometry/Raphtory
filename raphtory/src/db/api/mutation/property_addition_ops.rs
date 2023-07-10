use crate::{
    core::{
        entities::vertices::input_vertex::InputVertex,
        utils::{errors::GraphError, time::TryIntoTime},
    },
    db::api::mutation::internal::InternalPropertyAdditionOps
};

use super::Properties;

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
    /// use raphtory::prelude::*;
    /// let graph = Graph::new();
    /// graph.add_vertex(0, "Alice", NO_PROPS);
    /// let properties = vec![("color".to_owned(), Prop::Str("blue".to_owned())), ("weight".to_owned(), Prop::I64(11))];
    /// let result = graph.add_vertex_properties("Alice", properties);
    /// ```
    fn add_vertex_properties<V: InputVertex, PI:Properties>(
        &self,
        v: V,
        data: PI,
    ) -> Result<(), GraphError>;

    fn add_properties<T: TryIntoTime, PI:Properties>(
        &self,
        t: T,
        props: PI,
    ) -> Result<(), GraphError>;

    fn add_static_properties<PI: Properties>(&self, props: PI) -> Result<(), GraphError>;

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
    /// use raphtory::prelude::*;
    /// let graph = Graph::new();
    /// graph.add_vertex(1, "Alice", NO_PROPS);
    /// graph.add_vertex(2, "Bob", NO_PROPS);
    /// graph.add_edge(3, "Alice", "Bob", NO_PROPS, None);
    /// let properties = vec![("price", 100)];
    /// let result = graph.add_edge_properties("Alice", "Bob", properties, None);
    /// ```
    fn add_edge_properties<V: InputVertex, PI: Properties>(
        &self,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<(), GraphError>;
}

impl<G: InternalPropertyAdditionOps> PropertyAdditionOps for G {

    fn add_vertex_properties<V: InputVertex, PI: Properties>(
        &self,
        v: V,
        data: PI,
    ) -> Result<(), GraphError> {
        self.internal_add_vertex_properties(v.id(), data.collect_properties())
    }

    fn add_properties<T: TryIntoTime, PI: Properties>(
        &self,
        t: T,
        props: PI,
    ) -> Result<(), GraphError> {
        self.internal_add_properties(t.try_into_time()?, props.collect_properties())
    }

    fn add_static_properties<PI: Properties>(&self, props: PI) -> Result<(), GraphError> {
        self.internal_add_static_properties(props.collect_properties())
    }

    fn add_edge_properties<V: InputVertex, PI: Properties>(
        &self,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.internal_add_edge_properties(src.id(), dst.id(), props.collect_properties(), layer)
    }

}
