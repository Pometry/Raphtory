use crate::core::Prop;
use crate::db::view_api::edge::EdgeListOps;
use crate::db::view_api::{GraphViewOps, TimeOps};
use std::collections::HashMap;

/// Operations defined for a vertex
pub trait VertexViewOps: TimeOps {
    type Graph: GraphViewOps;
    type ValueType<T>;
    type PathType<'a>: VertexViewOps<Graph = Self::Graph> + 'a
    where
        Self: 'a;
    type EList: EdgeListOps<Graph = Self::Graph>;

    /// Get the numeric id of the vertex
    fn id(&self) -> Self::ValueType<u64>;

    /// Get the name of this vertex if a user has set one otherwise it returns the ID.
    ///
    /// # Returns
    ///
    /// The name of the vertex if one exists, otherwise the ID as a string.
    fn name(&self) -> Self::ValueType<String>;

    /// Get the timestamp for the earliest activity of the vertex
    fn earliest_time(&self) -> Self::ValueType<Option<i64>>;

    /// Get the timestamp for the latest activity of the vertex
    fn latest_time(&self) -> Self::ValueType<Option<i64>>;

    /// Gets the property value of this vertex given the name of the property.
    fn property(&self, name: String, include_static: bool) -> Self::ValueType<Option<Prop>>;

    /// Gets the history of the vertex (time that the vertex was added and times when changes were made to the vertex)
    fn history(&self) -> Self::ValueType<Vec<i64>>;

    /// Get the temporal property value of this vertex.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve.
    ///
    /// # Returns
    ///
    /// A vector of `(i64, Prop)` tuples where the `i64` value is the timestamp of the
    /// property value and `Prop` is the value itself.
    fn property_history(&self, name: String) -> Self::ValueType<Vec<(i64, Prop)>>;

    /// Get all property values of this vertex.
    ///
    /// # Arguments
    ///
    /// * `include_static` - If `true` then static properties are included in the result.
    ///
    /// # Returns
    ///
    /// A HashMap with the names of the properties as keys and the property values as values.
    fn properties(&self, include_static: bool) -> Self::ValueType<HashMap<String, Prop>>;

    /// Get all temporal property values of this vertex.
    ///
    /// # Returns
    ///
    /// A HashMap with the names of the properties as keys and a vector of `(i64, Prop)` tuples
    /// as values. The `i64` value is the timestamp of the property value and `Prop`
    /// is the value itself.
    fn property_histories(&self) -> Self::ValueType<HashMap<String, Vec<(i64, Prop)>>>;

    /// Get the names of all properties of this vertex.
    ///
    /// # Arguments
    ///
    /// * `include_static` - If `true` then static properties are included in the result.
    ///
    /// # Returns
    ///
    /// A vector of the names of the properties of this vertex.
    fn property_names(&self, include_static: bool) -> Self::ValueType<Vec<String>>;

    /// Checks if a property exists on this vertex.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to check for.
    /// * `include_static` - If `true` then static properties are included in the result.
    ///
    /// # Returns
    ///
    /// `true` if the property exists, otherwise `false`.
    fn has_property(&self, name: String, include_static: bool) -> Self::ValueType<bool>;

    /// Checks if a static property exists on this vertex.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to check for.
    ///
    /// # Returns
    ///
    /// `true` if the property exists, otherwise `false`.
    fn has_static_property(&self, name: String) -> Self::ValueType<bool>;

    /// Get the static property value of this vertex.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve.
    ///
    /// # Returns
    ///
    /// The value of the property if it exists, otherwise `None`.
    fn static_property(&self, name: String) -> Self::ValueType<Option<Prop>>;

    /// Get the static properties value of this vertex.
    ///
    /// # Arguments
    ///
    ///
    /// # Returns
    ///
    /// HashMap<String, Prop> - Static properties identified by their name
    fn static_properties(&self) -> Self::ValueType<HashMap<String, Prop>>;

    /// Get the degree of this vertex (i.e., the number of edges that are incident to it).
    ///
    /// # Returns
    ///
    /// The degree of this vertex.
    fn degree(&self) -> Self::ValueType<usize>;

    /// Get the in-degree of this vertex (i.e., the number of edges that point into it).
    ///
    /// # Returns
    ///
    /// The in-degree of this vertex.
    fn in_degree(&self) -> Self::ValueType<usize>;

    /// Get the out-degree of this vertex (i.e., the number of edges that point out of it).
    ///
    /// # Returns
    ///
    /// The out-degree of this vertex.
    fn out_degree(&self) -> Self::ValueType<usize>;

    /// Get the edges that are incident to this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that are incident to this vertex.
    fn edges(&self) -> Self::EList;

    /// Get the edges that point into this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that point into this vertex.
    fn in_edges(&self) -> Self::EList;

    /// Get the edges that point out of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that point out of this vertex.
    fn out_edges(&self) -> Self::EList;

    /// Get the neighbours of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex.
    fn neighbours(&self) -> Self::PathType<'_>;

    /// Get the neighbours of this vertex that point into this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex that point into this vertex.
    fn in_neighbours(&self) -> Self::PathType<'_>;

    /// Get the neighbours of this vertex that point out of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex that point out of this vertex.
    fn out_neighbours(&self) -> Self::PathType<'_>;
}

/// A trait for operations on a list of vertices.
pub trait VertexListOps:
    IntoIterator<Item = Self::ValueType<Self::Vertex>, IntoIter = Self::IterType<Self::Vertex>> + Sized
{
    type Graph: GraphViewOps;
    type Vertex: VertexViewOps<Graph = Self::Graph>;
    /// The type of the iterator for the list of vertices
    type IterType<T>: Iterator<Item = Self::ValueType<T>>;
    /// The type of the iterator for the list of edges
    type EList: EdgeListOps<Graph = Self::Graph, Vertex = Self::Vertex>;
    type ValueType<T>;

    /// Return the timestamp of the earliest activity.
    fn earliest_time(self) -> Self::IterType<Option<i64>>;

    /// Return the timestamp of the latest activity.
    fn latest_time(self) -> Self::IterType<Option<i64>>;

    /// Create views for the vertices including all events between `t_start` (inclusive) and `t_end` (exclusive)
    fn window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType>;

    /// Create views for the vertices including all events until `end` (inclusive)
    fn at(self, end: i64) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType> {
        self.window(i64::MIN, end.saturating_add(1))
    }

    /// Returns the ids of vertices in the list.
    ///
    /// # Returns
    /// The ids of vertices in the list.
    fn id(self) -> Self::IterType<u64>;
    fn name(self) -> Self::IterType<String>;

    fn property(self, name: String, include_static: bool) -> Self::IterType<Option<Prop>>;

    /// Returns an iterator of the values of the given property name
    /// including the times when it changed
    ///
    /// # Arguments
    /// * `name` - The name of the property.
    ///
    /// # Returns
    /// An iterator of the values of the given property name including the times when it changed
    /// as a vector of tuples of the form (time, property).
    fn property_history(self, name: String) -> Self::IterType<Vec<(i64, Prop)>>;
    fn properties(self, include_static: bool) -> Self::IterType<HashMap<String, Prop>>;
    fn history(self) -> Self::IterType<Vec<i64>>;
    /// Returns an iterator over all vertex properties.
    ///
    /// # Returns
    /// An iterator over all vertex properties.
    fn property_histories(self) -> Self::IterType<HashMap<String, Vec<(i64, Prop)>>>;
    fn property_names(self, include_static: bool) -> Self::IterType<Vec<String>>;
    fn has_property(self, name: String, include_static: bool) -> Self::IterType<bool>;

    fn has_static_property(self, name: String) -> Self::IterType<bool>;

    /// Get static property of a vertex by name
    ///
    /// # Arguments
    /// - name - Name of the static property
    ///
    /// # Returns
    /// - Option<Prop> - Static property if found
    fn static_property(self, name: String) -> Self::IterType<Option<Prop>>;

    /// Get all static properties of a vertex
    ///
    /// # Arguments
    ///
    /// # Returns
    /// - HashMap<String, Prop> - All static properties of a vertex
    fn static_properties(self) -> Self::IterType<HashMap<String, Prop>>;

    /// Returns an iterator over the degree of the vertices.
    ///
    /// # Returns
    /// An iterator over the degree of the vertices.
    fn degree(self) -> Self::IterType<usize>;

    /// Returns an iterator over the in-degree of the vertices.
    /// The in-degree of a vertex is the number of edges that connect to it from other vertices.
    ///
    /// # Returns
    /// An iterator over the in-degree of the vertices.
    fn in_degree(self) -> Self::IterType<usize>;

    /// Returns an iterator over the out-degree of the vertices.
    /// The out-degree of a vertex is the number of edges that connects to it from the vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the out-degree of the vertices.
    fn out_degree(self) -> Self::IterType<usize>;

    /// Returns an iterator over the edges of the vertices.
    fn edges(self) -> Self::EList;

    /// Returns an iterator over the incoming edges of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the incoming edges of the vertices.
    fn in_edges(self) -> Self::EList;

    /// Returns an iterator over the outgoing edges of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the outgoing edges of the vertices.
    fn out_edges(self) -> Self::EList;

    /// Returns an iterator over the neighbours of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of the vertices as VertexViews.
    fn neighbours(self) -> Self;

    /// Returns an iterator over the incoming neighbours of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the incoming neighbours of the vertices as VertexViews.
    fn in_neighbours(self) -> Self;

    /// Returns an iterator over the outgoing neighbours of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the outgoing neighbours of the vertices as VertexViews.
    fn out_neighbours(self) -> Self;
}
