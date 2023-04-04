use crate::vertex::VertexView;
use crate::view_api::edge::EdgeListOps;
use crate::view_api::GraphViewOps;
use docbrown_core::Prop;
use std::collections::HashMap;

/// A trait for operations on a list of vertices.
pub trait VertexListOps:
    IntoIterator<Item = VertexView<Self::Graph>, IntoIter = Self::IterType> + Sized + Send
{
    type Graph: GraphViewOps;

    /// The type of the iterator for the list of vertices
    type IterType: Iterator<Item = VertexView<Self::Graph>> + Send;
    /// The type of the iterator for the list of edges
    type EList: EdgeListOps<Graph = Self::Graph>;
    type ValueIterType<U>: Iterator<Item = U> + Send;

    /// Returns the ids of vertices in the list.
    ///
    /// # Returns
    /// The ids of vertices in the list.
    fn id(self) -> Self::ValueIterType<u64>;
    fn name(self) -> Self::ValueIterType<String>;

    fn property(self, name: String, include_static: bool) -> Self::ValueIterType<Option<Prop>>;

    /// Returns an iterator of the values of the given property name
    /// including the times when it changed
    ///
    /// # Arguments
    /// * `name` - The name of the property.
    ///
    /// # Returns
    /// An iterator of the values of the given property name including the times when it changed
    /// as a vector of tuples of the form (time, property).
    fn property_history(self, name: String) -> Self::ValueIterType<Vec<(i64, Prop)>>;
    fn properties(self, include_static: bool) -> Self::ValueIterType<HashMap<String, Prop>>;

    /// Returns an iterator over all vertex properties.
    ///
    /// # Returns
    /// An iterator over all vertex properties.
    fn property_histories(self) -> Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>>;
    fn property_names(self, include_static: bool) -> Self::ValueIterType<Vec<String>>;
    fn has_property(self, name: String, include_static: bool) -> Self::ValueIterType<bool>;

    fn has_static_property(self, name: String) -> Self::ValueIterType<bool>;

    fn static_property(self, name: String) -> Self::ValueIterType<Option<Prop>>;

    /// Returns an iterator over the degree of the vertices.
    ///
    /// # Returns
    /// An iterator over the degree of the vertices.
    fn degree(self) -> Self::ValueIterType<usize>;

    /// Returns an iterator over the degree of the vertices within a time window.
    /// The degree of a vertex is the number of edges that connect to it in both directions.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the degree of the vertices within the given time window.
    fn degree_window(self, t_start: i64, t_end: i64) -> Self::ValueIterType<usize>;

    /// Returns an iterator over the in-degree of the vertices.
    /// The in-degree of a vertex is the number of edges that connect to it from other vertices.
    ///
    /// # Returns
    /// An iterator over the in-degree of the vertices.
    fn in_degree(self) -> Self::ValueIterType<usize>;

    /// Returns an iterator over the in-degree of the vertices within a time window.
    /// The in-degree of a vertex is the number of edges that connects to it from other vertices.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the in-degree of the vertices within the given time window.
    fn in_degree_window(self, t_start: i64, t_end: i64) -> Self::ValueIterType<usize>;

    /// Returns an iterator over the out-degree of the vertices.
    /// The out-degree of a vertex is the number of edges that connects to it from the vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the out-degree of the vertices.
    fn out_degree(self) -> Self::ValueIterType<usize>;

    /// Returns an iterator over the out-degree of the vertices within a time window.
    /// The out-degree of a vertex is the number of edges that connects to it from the vertex.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the out-degree of the vertices within the given time window.
    fn out_degree_window(self, t_start: i64, t_end: i64) -> Self::ValueIterType<usize>;

    /// Returns an iterator over the edges of the vertices.
    fn edges(self) -> Self::EList;

    /// Returns an iterator over the edges of the vertices within a time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the edges of the vertices within the given time window.
    fn edges_window(self, t_start: i64, t_end: i64) -> Self::EList;

    /// Returns an iterator over the incoming edges of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the incoming edges of the vertices.
    fn in_edges(self) -> Self::EList;

    /// Returns an iterator over the incoming edges of the vertices within a time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the incoming edges of the vertices within the given time window.
    fn in_edges_window(self, t_start: i64, t_end: i64) -> Self::EList;

    /// Returns an iterator over the outgoing edges of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the outgoing edges of the vertices.
    fn out_edges(self) -> Self::EList;

    /// Returns an iterator over the outgoing edges of the vertices within a time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the outgoing edges of the vertices within the given time window.
    fn out_edges_window(self, t_start: i64, t_end: i64) -> Self::EList;

    /// Returns an iterator over the neighbours of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of the vertices as VertexViews.
    fn neighbours(self) -> Self;

    /// Returns an iterator over the neighbours of the vertices within a time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of the vertices within the given time window as VertexViews.
    fn neighbours_window(self, t_start: i64, t_end: i64) -> Self;

    /// Returns an iterator over the incoming neighbours of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the incoming neighbours of the vertices as VertexViews.
    fn in_neighbours(self) -> Self;

    /// Returns an iterator over the incoming neighbours of the vertices within a time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the incoming neighbours of the vertices within the given time
    /// window as VertexViews.
    fn in_neighbours_window(self, t_start: i64, t_end: i64) -> Self;

    /// Returns an iterator over the outgoing neighbours of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the outgoing neighbours of the vertices as VertexViews.
    fn out_neighbours(self) -> Self;

    /// Returns an iterator over the outgoing neighbours of the vertices within a time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// An iterator over the outgoing neighbours of the vertices within the given time
    fn out_neighbours_window(self, t_start: i64, t_end: i64) -> Self;
}
