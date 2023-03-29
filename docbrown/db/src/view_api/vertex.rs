use crate::view_api::edge::{EdgeListOps, EdgeViewOps};
use docbrown_core::{tgraph_shard::errors::GraphError, Direction, Prop};
use std::collections::HashMap;

/// Trait defining operations that can be performed on a vertex view.
///
/// This trait defines operations that can be performed on a vertex view, which represents a single vertex in a graph.
/// The trait is parameterized by associated types that define the types of edges, vertex lists, and edge lists that
/// are associated with the vertex.
pub trait VertexViewOps: Sized + Send + Sync {
    /// Associated type representing an edge in the graph.
    type Edge: EdgeViewOps<Vertex = Self>;

    /// Associated type representing a list of vertices in the graph.
    type VList: VertexListOps<Vertex = Self, Edge = Self::Edge, EList = Self::EList>;

    /// Associated type representing a list of edges in the graph.
    type EList: EdgeListOps<Vertex = Self, Edge = Self::Edge>;

    /// Returns the ID of the vertex.
    fn id(&self) -> u64;

    /// Returns the values of a given property for the vertex, as a vector of tuples with
    /// timestamps and property values.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// A vector of tuples representing the values of the property for the vertex,
    /// where each tuple contains a timestamp and a property value.
    fn prop(&self, name: String) -> Result<Vec<(i64, Prop)>, GraphError>;

    /// Returns a hashmap containing all properties of the vertex, with property names as keys and
    /// values as vectors of tuples with timestamps and property values.
    ///
    /// # Returns
    ///
    /// A hashmap representing all properties of the vertex, where keys are property names and
    /// values are vectors of tuples with timestamps and property values.
    fn props(&self) -> Result<HashMap<String, Vec<(i64, Prop)>>, GraphError>;

    /// Returns the degree (i.e., number of edges) of the vertex.
    fn degree(&self) -> Result<usize, GraphError>;

    /// Returns the degree (i.e., number of edges) of the vertex within a given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// The number of edges that connect to the vertex within the given time window.
    fn degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError>;

    /// Returns the in-degree (i.e., number of incoming edges) of the vertex.
    fn in_degree(&self) -> Result<usize, GraphError>;

    /// Returns the in-degree (i.e., number of incoming edges) of the vertex within a given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// The number of incoming edges that connect to the vertex within the given time window.
    fn in_degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError>;

    /// Returns the out-degree (i.e., number of outgoing edges) of the vertex.
    fn out_degree(&self) -> Result<usize, GraphError>;

    /// Returns the out-degree (i.e., number of outgoing edges) of the vertex within a given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window.
    /// * `t_end` - The end time of the window.
    ///
    /// # Returns
    ///
    /// The number of outgoing edges that connect to the vertex within the given time window.
    fn out_degree_window(&self, t_start: i64, t_end: i64) -> Result<usize, GraphError>;

    /// Returns a list of all edges that connect to the vertex.
    /// # Returns
    /// A list of all edges that connect to the vertex.
    fn edges(&self) -> Self::EList;

    /// Returns a list of all edges that connect to the vertex within a given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window.
    /// * `t_end` - The end time of the window.
    ///
    /// # Returns
    ///
    /// A list of all edges that connect to the vertex within the given time window.
    fn edges_window(&self, t_start: i64, t_end: i64) -> Self::EList;

    /// Returns a list of all incoming edges that connect to the vertex.
    ///
    /// # Returns
    /// A list of all incoming edges that connect to the vertex.
    fn in_edges(&self) -> Self::EList;

    /// Returns a list of all incoming edges that connect to the vertex within a given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window.
    /// * `t_end` - The end time of the window.
    ///
    /// # Returns
    ///
    /// A list of all incoming edges that connect to the vertex within the given time window.
    fn in_edges_window(&self, t_start: i64, t_end: i64) -> Self::EList;

    /// Returns a list of all outgoing edges that connect to the vertex.
    ///
    /// # Returns
    ///
    /// A list of all outgoing edges that connect to the vertex.
    fn out_edges(&self) -> Self::EList;

    /// Returns a list of all outgoing edges that connect to the vertex within a given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window.
    /// * `t_end` - The end time of the window.
    ///
    /// # Returns
    ///
    /// A list of all outgoing edges that connect to the vertex within the given time window.
    fn out_edges_window(&self, t_start: i64, t_end: i64) -> Self::EList;

    /// Returns a list of all neighbours of the vertex.
    ///
    /// # Returns
    ///
    /// A list of all neighbours of the vertex.
    fn neighbours(&self) -> Self::VList;

    /// Returns a list of all neighbours of the vertex within a given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window.
    /// * `t_end` - The end time of the window.
    ///
    /// # Returns
    ///
    /// A list of all neighbours of the vertex within the given time window.
    fn neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList;

    /// Returns a list of all incoming neighbours of the vertex.
    ///
    /// # Returns
    ///
    /// A list of all incoming neighbours of the vertex.
    fn in_neighbours(&self) -> Self::VList;

    /// Returns a list of all incoming neighbours of the vertex within a given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window.
    /// * `t_end` - The end time of the window.
    ///
    /// # Returns
    ///
    /// A list of all incoming neighbours of the vertex within the given time window.
    fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList;

    /// Returns a list of all outgoing neighbours of the vertex.
    ///
    /// # Returns
    ///
    /// A list of all outgoing neighbours of the vertex.
    fn out_neighbours(&self) -> Self::VList;

    /// Returns a list of all outgoing neighbours of the vertex within a given time window.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window.
    /// * `t_end` - The end time of the window.
    ///
    /// # Returns
    ///
    /// A list of all outgoing neighbours of the vertex within the given time window.
    fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> Self::VList;
}

/// A trait for operations on a list of vertices.
pub trait VertexListOps:
    IntoIterator<Item = Self::Vertex, IntoIter = Self::IterType> + Sized + Send
{
    /// The type of the vertex.
    type Vertex: VertexViewOps<Edge = Self::Edge>;

    /// The type of the edge.
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;

    /// The type of the iterator for the list of edges
    type EList: EdgeListOps<Vertex = Self::Vertex, Edge = Self::Edge>;

    /// The type of the iterator for the list of vertices
    type IterType: Iterator<Item = Self::Vertex> + Send;

    type ValueIterType<U>: Iterator<Item = U> + Send;

    /// Returns the ids of vertices in the list.
    ///
    /// # Returns
    /// The ids of vertices in the list.
    fn id(self) -> Self::ValueIterType<u64>;

    /// Returns an iterator of the values of the given property name
    /// including the times when it changed
    ///
    /// # Arguments
    /// * `name` - The name of the property.
    ///
    /// # Returns
    /// An iterator of the values of the given property name including the times when it changed
    /// as a vector of tuples of the form (time, property).
    fn prop(self, name: String) -> Result<Self::ValueIterType<Vec<(i64, Prop)>>, GraphError>;

    /// Returns an iterator over all vertex properties.
    ///
    /// # Returns
    /// An iterator over all vertex properties.
    fn props(self) -> Result<Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>>, GraphError>;

    /// Returns an iterator over the degree of the vertices.
    ///
    /// # Returns
    /// An iterator over the degree of the vertices.
    fn degree(self) -> Result<Self::ValueIterType<usize>, GraphError>;

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
    fn degree_window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Result<Self::ValueIterType<usize>, GraphError>;

    /// Returns an iterator over the in-degree of the vertices.
    /// The in-degree of a vertex is the number of edges that connect to it from other vertices.
    ///
    /// # Returns
    /// An iterator over the in-degree of the vertices.
    fn in_degree(self) -> Result<Self::ValueIterType<usize>, GraphError>;

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
    fn in_degree_window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Result<Self::ValueIterType<usize>, GraphError>;

    /// Returns an iterator over the out-degree of the vertices.
    /// The out-degree of a vertex is the number of edges that connects to it from the vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the out-degree of the vertices.
    fn out_degree(self) -> Result<Self::ValueIterType<usize>, GraphError>;

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
    fn out_degree_window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Result<Self::ValueIterType<usize>, GraphError>;

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
