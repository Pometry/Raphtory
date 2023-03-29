use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;

/// The GraphViewInternalOps trait provides a set of methods to query a directed graph
/// represented by the docbrown_core::tgraph::TGraph struct.
pub trait GraphViewInternalOps {
    /// Returns the total number of vertices in the graph.
    fn vertices_len(&self) -> Result<usize, GraphError>;

    /// Returns the number of vertices in the graph that were created between
    /// the start (t_start) and end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize;

    /// Returns the total number of edges in the graph.
    fn edges_len(&self) -> Result<usize, GraphError>;

    /// Returns the number of edges in the graph that were created between the
    /// start (t_start) and end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn edges_len_window(&self, t_start: i64, t_end: i64) -> usize;

    /// Returns true if the graph contains an edge between the source vertex
    /// (src) and the destination vertex (dst).
    /// # Arguments
    ///
    /// * `src` - The source vertex of the edge.
    /// * `dst` - The destination vertex of the edge.
    fn has_edge_ref<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
    ) -> Result<bool, GraphError>;

    /// Returns true if the graph contains an edge between the source vertex (src) and the
    /// destination vertex (dst) created between the start (t_start) and end (t_end) timestamps
    /// (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    /// * `src` - The source vertex of the edge.
    /// * `dst` - The destination vertex of the edge.
    fn has_edge_ref_window<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> Result<bool, GraphError>;

    /// Returns true if the graph contains the specified vertex (v).
    /// # Arguments
    ///
    /// * `v` - VertexRef of the vertex to check.
    fn has_vertex_ref<V: Into<VertexRef>>(&self, v: V) -> Result<bool, GraphError>;

    /// Returns true if the graph contains the specified vertex (v) created between the
    /// start (t_start) and end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `v` - VertexRef of the vertex to check.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn has_vertex_ref_window<V: Into<VertexRef>>(
        &self,
        v: V,
        t_start: i64,
        t_end: i64,
    ) -> Result<bool, GraphError>;

    /// Returns the number of edges that point towards or from the specified vertex
    /// (v) based on the direction (d).
    /// # Arguments
    ///
    /// * `v` - VertexRef of the vertex to check.
    /// * `d` - Direction of the edges to count.
    fn degree(&self, v: VertexRef, d: Direction) -> Result<usize, GraphError>;

    /// Returns the number of edges that point towards or from the specified vertex (v)
    /// created between the start (t_start) and end (t_end) timestamps (inclusive) based
    /// on the direction (d).
    /// # Arguments
    ///
    /// * `v` - VertexRef of the vertex to check.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn degree_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Result<usize, GraphError>;

    /// Returns the VertexRef that corresponds to the specified vertex ID (v).
    /// Returns None if the vertex ID is not present in the graph.
    /// # Arguments
    ///
    /// * `v` - The vertex ID to lookup.
    fn vertex_ref(&self, v: u64) -> Result<Option<VertexRef>, GraphError>;

    /// Returns the VertexRef that corresponds to the specified vertex ID (v) created
    /// between the start (t_start) and end (t_end) timestamps (inclusive).
    /// Returns None if the vertex ID is not present in the graph.
    /// # Arguments
    ///
    /// * `v` - The vertex ID to lookup.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    /// * `Option<VertexRef>` - The VertexRef of the vertex if it exists in the graph.
    fn vertex_ref_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
    ) -> Result<Option<VertexRef>, GraphError>;

    /// Retuns all the vertex IDs in the graph.
    /// # Returns
    /// * `Box<dyn Iterator<Item = u64> + Send>` - An iterator over all the vertex IDs in the graph.
    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send>;

    /// Returns all the vertex IDs in the graph created between the start (t_start) and
    /// end (t_end) timestamps
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    /// * `Box<dyn Iterator<Item = u64> + Send>` - An iterator over all the vertex IDs in the graph.
    fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send>;

    /// Returns all the vertex references in the graph.
    /// # Returns
    /// * `Box<dyn Iterator<Item = VertexRef> + Send>` - An iterator over all the vertex
    /// references in the graph.
    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    /// Returns all the vertex references in the graph created between the start (t_start) and
    /// end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    /// * `Box<dyn Iterator<Item = VertexRef> + Send>` - An iterator over all the vertexes
    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    /// Returns all the vertex references in the graph that are in the specified shard.
    /// Between the start (t_start) and end (t_end)
    ///
    /// # Arguments
    /// shard - The shard to return the vertex references for.
    /// t_start - The start time of the window (inclusive).
    /// t_end - The end time of the window (exclusive).
    ///
    /// # Returns
    /// * `Box<dyn Iterator<Item = VertexRef> + Send>` - An iterator over all the vertexes
    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    /// Returns the edge reference that corresponds to the specified src and dst vertex
    /// # Arguments
    ///
    /// * `src` - The source vertex.
    /// * `dst` - The destination vertex.
    ///
    /// # Returns
    ///
    /// * `Option<EdgeRef>` - The edge reference if it exists.
    fn edge_ref<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
    ) -> Result<Option<EdgeRef>, GraphError>;

    /// Returns the edge reference that corresponds to the specified src and dst vertex
    /// created between the start (t_start) and end (t_end) timestamps (exclusive).
    ///
    /// # Arguments
    ///
    /// * `src` - The source vertex.
    /// * `dst` - The destination vertex.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    ///
    /// * `Option<EdgeRef>` - The edge reference if it exists.
    fn edge_ref_window<V1: Into<VertexRef>, V2: Into<VertexRef>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> Result<Option<EdgeRef>, GraphError>;

    /// Returns all the edge references in the graph.
    ///
    /// # Returns
    ///
    /// * `Box<dyn Iterator<Item = EdgeRef> + Send>` - An iterator over all the edge references.
    fn edge_refs(&self) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    /// Returns all the edge references in the graph created between the start (t_start) and
    /// end (t_end) timestamps (inclusive).
    ///
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    /// # Returns
    ///
    /// * `Box<dyn Iterator<Item = EdgeRef> + Send>` - An iterator over all the edge references.
    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    /// Returns an iterator over the edges connected to a given vertex in a given direction.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the edges are being queried.
    /// * `d` - The direction in which to search for edges.
    ///
    /// # Returns
    ///
    /// Box<dyn Iterator<Item = EdgeRef> + Send> -  A boxed iterator that yields references to
    /// the edges connected to the vertex.
    fn vertex_edges(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    /// Returns an iterator over the edges connected to a given vertex within a
    /// specified time window in a given direction.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the edges are being queried.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    /// * `d` - The direction in which to search for edges.
    ///
    /// # Returns
    ///
    /// Box<dyn Iterator<Item = EdgeRef> + Send> - A boxed iterator that yields references
    /// to the edges connected to the vertex within the specified time window.
    fn vertex_edges_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    /// Returns an iterator over the edges connected to a given vertex within
    /// a specified time window in a given direction but exploded.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the edges are being queried.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    /// * `d` - The direction in which to search for edges.
    ///
    /// # Returns
    ///
    /// A boxed iterator that yields references to the edges connected to the vertex
    ///  within the specified time window but exploded.
    fn vertex_edges_window_t(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    /// Returns an iterator over the neighbors of a given vertex in a given direction.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the neighbors are being queried.
    /// * `d` - The direction in which to search for neighbors.
    ///
    /// # Returns
    ///
    /// A boxed iterator that yields references to the neighboring vertices.
    fn neighbours(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    /// Returns an iterator over the neighbors of a given vertex within a specified time window in a given direction.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the neighbors are being queried.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    /// * `d` - The direction in which to search for neighbors.
    ///
    /// # Returns
    ///
    /// A boxed iterator that yields references to the neighboring vertices within the specified time window.
    fn neighbours_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    ///  Returns the vertex ids of the neighbors of a given vertex in a given direction.
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the neighbors are being queried.
    /// * `d` - The direction in which to search for neighbors.
    ///
    /// # Returns
    ///
    /// A boxed iterator that yields the ids of the neighboring vertices.
    fn neighbours_ids(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = u64> + Send>;

    /// Returns the vertex ids of the neighbors of a given vertex within a specified
    /// time window in a given direction.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the neighbors are being queried.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    /// * `d` - The direction in which to search for neighbors.
    ///
    /// # Returns
    ///
    /// A boxed iterator that yields the ids of the neighboring vertices within the
    /// specified time window.
    fn neighbours_ids_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send>;

    /// Gets a static property of a given vertex given the name and vertex reference.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the property is being queried.
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// Option<Prop> - The property value if it exists.
    fn static_vertex_prop(&self, v: VertexRef, name: String) -> Result<Option<Prop>, GraphError>;

    /// Gets the keys of static properties of a given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the property is being queried.
    ///
    /// # Returns
    ///
    /// Vec<String> - The keys of the static properties.
    fn static_vertex_prop_keys(&self, v: VertexRef) -> Result<Vec<String>, GraphError>;

    /// Returns a vector of all temporal values of the vertex property with the given name for the
    /// given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    /// * `t_start` - The start time of the window to consider.
    /// * `t_end` - The end time of the window to consider.
    ///
    /// # Returns
    ///
    /// A vector of tuples representing the temporal values of the property for the given vertex
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_vertex_prop_vec(
        &self,
        v: VertexRef,
        name: String,
    ) -> Result<Vec<(i64, Prop)>, GraphError>;

    /// Returns a vector of all temporal values of the vertex property with the given name for the given vertex
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    /// * `t_start` - The start time of the window to consider.
    /// * `t_end` - The end time of the window to consider.
    ///
    /// # Returns
    ///
    /// A vector of tuples representing the temporal values of the property for the given vertex
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_vertex_prop_vec_window(
        &self,
        v: VertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Result<Vec<(i64, Prop)>, GraphError>;

    /// Returns a map of all temporal values of the vertex properties for the given vertex.
    /// The keys of the map are the names of the properties, and the values are vectors of tuples
    ///
    /// # Arguments
    ///
    /// - `v` - A reference to the vertex for which to retrieve the temporal property vector.
    ///
    /// # Returns
    /// - A map of all temporal values of the vertex properties for the given vertex.
    fn temporal_vertex_props(
        &self,
        v: VertexRef,
    ) -> Result<HashMap<String, Vec<(i64, Prop)>>, GraphError>;

    /// Returns a map of all temporal values of the vertex properties for the given vertex
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// - `v` - A reference to the vertex for which to retrieve the temporal property vector.
    /// - `t_start` - The start time of the window to consider (inclusive).
    /// - `t_end` - The end time of the window to consider (exclusive).
    ///
    /// # Returns
    /// - A map of all temporal values of the vertex properties for the given vertex
    fn temporal_vertex_props_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Result<HashMap<String, Vec<(i64, Prop)>>, GraphError>;

    /// Returns a vector of all temporal values of the edge property with the given name for the
    /// given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    ///
    /// # Returns
    ///
    /// A property if it exists
    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Result<Option<Prop>, GraphError>;

    /// Returns a vector of keys for the static properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// # Returns
    ///
    /// * A `Vec` of `String` containing the keys for the static properties of the given edge.
    fn static_edge_prop_keys(&self, e: EdgeRef) -> Result<Vec<String>, GraphError>;

    /// Returns a vector of tuples containing the values of the temporal property with the given name
    /// for the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    ///
    /// # Returns
    ///
    /// * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge.
    fn temporal_edge_props_vec(
        &self,
        e: EdgeRef,
        name: String,
    ) -> Result<Vec<(i64, Prop)>, GraphError>;

    /// Returns a vector of tuples containing the values of the temporal property with the given name
    /// for the given edge reference within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    /// * `t_start` - An `i64` containing the start time of the time window (inclusive).
    /// * `t_end` - An `i64` containing the end time of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge
    /// within the specified time window.
    ///
    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Result<Vec<(i64, Prop)>, GraphError>;

    /// Returns a hash map containing all the temporal properties of the given edge reference,
    /// where each key is the name of a temporal property and each value is a vector of tuples containing
    /// the property value and the time it was recorded.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge.
    ///
    /// # Returns
    ///
    /// * A `HashMap` containing all the temporal properties of the given edge, where each key is the name of a
    /// temporal property and each value is a vector of tuples containing the property value and the time it was recorded.
    ///
    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>>;

    /// Returns a hash map containing all the temporal properties of the given edge reference within the specified
    /// time window, where each key is the name of a temporal property and each value is a vector of tuples containing
    /// the property value and the time it was recorded.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge.
    /// * `t_start` - An `i64` containing the start time of the time window (inclusive).
    /// * `t_end` - An `i64` containing the end time of the time window (exclusive).
    ///
    /// # Returns
    ///
    /// * A `HashMap` containing all the temporal properties of the given edge within the specified time window,
    /// where each key is the name of a temporal property and each value is a vector of tuples containing the property
    /// value and the time it was recorded.
    ///
    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;
}
