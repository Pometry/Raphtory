use crate::core::edge_ref::EdgeRef;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::{Direction, Prop};
use std::collections::HashMap;
use std::ops::Range;

/// The GraphViewInternalOps trait provides a set of methods to query a directed graph
/// represented by the raphtory_core::tgraph::TGraph struct.
pub trait GraphViewInternalOps {
    /// Gets the local reference for a remote vertex and keeps local references unchanged. Assumes vertex exists!
    fn localise_vertex_unchecked(&self, v: VertexRef) -> LocalVertexRef {
        match v {
            VertexRef::Local(v) => v,
            VertexRef::Remote(g_id) => self.vertex_ref(g_id).expect("Vertex should already exists"),
        }
    }

    /// Check if a vertex exists locally and returns local reference.
    fn local_vertex(&self, v: VertexRef) -> Option<LocalVertexRef>;

    /// Check if a vertex exists locally in the window and returns local reference.
    fn local_vertex_window(&self, v: VertexRef, t_start: i64, t_end: i64)
        -> Option<LocalVertexRef>;

    fn get_unique_layers_internal(&self) -> Vec<usize>;

    fn get_layer_name_by_id(&self, layer_id: usize) -> String;

    /// Get the layer id for the given layer name
    fn get_layer(&self, key: Option<&str>) -> Option<usize>;

    /// Returns the default start time for perspectives over the view
    fn view_start(&self) -> Option<i64>;

    /// Returns the default end time for perspectives over the view
    fn view_end(&self) -> Option<i64>;

    /// Returns the timestamp for the earliest activity
    fn earliest_time_global(&self) -> Option<i64>;

    /// Returns the timestamp for the earliest activity in the window
    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64>;

    /// Returns the timestamp for the latest activity
    fn latest_time_global(&self) -> Option<i64>;

    /// Returns the timestamp for the latest activity in the window
    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64>;

    /// Returns the total number of vertices in the graph.
    fn vertices_len(&self) -> usize;

    /// Returns the number of vertices in the graph that were created between
    /// the start (t_start) and end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize;

    /// Returns the total number of edges in the graph.
    fn edges_len(&self, layer: Option<usize>) -> usize;

    /// Returns the number of edges in the graph that were created between the
    /// start (t_start) and end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize;

    /// Returns true if the graph contains an edge between the source vertex
    /// (src) and the destination vertex (dst).
    /// # Arguments
    ///
    /// * `src` - The source vertex of the edge.
    /// * `dst` - The destination vertex of the edge.
    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool;

    /// Returns true if the graph contains an edge between the source vertex (src) and the
    /// destination vertex (dst) created between the start (t_start) and end (t_end) timestamps
    /// (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    /// * `src` - The source vertex of the edge.
    /// * `dst` - The destination vertex of the edge.
    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> bool;

    /// Returns true if the graph contains the specified vertex (v).
    /// # Arguments
    ///
    /// * `v` - VertexRef of the vertex to check.
    fn has_vertex_ref(&self, v: VertexRef) -> bool;

    /// Returns true if the graph contains the specified vertex (v) created between the
    /// start (t_start) and end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `v` - VertexRef of the vertex to check.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool;

    /// Returns the number of edges that point towards or from the specified vertex
    /// (v) based on the direction (d).
    /// # Arguments
    ///
    /// * `v` - LocalVertexRef of the vertex to check.
    /// * `d` - Direction of the edges to count.
    fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize;

    /// Returns the number of edges that point towards or from the specified vertex (v)
    /// created between the start (t_start) and end (t_end) timestamps (inclusive) based
    /// on the direction (d).
    /// # Arguments
    ///
    /// * `v` - LocalVertexRef of the vertex to check.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn degree_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> usize;

    /// Returns the LocalVertexRef that corresponds to the specified vertex ID (v).
    /// Returns None if the vertex ID is not present in the graph.
    /// # Arguments
    ///
    /// * `v` - The vertex ID to lookup.
    fn vertex_ref(&self, v: u64) -> Option<LocalVertexRef>;

    /// Returns the global ID for a vertex
    fn vertex_id(&self, v: LocalVertexRef) -> u64;

    /// Returns the string name for a vertex
    fn vertex_name(&self, v: LocalVertexRef) -> String {
        match self.static_vertex_prop(v, "_id".to_string()) {
            None => self.vertex_id(v).to_string(),
            Some(prop) => prop.to_string(),
        }
    }

    /// Returns the LocalVertexRef that corresponds to the specified vertex ID (v) created
    /// between the start (t_start) and end (t_end) timestamps (inclusive).
    /// Returns None if the vertex ID is not present in the graph.
    /// # Arguments
    ///
    /// * `v` - The vertex ID to lookup.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    /// * `Option<LocalVertexRef>` - The LocalVertexRef of the vertex if it exists in the graph.
    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<LocalVertexRef>;

    /// Return the earliest time for a vertex
    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64>;

    /// Return the earliest time for a vertex in a window
    fn vertex_earliest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64>;

    /// Return the latest time for a vertex
    fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64>;

    /// Return the latest time for a vertex in a window
    fn vertex_latest_time_window(&self, v: LocalVertexRef, t_start: i64, t_end: i64)
        -> Option<i64>;

    /// Returns all the vertex references in the graph.
    /// # Returns
    /// * `Box<dyn Iterator<Item = LocalVertexRef> + Send>` - An iterator over all the vertex
    /// references in the graph.
    fn vertex_refs(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send>;

    /// Returns all the vertex references in the graph created between the start (t_start) and
    /// end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    /// * `Box<dyn Iterator<Item = LocalVertexRef> + Send>` - An iterator over all the vertexes
    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send>;

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = LocalVertexRef> + Send>;

    /// Returns all the vertex references in the graph that are in the specified shard.
    /// Between the start (t_start) and end (t_end)
    ///
    /// # Arguments
    /// shard - The shard to return the vertex references for.
    /// t_start - The start time of the window (inclusive).
    /// t_end - The end time of the window (exclusive).
    ///
    /// # Returns
    /// * `Box<dyn Iterator<Item = LocalVertexRef> + Send>` - An iterator over all the vertexes
    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send>;

    /// Returns the edge reference that corresponds to the specified src and dst vertex
    /// # Arguments
    ///
    /// * `src` - The source vertex.
    /// * `dst` - The destination vertex.
    ///
    /// # Returns
    ///
    /// * `Option<EdgeRef>` - The edge reference if it exists.
    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef>;

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
    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> Option<EdgeRef>;

    /// Returns all the edge references in the graph.
    ///
    /// # Returns
    ///
    /// * `Box<dyn Iterator<Item = EdgeRef> + Send>` - An iterator over all the edge references.
    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

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
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    /// Returns an iterator over the edges connected to a given vertex in a given direction.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the edges are being queried.
    /// * `d` - The direction in which to search for edges.
    /// * `layer` - The optional layer to consider
    ///
    /// # Returns
    ///
    /// Box<dyn Iterator<Item = EdgeRef> + Send> -  A boxed iterator that yields references to
    /// the edges connected to the vertex.
    fn vertex_edges(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    /// Returns an iterator over the exploded edges connected to a given vertex in a given direction.
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
    fn vertex_edges_t(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

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
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
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
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
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
    fn neighbours(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

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
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

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
    fn static_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<Prop>;

    fn static_vertex_props(&self, v: LocalVertexRef) -> HashMap<String, Prop>;

    /// Gets a static property of a given graph given the name
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// Option<Prop> - The property value if it exists.
    fn static_prop(&self, name: String) -> Option<Prop>;

    /// Gets the keys of static properties of a given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the property is being queried.
    ///
    /// # Returns
    ///
    /// Vec<String> - The keys of the static properties.
    fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String>;

    /// Gets the keys of static properties of graph
    ///
    /// # Arguments
    ///
    /// # Returns
    ///
    /// Vec<String> - The keys of the static properties.
    fn static_prop_names(&self) -> Vec<String>;

    /// Returns a vector of all names of temporal properties within the given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the names.
    ///
    /// # Returns
    ///
    /// A vector of strings representing the names of the temporal properties
    fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String>;

    /// Returns a vector of all names of temporal properties within the graph
    ///
    /// # Arguments
    ///
    /// # Returns
    ///
    /// A vector of strings representing the names of the temporal properties
    fn temporal_prop_names(&self) -> Vec<String>;

    /// Returns a vector of all temporal values of the vertex property with the given name for the
    /// given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    ///
    /// # Returns
    ///
    /// A vector of tuples representing the temporal values of the property for the given vertex
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: String) -> Vec<(i64, Prop)>;

    /// Returns a vector of all temporal values of the graph property with the given name
    ///
    /// # Arguments
    /// * `name` - The name of the property to retrieve.
    ///
    /// # Returns
    ///
    /// A vector of tuples representing the temporal values of the property for the given vertex
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_prop_vec(&self, name: String) -> Vec<(i64, Prop)>;

    /// Returns a vector of all temporal values of the vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the timestamp.
    ///
    /// # Returns
    ///
    /// A vector of timestamps representing the temporal values for the given vertex.
    fn vertex_timestamps(&self, v: LocalVertexRef) -> Vec<i64>;

    /// Returns a vector of all temporal values of the vertex for a given window.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the timestamp.
    /// * `t_start` - The start time of the window.
    /// * `t_end` - The end time of the window.
    ///
    /// # Returns
    ///
    /// A vector of timestamps representing the temporal values for the given vertex in a given window.
    fn vertex_timestamps_window(&self, v: LocalVertexRef, t_start: i64, t_end: i64) -> Vec<i64>;

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
        v: LocalVertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)>;

    /// Returns a vector of all temporal values of the graph property with the given name
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve.
    /// * `t_start` - The start time of the window to consider.
    /// * `t_end` - The end time of the window to consider.
    ///
    /// # Returns
    ///
    /// A vector of tuples representing the temporal values of the property for graph
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_prop_vec_window(&self, name: String, t_start: i64, t_end: i64) -> Vec<(i64, Prop)>;

    /// Returns a map of all temporal values of the vertex properties for the given vertex.
    /// The keys of the map are the names of the properties, and the values are vectors of tuples
    ///
    /// # Arguments
    ///
    /// - `v` - A reference to the vertex for which to retrieve the temporal property vector.
    ///
    /// # Returns
    /// - A map of all temporal values of the vertex properties for the given vertex.
    fn temporal_vertex_props(&self, v: LocalVertexRef) -> HashMap<String, Vec<(i64, Prop)>>;

    /// Returns a map of all temporal values of the graph properties.
    /// The keys of the map are the names of the properties, and the values are vectors of tuples
    ///
    /// # Arguments
    ///
    /// # Returns
    ///
    /// - A map of all temporal values of the graph properties.
    fn temporal_props(&self) -> HashMap<String, Vec<(i64, Prop)>>;

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
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;

    /// Returns a map of all temporal values of the graph properties
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// - `t_start` - The start time of the window to consider (inclusive).
    /// - `t_end` - The end time of the window to consider (exclusive).
    ///
    /// # Returns
    /// - A map of all temporal values of the graph properties
    fn temporal_props_window(&self, t_start: i64, t_end: i64) -> HashMap<String, Vec<(i64, Prop)>>;

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
    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop>;

    /// Returns a vector of keys for the static properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// # Returns
    ///
    /// * A `Vec` of `String` containing the keys for the static properties of the given edge.
    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String>;

    /// Returns a vector of keys for the temporal properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// # Returns
    ///
    /// * A `Vec` of `String` containing the keys for the temporal properties of the given edge.
    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String>;

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
    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)>;

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
    ) -> Vec<(i64, Prop)>;

    fn edge_timestamps(&self, e: EdgeRef, window: Option<Range<i64>>) -> Vec<i64>;

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

    fn num_shards(&self) -> usize;
}

pub trait WrappedGraph {
    type Internal: GraphViewInternalOps + Send + Sync + 'static + ?Sized;

    fn as_graph(&self) -> &Self::Internal;
}

/// Helper trait for various graphs that just delegate to the internal graph
///
impl<G> GraphViewInternalOps for G
where
    G: WrappedGraph,
{
    fn local_vertex(&self, v: VertexRef) -> Option<LocalVertexRef> {
        self.as_graph().local_vertex(v)
    }

    fn local_vertex_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<LocalVertexRef> {
        self.as_graph().local_vertex_window(v, t_start, t_end)
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        self.as_graph().get_unique_layers_internal()
    }

    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.as_graph().get_layer_name_by_id(layer_id)
    }

    fn get_layer(&self, key: Option<&str>) -> Option<usize> {
        self.as_graph().get_layer(key)
    }

    fn view_start(&self) -> Option<i64> {
        self.as_graph().view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.as_graph().view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.as_graph().earliest_time_global()
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.as_graph().earliest_time_window(t_start, t_end)
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.as_graph().latest_time_global()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.as_graph().latest_time_window(t_start, t_end)
    }

    fn vertices_len(&self) -> usize {
        self.as_graph().vertices_len()
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.as_graph().vertices_len_window(t_start, t_end)
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        self.as_graph().edges_len(layer)
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize {
        self.as_graph().edges_len_window(t_start, t_end, layer)
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        self.as_graph().has_edge_ref(src, dst, layer)
    }

    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> bool {
        self.as_graph()
            .has_edge_ref_window(src, dst, t_start, t_end, layer)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.as_graph().has_vertex_ref(v)
    }

    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
        self.as_graph().has_vertex_ref_window(v, t_start, t_end)
    }

    fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        self.as_graph().degree(v, d, layer)
    }

    fn degree_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> usize {
        self.as_graph().degree_window(v, t_start, t_end, d, layer)
    }

    fn vertex_ref(&self, v: u64) -> Option<LocalVertexRef> {
        self.as_graph().vertex_ref(v)
    }

    fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        self.as_graph().vertex_id(v)
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<LocalVertexRef> {
        self.as_graph().vertex_ref_window(v, t_start, t_end)
    }

    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.as_graph().vertex_earliest_time(v)
    }

    fn vertex_earliest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.as_graph()
            .vertex_earliest_time_window(v, t_start, t_end)
    }

    fn vertex_latest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.as_graph().vertex_latest_time(v)
    }

    fn vertex_latest_time_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<i64> {
        self.as_graph().vertex_latest_time_window(v, t_start, t_end)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        self.as_graph().vertex_refs()
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        self.as_graph().vertex_refs_window(t_start, t_end)
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        self.as_graph().vertex_refs_shard(shard)
    }

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        self.as_graph()
            .vertex_refs_window_shard(shard, t_start, t_end)
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        self.as_graph().edge_ref(src, dst, layer)
    }

    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> Option<EdgeRef> {
        self.as_graph()
            .edge_ref_window(src, dst, t_start, t_end, layer)
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.as_graph().edge_refs(layer)
    }

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.as_graph().edge_refs_window(t_start, t_end, layer)
    }

    fn vertex_edges(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.as_graph().vertex_edges(v, d, layer)
    }

    fn vertex_edges_t(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.as_graph().vertex_edges_t(v, d, layer)
    }

    fn vertex_edges_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.as_graph()
            .vertex_edges_window(v, t_start, t_end, d, layer)
    }

    fn vertex_edges_window_t(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.as_graph()
            .vertex_edges_window_t(v, t_start, t_end, d, layer)
    }

    fn neighbours(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.as_graph().neighbours(v, d, layer)
    }

    fn neighbours_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.as_graph()
            .neighbours_window(v, t_start, t_end, d, layer)
    }

    fn static_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<Prop> {
        self.as_graph().static_vertex_prop(v, name)
    }

    fn static_vertex_props(&self, v: LocalVertexRef) -> HashMap<String, Prop> {
        self.as_graph().static_vertex_props(v)
    }

    fn static_prop(&self, name: String) -> Option<Prop> {
        self.as_graph().static_prop(name)
    }

    fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.as_graph().static_vertex_prop_names(v)
    }

    fn static_prop_names(&self) -> Vec<String> {
        self.as_graph().static_prop_names()
    }

    fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.as_graph().temporal_vertex_prop_names(v)
    }

    fn temporal_prop_names(&self) -> Vec<String> {
        self.as_graph().temporal_prop_names()
    }

    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: String) -> Vec<(i64, Prop)> {
        self.as_graph().temporal_vertex_prop_vec(v, name)
    }

    fn temporal_prop_vec(&self, name: String) -> Vec<(i64, Prop)> {
        self.as_graph().temporal_prop_vec(name)
    }

    fn vertex_timestamps(&self, v: LocalVertexRef) -> Vec<i64> {
        self.as_graph().vertex_timestamps(v)
    }

    fn vertex_timestamps_window(&self, v: LocalVertexRef, t_start: i64, t_end: i64) -> Vec<i64> {
        self.as_graph().vertex_timestamps_window(v, t_start, t_end)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.as_graph()
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_prop_vec_window(&self, name: String, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        self.as_graph()
            .temporal_prop_vec_window(name, t_start, t_end)
    }

    fn temporal_vertex_props(&self, v: LocalVertexRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.as_graph().temporal_vertex_props(v)
    }

    fn temporal_props(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.as_graph().temporal_props()
    }

    fn temporal_vertex_props_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.as_graph()
            .temporal_vertex_props_window(v, t_start, t_end)
    }

    fn temporal_props_window(&self, t_start: i64, t_end: i64) -> HashMap<String, Vec<(i64, Prop)>> {
        self.as_graph().temporal_props_window(t_start, t_end)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        self.as_graph().static_edge_prop(e, name)
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.as_graph().static_edge_prop_names(e)
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.as_graph().temporal_edge_prop_names(e)
    }

    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        self.as_graph().temporal_edge_props_vec(e, name)
    }

    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.as_graph()
            .temporal_edge_props_vec_window(e, name, t_start, t_end)
    }

    fn edge_timestamps(&self, e: EdgeRef, window: Option<Range<i64>>) -> Vec<i64> {
        self.as_graph().edge_timestamps(e, window)
    }

    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.as_graph().temporal_edge_props(e)
    }

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.as_graph()
            .temporal_edge_props_window(e, t_start, t_end)
    }

    fn num_shards(&self) -> usize {
        self.as_graph().num_shards()
    }
}
