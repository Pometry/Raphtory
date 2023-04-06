//! A windowed view is a subset of a graph between a specific time window.
//! For example, lets say you wanted to run an algorithm each month over a graph, graph window
//! would allow you to split the graph into 30 day chunks to do so.
//!
//! This module also defines the `GraphWindow` trait, which represents a window of time over
//! which a graph can be queried.
//!
//! GraphWindowSet implements the `Iterator` trait, producing `WindowedGraph` views
//! for each perspective within it.
//!
//! # Types
//!
//! * `GraphWindowSet` - A struct that allows iterating over a Graph broken down into multiple
//! windowed views. It contains a `Graph` and an iterator of `Perspective`.
//!
//! * `WindowedGraph` - A struct that represents a windowed view of a `Graph`.
//! It contains a `Graph`, a start time (`t_start`) and an end time (`t_end`).
//!
//! # Traits
//!
//! * `GraphViewInternalOps` - A trait that provides operations to a `WindowedGraph`
//! used internally by the `GraphWindowSet`.
//!
//! # Examples
//!
//! ```rust
//!
//! use docbrown_db::graph::Graph;
//! use docbrown_db::view_api::*;
//!
//! let graph = Graph::new(2);
//! graph.add_edge(0, 1, 2, &vec![]);
//! graph.add_edge(1, 1, 3, &vec![]);
//! graph.add_edge(2, 2, 3, &vec![]);
//!
//!  let wg = graph.window(0, 1);
//!  assert_eq!(wg.edge(1, 2).unwrap().src().id(), 1);
//! ```

use crate::perspective::Perspective;
use crate::view_api::internal::GraphViewInternalOps;
use crate::view_api::time::TimeOps;
use crate::view_api::GraphViewOps;
use docbrown_core::{
    tgraph::{EdgeRef, VertexRef},
    Direction, Prop,
};
use std::collections::HashMap;

/// A set of windowed views of a `Graph`, allows user to iterating over a Graph broken
/// down into multiple windowed views.
pub struct WindowSet<T: TimeOps> {
    /// The underlying `Graph` object.
    pub view: T,
    /// An iterator of `Perspective`s to window the `Graph`.
    perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
}

impl<T: TimeOps> WindowSet<T> {
    /// Constructs a new `WindowSet` object.
    ///
    /// # Arguments
    ///
    /// * `view` - The underlying object.
    /// * `perspectives` - An iterator of `Perspective`s to window the `view`.
    ///
    /// # Returns
    ///
    /// A new `WindowSet` object.
    pub fn new(
        view: T,
        perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
    ) -> WindowSet<T> {
        WindowSet { view, perspectives }
    }
}

impl<T: TimeOps> Iterator for WindowSet<T> {
    type Item = T::WindowedViewType;
    fn next(&mut self) -> Option<Self::Item> {
        let perspective = self.perspectives.next()?;
        Some(self.view.window(
            perspective.start.unwrap_or(i64::MIN),
            perspective.end.unwrap_or(i64::MAX),
        ))
    }
}

/// A struct that represents a windowed view of a `Graph`.
#[derive(Debug, Clone)]
pub struct WindowedGraph<G: GraphViewInternalOps> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The inclusive start time of the window.
    pub t_start: i64,
    /// The exclusive end time of the window.
    pub t_end: i64,
}

/// Implementation of the GraphViewInternalOps trait for WindowedGraph.
/// This trait provides operations to a `WindowedGraph` used internally by the `GraphWindowSet`.
/// *Note: All functions in this are bound by the time set in the windowed graph.
impl<G: GraphViewOps> GraphViewInternalOps for WindowedGraph<G> {
    fn view_start(&self) -> Option<i64> {
        Some(self.t_start)
    }

    fn view_end(&self) -> Option<i64> {
        Some(self.t_end)
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.graph.earliest_time_window(self.t_start, self.t_end)
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph
            .earliest_time_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.graph.latest_time_window(self.t_start, self.t_end)
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph
            .latest_time_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    /// Returns the number of vertices in the windowed view.
    fn vertices_len(&self) -> usize {
        self.graph.vertices_len_window(self.t_start, self.t_end)
    }

    /// Returns the number of vertices in the windowed view, for a window specified by start and end times.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The inclusive start time of the window.
    /// * `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// The number of vertices in the windowed view for the given window.
    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.graph
            .vertices_len_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    /// Returns the number of edges in the windowed view.
    fn edges_len(&self) -> usize {
        self.graph.edges_len_window(self.t_start, self.t_end)
    }

    /// Returns the number of edges in the windowed view, for a window specified by start and end times.
    ///
    /// # Arguments
    ///
    /// * `t_start` - The inclusive start time of the window.
    /// * `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// The number of edges in the windowed view for the given window.
    fn edges_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.graph
            .edges_len_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    /// Check if there is an edge from src to dst in the window.
    ///
    /// # Arguments
    ///
    /// - `src` - The source vertex.
    /// - `dst` - The destination vertex.
    ///
    /// # Returns
    ///
    /// A result containing `true` if there is an edge from src to dst in the window, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if either `src` or `dst` is not a valid vertex.
    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef) -> bool {
        self.graph
            .has_edge_ref_window(src, dst, self.t_start, self.t_end)
    }

    /// Check if there is an edge from src to dst in the window defined by t_start and t_end.
    ///
    /// # Arguments
    ///
    /// - `src` - The source vertex.
    /// - `dst` - The destination vertex.
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// A result containing `true` if there is an edge from src to dst in the window, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if either `src` or `dst` is not a valid vertex.
    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> bool {
        self.graph
            .has_edge_ref_window(src, dst, self.actual_start(t_start), self.actual_end(t_end))
    }

    /// Check if a vertex v exists in the window.
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to check.
    ///
    /// # Returns
    ///
    /// A result containing `true` if the vertex exists in the window, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid vertex.
    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.graph
            .has_vertex_ref_window(v, self.t_start, self.t_end)
    }

    /// Check if a vertex v exists in the window defined by t_start and t_end.
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to check.
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// A result containing `true` if the vertex exists in the window, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid vertex.
    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
        self.graph
            .has_vertex_ref_window(v, self.actual_start(t_start), self.actual_end(t_end))
    }

    /// Returns the number of edges from a vertex in the window.
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to check.
    /// - `d` - The direction of the edges to count.
    ///
    /// # Returns
    ///
    /// A result containing the number of edges from the vertex in the window.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid vertex.
    fn degree(&self, v: VertexRef, d: Direction) -> usize {
        self.graph.degree_window(v, self.t_start, self.t_end, d)
    }

    /// Returns the number of edges from a vertex in the window defined by t_start and t_end.
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to check.
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    /// - `d` - The direction of the edges to count.
    ///
    /// # Returns
    ///
    /// A result containing the number of edges from the vertex in the window.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid vertex.
    fn degree_window(&self, v: VertexRef, t_start: i64, t_end: i64, d: Direction) -> usize {
        self.graph
            .degree_window(v, self.actual_start(t_start), self.actual_end(t_end), d)
    }

    /// Get the reference of the vertex with ID v if it exists
    ///
    /// # Arguments
    ///
    /// - `v` - The ID of the vertex to get
    ///
    /// # Returns
    ///
    /// A result of an option containing the vertex reference if it exists, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid vertex.
    fn vertex_ref(&self, v: u64) -> Option<VertexRef> {
        self.graph.vertex_ref_window(v, self.t_start, self.t_end)
    }

    /// Get the reference of the vertex with ID v if it exists in a window
    ///
    /// # Arguments
    ///
    /// - `v` - The ID of the vertex to get
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// A result of an option containing the vertex reference if it exists, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `v` is not a valid vertex.
    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexRef> {
        self.graph
            .vertex_ref_window(v, self.actual_start(t_start), self.actual_end(t_end))
    }

    fn vertex_earliest_time(&self, v: VertexRef) -> Option<i64> {
        self.graph
            .vertex_earliest_time_window(v, self.t_start, self.t_end)
    }

    fn vertex_earliest_time_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph.vertex_earliest_time_window(
            v,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    fn vertex_latest_time(&self, v: VertexRef) -> Option<i64> {
        self.graph
            .vertex_latest_time_window(v, self.t_start, self.t_end)
    }

    fn vertex_latest_time_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph
            .vertex_latest_time_window(v, self.actual_start(t_start), self.actual_end(t_end))
    }

    /// Get an iterator over the IDs of all vertices
    ///
    /// # Returns
    ///
    /// An iterator over the IDs of all vertices
    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.vertex_ids_window(self.t_start, self.t_end)
    }

    /// Get an iterator over the IDs of all vertices in a window
    ///
    /// # Arguments
    ///
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// An iterator over the IDs of all vertices
    fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph
            .vertex_ids_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    /// Get an iterator over the references of all vertices as references
    ///
    /// # Returns
    ///
    /// An iterator over the references of all vertices
    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertex_refs_window(self.t_start, self.t_end)
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph
            .vertex_refs_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph
            .vertex_refs_window_shard(shard, self.t_start, self.t_end)
    }

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertex_refs_window_shard(
            shard,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    /// Get an iterator over the references of an edges as a reference
    ///
    /// # Arguments
    ///
    /// - `src` - The source vertex of the edge
    /// - `dst` - The destination vertex of the edge
    ///
    /// # Returns
    ///
    /// A result of an option containing the edge reference if it exists, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `src` or `dst` are not valid vertices.
    fn edge_ref(&self, src: VertexRef, dst: VertexRef) -> Option<EdgeRef> {
        self.graph
            .edge_ref_window(src, dst, self.t_start, self.t_end)
    }

    /// Get an iterator over the references of an edges as a reference in a window
    ///
    /// # Arguments
    ///
    /// - `src` - The source vertex of the edge
    /// - `dst` - The destination vertex of the edge
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// A result of an option containing the edge reference if it exists, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `src` or `dst` are not valid vertices.
    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<EdgeRef> {
        self.graph
            .edge_ref_window(src, dst, self.actual_start(t_start), self.actual_end(t_end))
    }

    /// Get an iterator of all edges as references
    ///
    /// # Returns
    ///
    /// An iterator over all edges as references
    fn edge_refs(&self) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph.edge_refs_window(self.t_start, self.t_end)
    }

    /// Get an iterator of all edges as references in a window
    ///
    /// # Arguments
    ///
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// An iterator over all edges as references
    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph
            .edge_refs_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    /// Get an iterator of all edges as references for a given vertex and direction
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the edges for
    /// - `d` - The direction of the edges
    ///
    /// # Returns
    ///
    /// An iterator over all edges in that vertex direction as references
    fn vertex_edges(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph
            .vertex_edges_window(v, self.t_start, self.t_end, d)
    }

    fn vertex_edges_t(
        &self,
        v: VertexRef,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph
            .vertex_edges_window_t(v, self.t_start, self.t_end, d)
    }

    /// Get an iterator of all edges as references for a given vertex and direction in a window
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the edges for
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    /// - `d` - The direction of the edges
    ///
    /// # Returns
    ///
    /// An iterator over all edges in that vertex direction as references
    fn vertex_edges_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph
            .vertex_edges_window(v, self.actual_start(t_start), self.actual_end(t_end), d)
    }

    /// Get an iterator of all edges as references for a given vertex and direction in a window
    /// but exploded. This means, if a timestamp has two edges, they will be returned as two
    /// seperate edges.
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the edges for
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    /// - `d` - The direction of the edges
    ///
    /// # Returns
    ///
    /// An iterator over all edges in that vertex direction as references

    fn vertex_edges_window_t(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph
            .vertex_edges_window_t(v, self.actual_start(t_start), self.actual_end(t_end), d)
    }

    /// Get the neighbours of a vertex as references in a given direction
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the neighbours for
    /// - `d` - The direction of the edges
    ///
    /// # Returns
    ///
    /// An iterator over all neighbours in that vertex direction as references
    fn neighbours(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.neighbours_window(v, self.t_start, self.t_end, d)
    }

    /// Get the neighbours of a vertex as references in a given direction across a window
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the neighbours for
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    /// - `d` - The direction of the edges
    ///
    /// # Returns
    ///
    /// An iterator over all neighbours in that vertex direction as references
    fn neighbours_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph
            .neighbours_window(v, self.actual_start(t_start), self.actual_end(t_end), d)
    }

    /// Get the neighbours of a vertex as vertex ids in a given direction
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the neighbours for
    /// - `d` - The direction of the edges
    ///
    /// # Returns
    ///
    /// An iterator over all neighbours in that vertex direction as ids
    fn neighbours_ids(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph
            .neighbours_ids_window(v, self.t_start, self.t_end, d)
    }

    /// Get the neighbours of a vertex as vertex ids in a given direction across a window
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the neighbours for
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    /// - `d` - The direction of the edges
    ///
    /// # Returns
    ///
    /// An iterator over all neighbours in that vertex direction as ids
    fn neighbours_ids_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph
            .neighbours_ids_window(v, self.actual_start(t_start), self.actual_end(t_end), d)
    }

    /// Get the static property of a vertex
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the property for
    /// - `name` - The name of the property
    ///
    /// # Returns
    ///
    /// A result of an option of a property
    fn static_vertex_prop(&self, v: VertexRef, name: String) -> Option<Prop> {
        self.graph.static_vertex_prop(v, name)
    }

    /// Get all static property names of a vertex
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the property for
    ///
    /// # Returns
    ///
    /// a Vector of Strings representing all the property names
    fn static_vertex_prop_names(&self, v: VertexRef) -> Vec<String> {
        self.graph.static_vertex_prop_names(v)
    }

    /// Get all temporal property names of a vertex
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the property for
    ///
    /// # Returns
    ///
    /// a Vector of Strings representing all the property names
    fn temporal_vertex_prop_names(&self, v: VertexRef) -> Vec<String> {
        self.graph.temporal_vertex_prop_names(v)
    }

    /// Get the temporal property of a vertex
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the property for
    /// - `name` - The name of the property
    ///
    /// # Returns
    ///
    /// A result of an vector of a tuple of a timestamp and a property
    fn temporal_vertex_prop_vec(&self, v: VertexRef, name: String) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_vertex_prop_vec_window(v, name, self.t_start, self.t_end)
    }

    /// Get the temporal property of a vertex in a window
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the property for
    /// - `name` - The name of the property
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// A result of an vector of a tuple of a timestamp and a property
    ///
    /// # Errors
    ///
    /// - `GraphError` - Raised if vertex or property does not exist
    fn temporal_vertex_prop_vec_window(
        &self,
        v: VertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec_window(
            v,
            name,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    /// Get all temporal properties of a vertex
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the property for
    ///
    /// # Returns
    ///
    /// A result of an vector of a tuple of a timestamp and a property
    ///
    /// # Errors
    ///
    /// - `GraphError` - Raised if vertex or property does not exist
    fn temporal_vertex_props(&self, v: VertexRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph
            .temporal_vertex_props_window(v, self.t_start, self.t_end)
    }

    /// Get all temporal properties of a vertex in a window
    ///
    /// # Arguments
    ///
    /// - `v` - The vertex to get the property for
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// A result of an hashmap of a tuple of a string being names and
    /// vectors of timestamp and the property value
    ///
    /// # Errors
    ///
    /// - `GraphError` - Raised if vertex or property does not exist
    fn temporal_vertex_props_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_vertex_props_window(
            v,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    /// Get the static property of an edge
    ///
    /// # Arguments
    ///
    /// - `e` - The edge to get the property for
    /// - `name` - The name of the property
    ///
    /// # Returns
    ///
    /// A result of an option of a property  or a graph error
    ///
    /// # Errors
    ///
    /// - `GraphError` - Raised if edge or property does not exist
    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        self.graph.static_edge_prop(e, name)
    }

    /// Get the names of all static properties of an edge
    ///
    /// # Arguments
    ///
    /// - `e` - The edge to get the property for
    ///
    /// # Returns
    ///
    /// A result of an vector of all property names
    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.graph.static_edge_prop_names(e)
    }

    /// Get the names of all temporal properties of an edge
    ///
    /// # Arguments
    ///
    /// - `e` - The edge to get the property for
    ///
    /// # Returns
    ///
    /// A result of an vector of all property names
    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.graph.temporal_edge_prop_names(e)
    }

    /// Get the temporal property of an edge
    ///
    /// # Arguments
    ///
    /// - `e` - The edge to get the property for
    /// - `name` - The name of the property
    ///
    /// # Returns
    ///
    /// A result of an option of a property or a graph error
    ///
    /// # Errors
    ///
    /// - `GraphError` - Raised if edge or property does not exist
    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_edge_props_vec_window(e, name, self.t_start, self.t_end)
    }

    /// Get the temporal property of an edge in a window
    ///
    /// # Arguments
    ///
    /// - `e` - The edge to get the property for
    /// - `name` - The name of the property
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// A result of an vector of a timestamp and property or a graph error
    ///
    /// # Errors
    ///
    /// - `GraphError` - Returned if edge or property does not exist
    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_props_vec_window(
            e,
            name,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    /// Get all temporal properties of an edge
    ///
    /// # Arguments
    ///
    /// - `e` - The edge to get the property for
    ///
    /// # Returns
    ///
    /// A hashmap containing the name of a property as a key
    /// and the vector of a timestamp and property value
    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph
            .temporal_edge_props_window(e, self.t_start, self.t_end)
    }

    /// Get all temporal properties of an edge in a window
    ///
    /// # Arguments
    ///
    /// - `e` - The edge to get the property for
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// A hashmap containing the name of a property as a key
    /// and the vector of a timestamp and property value
    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph
            .temporal_edge_props_window(e, self.actual_start(t_start), self.actual_end(t_end))
    }

    fn num_shards(&self) -> usize {
        self.graph.num_shards()
    }

    fn vertices_shard(&self, shard_id: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph
            .vertices_shard_window(shard_id, self.t_start, self.t_end)
    }

    fn vertices_shard_window(
        &self,
        shard_id: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertices_shard_window(
            shard_id,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }
}

/// A windowed graph is a graph that only allows access to vertices and edges within a time window.
///
/// This struct is used to represent a graph with a time window. It is constructed
/// by providing a `Graph` object and a time range that defines the window.
///
/// # Examples
///
/// ```rust
/// use docbrown_db::graph::Graph;
/// use docbrown_db::view_api::*;
///
/// let graph = Graph::new(1);
/// graph.add_edge(0, 1, 2, &vec![]);
/// graph.add_edge(1, 2, 3, &vec![]);
/// let windowed_graph = graph.window(0, 1);
/// ```
impl<G: GraphViewInternalOps> WindowedGraph<G> {
    /// Create a new windowed graph
    ///
    /// # Arguments
    ///
    /// - `graph` - The graph to create the windowed graph from
    /// - `t_start` - The inclusive start time of the window.
    /// - `t_end` - The exclusive end time of the window.
    ///
    /// # Returns
    ///
    /// A new windowed graph
    pub fn new(graph: G, t_start: i64, t_end: i64) -> Self {
        WindowedGraph {
            graph,
            t_start,
            t_end,
        }
    }
}

#[cfg(test)]
mod views_test {

    use super::*;
    use crate::graph::Graph;
    use crate::view_api::*;
    use docbrown_core::Prop;
    use itertools::Itertools;
    use quickcheck::TestResult;
    use rand::prelude::*;
    use rayon::prelude::*;

    #[test]
    fn windowed_graph_vertices_degree() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]).unwrap();
        }

        let wg = WindowedGraph::new(g, -1, 1);

        let actual = wg
            .vertices()
            .iter()
            .map(|v| (v.id(), v.degree()))
            .collect::<Vec<_>>();

        let expected = vec![(2, 1), (1, 2)];

        assert_eq!(actual, expected);
    }

    #[test]
    fn windowed_graph_edge() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(2);

        for (t, src, dst) in vs {
            g.add_edge(t, src, dst, &vec![]).unwrap();
        }

        let wg = g.window(i64::MIN, i64::MAX);
        assert_eq!(wg.edge(1, 3).unwrap().src().id(), 1);
        assert_eq!(wg.edge(1, 3).unwrap().dst().id(), 3);
    }

    #[test]
    fn windowed_graph_vertex_edges() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]).unwrap();
        }

        let wg = WindowedGraph::new(g, -1, 1);

        assert_eq!(wg.vertex(1).unwrap().id(), 1);
    }

    #[test]
    fn graph_has_vertex_check_fail() {
        let vs: Vec<(i64, u64)> = vec![
            (1, 0),
            (-100, 262),
            // (327226439, 108748364996394682),
            (1, 9135428456135679950),
            // (0, 1),
            // (2, 2),
        ];
        let g = Graph::new(2);

        for (t, v) in &vs {
            g.add_vertex(*t, *v, &vec![])
                .map_err(|err| println!("{:?}", err))
                .ok();
        }

        let wg = WindowedGraph::new(g, 1, 2);
        assert!(!wg.has_vertex(262))
    }

    #[quickcheck]
    fn windowed_graph_has_vertex(mut vs: Vec<(i64, u64)>) -> TestResult {
        if vs.is_empty() {
            return TestResult::discard();
        }

        vs.sort_by_key(|v| v.1); // Sorted by vertex
        vs.dedup_by_key(|v| v.1); // Have each vertex only once to avoid headaches
        vs.sort_by_key(|v| v.0); // Sorted by time

        let rand_start_index = thread_rng().gen_range(0..vs.len());
        let rand_end_index = thread_rng().gen_range(rand_start_index..vs.len());

        let g = Graph::new(2);

        for (t, v) in &vs {
            g.add_vertex(*t, *v, &vec![])
                .map_err(|err| println!("{:?}", err))
                .ok();
        }

        let start = vs.get(rand_start_index).expect("start index in range").0;
        let end = vs.get(rand_end_index).expect("end index in range").0;

        let wg = WindowedGraph::new(g, start, end);

        let rand_test_index: usize = thread_rng().gen_range(0..vs.len());

        let (i, v) = vs.get(rand_test_index).expect("test index in range");
        if (start..end).contains(i) {
            if wg.has_vertex(*v) {
                TestResult::passed()
            } else {
                TestResult::error(format!(
                    "Vertex {:?} was not in window {:?}",
                    (i, v),
                    start..end
                ))
            }
        } else if !wg.has_vertex(*v) {
            TestResult::passed()
        } else {
            TestResult::error(format!(
                "Vertex {:?} was in window {:?}",
                (i, v),
                start..end
            ))
        }
    }

    #[quickcheck]
    fn windowed_graph_has_edge(mut edges: Vec<(i64, (u64, u64))>) -> TestResult {
        if edges.is_empty() {
            return TestResult::discard();
        }

        edges.sort_by_key(|e| e.1); // Sorted by edge
        edges.dedup_by_key(|e| e.1); // Have each edge only once to avoid headaches
        edges.sort_by_key(|e| e.0); // Sorted by time

        let rand_start_index = thread_rng().gen_range(0..edges.len());
        let rand_end_index = thread_rng().gen_range(rand_start_index..edges.len());

        let g = Graph::new(2);

        for (t, e) in &edges {
            g.add_edge(*t, e.0, e.1, &vec![]).unwrap();
        }

        let start = edges.get(rand_start_index).expect("start index in range").0;
        let end = edges.get(rand_end_index).expect("end index in range").0;

        let wg = WindowedGraph::new(g, start, end);

        let rand_test_index: usize = thread_rng().gen_range(0..edges.len());

        let (i, e) = edges.get(rand_test_index).expect("test index in range");
        if (start..end).contains(i) {
            if wg.has_edge(e.0, e.1) {
                TestResult::passed()
            } else {
                TestResult::error(format!(
                    "Edge {:?} was not in window {:?}",
                    (i, e),
                    start..end
                ))
            }
        } else if !wg.has_edge(e.0, e.1) {
            TestResult::passed()
        } else {
            TestResult::error(format!("Edge {:?} was in window {:?}", (i, e), start..end))
        }
    }

    #[quickcheck]
    fn windowed_graph_edge_count(mut edges: Vec<(i64, (u64, u64))>) -> TestResult {
        edges.sort_by_key(|e| e.1); // Sorted by edge
        edges.dedup_by_key(|e| e.1); // Have each edge only once to avoid headaches

        let mut window: [i64; 2] = thread_rng().gen();
        window.sort();
        let window = window[0]..window[1];
        let true_edge_count = edges.iter().filter(|e| window.contains(&e.0)).count();

        let g = Graph::new(2);

        for (t, e) in &edges {
            g.add_edge(*t, e.0, e.1, &vec![("test".to_owned(), Prop::Bool(true))])
                .unwrap();
        }

        let wg = WindowedGraph::new(g, window.start, window.end);
        TestResult::from_bool(wg.num_edges() == true_edge_count)
    }

    #[quickcheck]
    fn trivial_window_has_all_edges(edges: Vec<(i64, u64, u64)>) -> bool {
        let g = Graph::new(10);
        edges
            .into_par_iter()
            .filter(|e| e.0 < i64::MAX)
            .for_each(|(t, src, dst)| {
                g.add_edge(t, src, dst, &vec![("test".to_owned(), Prop::Bool(true))])
                    .unwrap()
            });
        let w = g.window(i64::MIN, i64::MAX);
        g.edges().all(|e| w.has_edge(e.src().id(), e.dst().id()))
    }

    #[quickcheck]
    fn large_vertex_in_window(dsts: Vec<u64>) -> bool {
        let dsts: Vec<u64> = dsts.into_iter().unique().collect();
        let n = dsts.len();
        let g = Graph::new(1);

        for dst in dsts {
            let t = 1;
            g.add_edge(t, 0, dst, &vec![]).unwrap();
        }
        let w = g.window(i64::MIN, i64::MAX);
        w.num_edges() == n
    }

    #[test]
    fn windowed_graph_vertex_ids() {
        let vs = vec![(1, 1, 2), (3, 3, 4), (5, 5, 6), (7, 7, 1)];

        let args = vec![(i64::MIN, 8), (i64::MIN, 2), (i64::MIN, 4), (3, 6)];

        let expected = vec![
            vec![1, 2, 3, 4, 5, 6, 7],
            vec![1, 2],
            vec![1, 2, 3, 4],
            vec![3, 4, 5, 6],
        ];

        let g = Graph::new(1);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]).unwrap();
        }

        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = g.window(args[i].0, args[i].1);
                let mut e = wg.vertex_ids().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();

        assert_eq!(res, expected);

        let g = Graph::new(3);
        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]).unwrap();
        }
        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = g.window(args[i].0, args[i].1);
                let mut e = wg.vertex_ids().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();
        assert_eq!(res, expected);
    }

    #[test]
    fn windowed_graph_vertices() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(1);

        g.add_vertex(
            0,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(99.5)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_vertex(
            -1,
            2,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(10.0)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_vertex(
            6,
            3,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(76.2)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        for (t, src, dst) in &vs {
            g.add_edge(
                *t,
                *src,
                *dst,
                &vec![("eprop".into(), Prop::Str("commons".into()))],
            )
            .unwrap();
        }

        let wg = g.window(-2, 0);

        let actual = wg.vertices().id().collect::<Vec<_>>();

        let expected = vec![1, 2];

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = Graph::new(10);

        g.add_vertex(
            0,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(99.5)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_vertex(
            -1,
            2,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(10.0)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        g.add_vertex(
            6,
            3,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(76.2)),
            ],
        )
        .map_err(|err| println!("{:?}", err))
        .ok();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]).unwrap();
        }

        let expected = wg.vertices().id().collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
