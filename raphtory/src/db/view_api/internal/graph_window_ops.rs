use crate::core::edge_ref::EdgeRef;
use crate::core::timeindex::TimeIndexOps;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::{Direction, Prop};
use crate::db::view_api::internal::time_semantics::TimeSemantics;
use crate::db::view_api::internal::{CoreGraphOps, GraphViewInternalOps};
use itertools::Itertools;
use std::collections::HashMap;


/// Methods for interacting with windowed data (automatically implemented based on `TimeSemantics` trait
pub trait GraphWindowOps: GraphViewInternalOps {
    /// Check if a vertex exists locally in a window and returns local reference.
    fn local_vertex_ref_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<LocalVertexRef>;

    /// Returns the number of vertices in the graph that were created between
    /// the start (t_start) and end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize;

    /// Returns the number of edges in the graph that were created between the
    /// start (t_start) and end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize;

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

    /// Returns true if the graph contains the specified vertex (v) created between the
    /// start (t_start) and end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `v` - VertexRef of the vertex to check.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool;

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
}

impl<G: GraphViewInternalOps + TimeSemantics + CoreGraphOps + Clone + 'static> GraphWindowOps
    for G
{
    fn local_vertex_ref_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<LocalVertexRef> {
        self.local_vertex_ref(v)
            .filter(|&v| self.include_vertex_window(v, t_start..t_end))
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.vertex_refs_window(t_start, t_end).count()
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize {
        self.edge_refs_window(t_start, t_end, layer).count()
    }

    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> bool {
        self.edge_ref_window(src, dst, t_start, t_end, layer)
            .is_some()
    }

    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
        self.local_vertex_ref_window(v, t_start, t_end).is_some()
    }

    fn degree_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> usize {
        self.neighbours_window(v, t_start, t_end, d, layer).count()
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<LocalVertexRef> {
        self.local_vertex_ref_window(v.into(), t_start, t_end)
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        let g = self.clone();
        Box::new(
            self.vertex_refs()
                .filter(move |&v| g.include_vertex_window(v, t_start..t_end)),
        )
    }

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        let g = self.clone();
        Box::new(
            self.vertex_refs_shard(shard)
                .filter(move |&v| g.include_vertex_window(v, t_start..t_end)),
        )
    }

    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> Option<EdgeRef> {
        self.edge_ref(src, dst, layer)
            .filter(|&e| self.include_edge_window(e, t_start..t_end))
    }

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g = self.clone();
        Box::new(
            self.edge_refs(layer)
                .filter(move |&e| g.include_edge_window(e, t_start..t_end)),
        )
    }

    fn vertex_edges_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g = self.clone();
        Box::new(
            self.vertex_edges(v, d, layer)
                .filter(move |&e| g.include_edge_window(e, t_start..t_end)),
        )
    }

    fn neighbours_window(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        Box::new(
            self.vertex_edges_window(v, t_start, t_end, d, layer)
                .map(|e| e.remote())
                .dedup(),
        )
    }
}
