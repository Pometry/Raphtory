use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, VID, LayerIds},
        Direction,
    },
    db::api::view::internal::{time_semantics::TimeSemantics, CoreGraphOps, GraphOps},
};
use itertools::*;

/// Methods for interacting with windowed data (automatically implemented based on `TimeSemantics` trait
pub trait GraphWindowOps {
    /// Check if a vertex exists locally in a window and returns local reference.
    fn local_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<VID>;

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
    fn edges_len_window(&self, t_start: i64, t_end: i64, layers: LayerIds) -> usize;

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
        layer: LayerIds,
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
    /// * `v` - VID of the vertex to check.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    fn degree_window(
        &self,
        v: VID,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layers: LayerIds,
    ) -> usize;

    /// Returns the VID that corresponds to the specified vertex ID (v) created
    /// between the start (t_start) and end (t_end) timestamps (inclusive).
    /// Returns None if the vertex ID is not present in the graph.
    /// # Arguments
    ///
    /// * `v` - The vertex ID to lookup.
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    /// * `Option<VID>` - The VID of the vertex if it exists in the graph.
    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VID>;

    /// Returns all the vertex references in the graph created between the start (t_start) and
    /// end (t_end) timestamps (inclusive).
    /// # Arguments
    ///
    /// * `t_start` - The start time of the window (inclusive).
    /// * `t_end` - The end time of the window (exclusive).
    ///
    /// # Returns
    /// * `Box<dyn Iterator<Item = VID> + Send>` - An iterator over all the vertexes
    fn vertex_refs_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = VID> + Send>;

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
        layer: LayerIds,
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
        layers: LayerIds,
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
        v: VID,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layers: LayerIds,
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
        v: VID,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;
}

impl<G: TimeSemantics + CoreGraphOps + GraphOps + Clone + 'static> GraphWindowOps for G {
    fn local_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> Option<VID> {
        self.local_vertex_ref(v)
            .filter(|&v| self.include_vertex_window(v, t_start..t_end))
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.vertex_refs_window(t_start, t_end).count()
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64, layers: LayerIds) -> usize {
        self.edge_refs_window(t_start, t_end, layers).count()
    }

    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: LayerIds,
    ) -> bool {
        self.edge_ref_window(src, dst, t_start, t_end, layer)
            .is_some()
    }

    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
        self.local_vertex_ref_window(v, t_start, t_end).is_some()
    }

    fn degree_window(
        &self,
        v: VID,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layers: LayerIds,
    ) -> usize {
        self.neighbours_window(v, t_start, t_end, d, layers).count()
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VID> {
        self.local_vertex_ref_window(v.into(), t_start, t_end)
    }

    fn vertex_refs_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = VID> + Send> {
        let g = self.clone();
        Box::new(
            self.vertex_refs()
                .filter(move |&v| g.include_vertex_window(v, t_start..t_end)),
        )
    }

    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer: LayerIds,
    ) -> Option<EdgeRef> {
        self.edge_ref(src, dst, layer.clone())
            .filter(|&e| self.include_edge_window(e, t_start..t_end, layer))
    }

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
        layer: LayerIds,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g = self.clone();
        Box::new(
            self.edge_refs(layer.clone())
                .filter(move |&e| g.include_edge_window(e, t_start..t_end, layer.clone())),
        )
    }

    fn vertex_edges_window(
        &self,
        v: VID,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g = self.clone();
        Box::new(
            self.vertex_edges(v, d, layers.clone())
                .filter(move |&e| g.include_edge_window(e, t_start..t_end, layers.clone())),
        )
    }

    fn neighbours_window(
        &self,
        v: VID,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        Box::new(
            self.vertex_edges_window(v, t_start, t_end, d, layers)
                .map(|e| e.remote())
                .dedup(),
        )
    }
}
