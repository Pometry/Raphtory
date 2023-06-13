use crate::core::edge_ref::EdgeRef;
use crate::core::vertex_ref::LocalVertexRef;
use crate::core::Direction;
use crate::db::view_api::internal::graph_window_ops::GraphWindowOps;
use crate::db::view_api::internal::time_semantics::TimeSemantics;
use crate::db::view_api::internal::GraphViewInternalOps;
use crate::db::view_api::BoxedIter;
use std::ops::Range;

pub trait ExplodedEdgeOps {
    fn edge_t(&self, e: EdgeRef) -> BoxedIter<EdgeRef>;

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<EdgeRef>;

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
}

impl<G: GraphViewInternalOps> ExplodedEdgeOps for G {
    fn edge_t(&self, e: EdgeRef) -> BoxedIter<EdgeRef> {
        Box::new(self.edge_history(e).map(|(t, d)| e.at(t)))
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<EdgeRef> {
        Box::new(self.edge_history_window(e, w).map(|(t, d)| e.at(t)))
    }

    fn vertex_edges_t(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        {
            Box::new(self.vertex_edges(v, d, layer).flat_map(|e| self.edge_t(e)))
        }
    }

    fn vertex_edges_window_t(
        &self,
        v: LocalVertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        Box::new(self.vertex_edges(v, d, layer).flat_map(|e| {
            self.edge_history_window(e, t_start..t_end)
                .map(|(t, d)| e.at(t))
        }))
    }
}
