use crate::core::edge_ref::EdgeRef;
use crate::core::Direction;
use crate::core::tgraph2::VID;
use crate::db::view_api::internal::time_semantics::TimeSemantics;
use crate::db::view_api::internal::GraphOps;
use crate::db::view_api::BoxedIter;
use std::ops::Range;

/// Additional methods for returning exploded edge data that are automatically implemented
pub trait ExplodedEdgeOps {
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
        v: VID,
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
        v: VID,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    /// Get the activation timestamps for an edge `e`
    fn edge_history(&self, e: EdgeRef) -> BoxedIter<i64>;

    /// Get the activation timestamps for an edge `e` in window `w`
    fn edge_history_window(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<i64>;
}

impl<G: GraphOps + TimeSemantics + Clone + 'static> ExplodedEdgeOps for G {
    fn vertex_edges_t(
        &self,
        v: VID,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        {
            let g = self.clone();
            Box::new(
                self.vertex_edges(v, d, layer)
                    .flat_map(move |e| g.edge_t(e)),
            )
        }
    }

    fn vertex_edges_window_t(
        &self,
        v: VID,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        let g = self.clone();
        Box::new(
            self.vertex_edges(v, d, layer)
                .flat_map(move |e| g.edge_window_t(e, t_start..t_end)),
        )
    }

    fn edge_history(&self, e: EdgeRef) -> BoxedIter<i64> {
        Box::new(self.edge_t(e).map(|e| e.time().expect("exploded")))
    }

    fn edge_history_window(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<i64> {
        Box::new(
            self.edge_window_t(e, w)
                .map(|e| e.time().expect("exploded")),
        )
    }
}
