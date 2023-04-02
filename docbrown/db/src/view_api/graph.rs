use crate::edge::EdgeView;
use crate::graph_window::{GraphWindowSet, WindowedGraph};
use crate::perspective::{Perspective, PerspectiveIterator, PerspectiveSet};
use crate::vertex::VertexView;
use crate::vertices::Vertices;
use crate::view_api::internal::GraphViewInternalOps;
use docbrown_core::tgraph::VertexRef;
use docbrown_core::tgraph_shard::errors::GraphError;
use std::iter;

/// This trait GraphViewOps defines operations for accessing
/// information about a graph. The trait has associated types
/// that are used to define the type of the vertices, edges
/// and the corresponding iterators.
pub trait GraphViewOps: Send + Sync + Sized + GraphViewInternalOps + 'static + Clone {
    /// Return the number of vertices in the graph.
    fn num_vertices(&self) -> usize;

    /// Return the earliest timestamp in the graph.
    fn earliest_time(&self) -> Option<i64>;

    /// Return the latest timestamp in the graph.
    fn latest_time(&self) -> Option<i64>;

    /// Check if the graph is empty.
    fn is_empty(&self) -> bool {
        self.num_vertices() == 0
    }

    /// Return the number of edges in the graph.
    fn num_edges(&self) -> usize;

    /// Check if the graph contains a vertex `v`.
    fn has_vertex<T: Into<VertexRef>>(&self, v: T) -> bool;

    /// Check if the graph contains an edge given a pair of vertices `(src, dst)`.
    fn has_edge<T: Into<VertexRef>>(&self, src: T, dst: T) -> bool;

    /// Get a vertex `v`.
    fn vertex<T: Into<VertexRef>>(&self, v: T) -> Option<VertexView<Self>>;

    /// Return a View of the vertices in the Graph
    fn vertices(&self) -> Vertices<Self>;

    /// Get an edge `(src, dst)`.
    fn edge<T: Into<VertexRef>>(&self, src: T, dst: T) -> Option<EdgeView<Self>>;

    /// Return an iterator over all edges in the graph.
    fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<Self>> + Send>;

    fn window(&self, t_start: i64, t_end: i64) -> WindowedGraph<Self>;
    fn at(&self, end: i64) -> WindowedGraph<Self> {
        self.window(i64::MIN, end.saturating_add(1))
    }
    fn through_perspectives(&self, perspectives: PerspectiveSet) -> GraphWindowSet<Self> {
        let iter = match (self.earliest_time(), self.latest_time()) {
            (Some(start), Some(end)) => perspectives.build_iter(start..end),
            _ => PerspectiveIterator::empty(),
        };
        GraphWindowSet::new(self.clone(), Box::new(iter))
    }

    fn through_iter(
        &self,
        perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
    ) -> GraphWindowSet<Self> {
        let iter = if self.earliest_time().is_some() && self.latest_time().is_some() {
            perspectives
        } else {
            Box::new(iter::empty::<Perspective>())
        };
        GraphWindowSet::new(self.clone(), iter)
    }
}

impl<G: Send + Sync + Sized + GraphViewInternalOps + 'static + Clone> GraphViewOps for G {
    fn num_vertices(&self) -> usize {
        self.vertices_len()
    }
    fn earliest_time(&self) -> Option<i64> {
        self.earliest_time_global()
    }

    fn latest_time(&self) -> Option<i64> {
        self.latest_time_global()
    }

    fn num_edges(&self) -> usize {
        self.edges_len()
    }

    fn has_vertex<T: Into<VertexRef>>(&self, v: T) -> bool {
        self.has_vertex_ref(v.into())
    }

    fn has_edge<T: Into<VertexRef>>(&self, src: T, dst: T) -> bool {
        self.has_edge_ref(src.into(), dst.into())
    }

    fn vertex<T: Into<VertexRef>>(&self, v: T) -> Option<VertexView<Self>> {
        let v = v.into().g_id;
        self.vertex_ref(v).map(|v| VertexView::new(self.clone(), v))
    }

    fn vertices(&self) -> Vertices<Self> {
        let graph = self.clone();
        Vertices::new(graph)
    }

    fn edge<T: Into<VertexRef>>(&self, src: T, dst: T) -> Option<EdgeView<Self>> {
        self.edge_ref(src.into(), dst.into())
            .map(|e| EdgeView::new(self.clone(), e))
    }

    fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<Self>> + Send> {
        Box::new(self.vertices().iter().flat_map(|v| v.out_edges()))
    }

    fn window(&self, t_start: i64, t_end: i64) -> WindowedGraph<Self> {
        WindowedGraph::new(self.clone(), t_start, t_end)
    }
}
