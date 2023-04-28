use crate::core::tgraph::VertexRef;
use crate::db::edge::EdgeView;
use crate::db::graph_layer::LayeredGraph;
use crate::db::graph_window::WindowedGraph;
use crate::db::vertex::VertexView;
use crate::db::vertices::Vertices;
use crate::db::view_api::internal::GraphViewInternalOps;
use crate::db::view_api::time::TimeOps;
use crate::db::view_api::VertexViewOps;

/// This trait GraphViewOps defines operations for accessing
/// information about a graph. The trait has associated types
/// that are used to define the type of the vertices, edges
/// and the corresponding iterators.
pub trait GraphViewOps: Send + Sync + Sized + GraphViewInternalOps + 'static + Clone {
    /// Timestamp of earliest activity in the graph
    fn earliest_time(&self) -> Option<i64>;
    /// Timestamp of latest activity in the graph
    fn latest_time(&self) -> Option<i64>;
    /// Return the number of vertices in the graph.
    fn num_vertices(&self) -> usize;

    /// Check if the graph is empty.
    fn is_empty(&self) -> bool {
        self.num_vertices() == 0
    }

    /// Return the number of edges in the graph.
    fn num_edges(&self) -> usize;

    /// Check if the graph contains a vertex `v`.
    fn has_vertex<T: Into<VertexRef>>(&self, v: T) -> bool;

    /// Check if the graph contains an edge given a pair of vertices `(src, dst)`.
    fn has_edge<T: Into<VertexRef>>(&self, src: T, dst: T, layer: Option<&str>) -> bool;

    /// Get a vertex `v`.
    fn vertex<T: Into<VertexRef>>(&self, v: T) -> Option<VertexView<Self>>;

    /// Return a View of the vertices in the Graph
    fn vertices(&self) -> Vertices<Self>;

    /// Get an edge `(src, dst)`.
    fn edge<T: Into<VertexRef>>(
        &self,
        src: T,
        dst: T,
        layer: Option<&str>,
    ) -> Option<EdgeView<Self>>;

    /// Return an iterator over all edges in the graph.
    fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<Self>> + Send>;
}

impl<G: Send + Sync + Sized + GraphViewInternalOps + 'static + Clone> GraphViewOps for G {
    fn earliest_time(&self) -> Option<i64> {
        self.earliest_time_global()
    }

    fn latest_time(&self) -> Option<i64> {
        self.latest_time_global()
    }

    fn num_vertices(&self) -> usize {
        self.vertices_len()
    }

    fn num_edges(&self) -> usize {
        self.edges_len(None)
    }

    fn has_vertex<T: Into<VertexRef>>(&self, v: T) -> bool {
        self.has_vertex_ref(v.into())
    }

    fn has_edge<T: Into<VertexRef>>(&self, src: T, dst: T, layer: Option<&str>) -> bool {
        match self.get_layer(layer) {
            Some(layer_id) => self.has_edge_ref(src.into(), dst.into(), layer_id),
            None => false,
        }
    }

    fn vertex<T: Into<VertexRef>>(&self, v: T) -> Option<VertexView<Self>> {
        let v = v.into().g_id;
        self.vertex_ref(v).map(|v| VertexView::new(self.clone(), v))
    }

    fn vertices(&self) -> Vertices<Self> {
        let graph = self.clone();
        Vertices::new(graph)
    }

    fn edge<T: Into<VertexRef>>(
        &self,
        src: T,
        dst: T,
        layer: Option<&str>,
    ) -> Option<EdgeView<Self>> {
        let layer_id = self.get_layer(layer)?;
        self.edge_ref(src.into(), dst.into(), layer_id)
            .map(|e| EdgeView::new(self.clone(), e))
    }

    fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<Self>> + Send> {
        Box::new(self.vertices().iter().flat_map(|v| v.out_edges()))
    }
}

impl<G: GraphViewOps> TimeOps for G {
    type WindowedViewType = WindowedGraph<Self>;
    type LayeredViewType = LayeredGraph<G>;

    fn start(&self) -> Option<i64> {
        self.view_start()
    }

    fn end(&self) -> Option<i64> {
        self.view_end()
    }

    fn window(&self, t_start: i64, t_end: i64) -> WindowedGraph<Self> {
        WindowedGraph::new(self.clone(), t_start, t_end)
    }

    fn default_layer(&self) -> Self::LayeredViewType {
        LayeredGraph::new(self.clone(), 0)
    }

    fn layer(&self, name: &str) -> Option<Self::LayeredViewType> {
        let id = self.get_layer(Some(name))?;
        Some(LayeredGraph::new(self.clone(), id))
    }
}
