use crate::db::api::properties::internal::{StaticProperties, TemporalProperties};
use crate::{
    core::{entities::vertices::vertex_ref::VertexRef, utils::time::IntoTime, Direction},
    db::{
        api::view::{BoxedIter, LayerOps},
        graph::{
            edge::EdgeView,
            path::{Operations, PathFromGraph},
            vertex::VertexView,
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
        },
    },
    prelude::*,
};

#[derive(Clone)]
pub struct Vertices<G: GraphViewOps> {
    pub graph: G,
}

impl<G: GraphViewOps> Vertices<G> {
    pub fn new(graph: G) -> Vertices<G> {
        Self { graph }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = VertexView<G>> + Send> {
        let g = self.graph.clone();
        Box::new(
            g.vertex_refs()
                .map(move |v| VertexView::new_local(g.clone(), v)),
        )
    }

    /// Returns the number of vertices in the graph.
    pub fn len(&self) -> usize {
        self.graph.num_vertices()
    }

    /// Returns true if the graph contains no vertices.
    pub fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    pub fn get<V: Into<VertexRef>>(&self, vertex: V) -> Option<VertexView<G>> {
        self.graph.vertex(vertex)
    }
}

impl<G: GraphViewOps> VertexViewOps for Vertices<G> {
    type Graph = G;
    type ValueType<T> = BoxedIter<T>;
    type PathType<'a> = PathFromGraph<G>;
    type EList = BoxedIter<BoxedIter<EdgeView<G>>>;

    /// Returns an iterator over the vertices id
    fn id(&self) -> Self::ValueType<u64> {
        self.iter().id()
    }

    /// Returns an iterator over the vertices name
    fn name(&self) -> Self::ValueType<String> {
        self.iter().name()
    }

    /// Returns an iterator over the vertices earliest time
    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.iter().earliest_time()
    }

    /// Returns an iterator over the vertices latest time
    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.iter().latest_time()
    }

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.iter().history()
    }

    fn properties(&self) -> Self::ValueType<TemporalProperties<VertexView<G>>> {
        self.iter().properties()
    }

    fn static_properties(&self) -> Self::ValueType<StaticProperties<VertexView<G>>> {
        self.iter().static_properties()
    }

    /// Returns the number of edges of the vertices
    ///
    /// # Returns
    ///
    /// An iterator of the number of edges of the vertices
    fn degree(&self) -> Self::ValueType<usize> {
        self.iter().degree()
    }

    /// Returns the number of in edges of the vertices
    ///
    /// # Returns
    ///
    /// An iterator of the number of in edges of the vertices
    fn in_degree(&self) -> Self::ValueType<usize> {
        self.iter().in_degree()
    }

    /// Returns the number of out edges of the vertices
    ///
    /// # Returns
    ///
    /// An iterator of the number of out edges of the vertices
    fn out_degree(&self) -> Self::ValueType<usize> {
        self.iter().out_degree()
    }

    /// Returns the edges of the vertices
    ///
    /// # Returns
    ///
    /// An iterator of edges of the vertices
    fn edges(&self) -> Self::EList {
        Box::new(self.iter().map(|v| v.edges()))
    }

    /// Returns the in edges of the vertices
    ///
    /// # Returns
    ///
    /// An iterator of in edges of the vertices
    fn in_edges(&self) -> Self::EList {
        Box::new(self.iter().map(|v| v.in_edges()))
    }

    /// Returns the out edges of the vertices
    ///
    /// # Returns
    ///
    /// An iterator of out edges of the vertices
    fn out_edges(&self) -> Self::EList {
        Box::new(self.iter().map(|v| v.out_edges()))
    }

    /// Get the neighbours of the vertices
    ///
    /// # Returns
    ///
    /// An iterator of the neighbours of the vertices
    fn neighbours(&self) -> PathFromGraph<G> {
        let dir = Direction::BOTH;
        PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir })
    }

    /// Get the in neighbours of the vertices
    ///
    /// # Returns
    ///
    /// An iterator of the in neighbours of the vertices
    fn in_neighbours(&self) -> PathFromGraph<G> {
        let dir = Direction::IN;
        PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir })
    }

    /// Get the out neighbours of the vertices
    ///
    /// # Returns
    ///
    /// An iterator of the out neighbours of the vertices
    fn out_neighbours(&self) -> PathFromGraph<G> {
        let dir = Direction::OUT;
        PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir })
    }
}

impl<G: GraphViewOps> TimeOps for Vertices<G> {
    type WindowedViewType = Vertices<WindowedGraph<G>>;

    fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn window<T: IntoTime>(&self, t_start: T, t_end: T) -> Self::WindowedViewType {
        Vertices {
            graph: self.graph.window(t_start, t_end),
        }
    }
}

impl<G: GraphViewOps> LayerOps for Vertices<G> {
    type LayeredViewType = Vertices<LayeredGraph<G>>;

    /// Create a view including all the vertices in the default layer
    ///
    /// # Returns
    ///
    /// A view including all the vertices in the default layer
    fn default_layer(&self) -> Self::LayeredViewType {
        Vertices {
            graph: self.graph.default_layer(),
        }
    }

    /// Create a view including all the vertices in the given layer
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the layer
    ///
    /// # Returns
    ///
    /// A view including all the vertices in the given layer
    fn layer(&self, name: &str) -> Option<Self::LayeredViewType> {
        Some(Vertices {
            graph: self.graph.layer(name)?,
        })
    }
}

impl<G: GraphViewOps> IntoIterator for Vertices<G> {
    type Item = VertexView<G>;
    type IntoIter = Box<dyn Iterator<Item = VertexView<G>> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
