use crate::core::time::IntoTime;
use crate::core::vertex_ref::VertexRef;
use crate::core::{Direction, Prop};
use crate::db::edge::EdgeView;
use crate::db::graph_layer::LayeredGraph;
use crate::db::graph_window::WindowedGraph;
use crate::db::path::{Operations, PathFromGraph};
use crate::db::vertex::VertexView;
use crate::db::view_api::*;
use std::collections::HashMap;

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

    /// Returns an iterator over the vertices properties
    /// If include_static is true, static properties are included
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property
    /// * `include_static` - If true, static properties are included
    ///
    /// # Returns
    ///
    /// An iterator over the vertices properties
    fn property(&self, name: String, include_static: bool) -> Self::ValueType<Option<Prop>> {
        self.iter().property(name, include_static)
    }

    /// Returns an iterator over the vertices property with the complete history
    /// If include_static is true, static properties are included
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property
    ///
    /// # Returns
    ///
    /// An iterator over the vertices property with the complete history as a
    /// vector of tuples of the time and the property
    fn property_history(&self, name: String) -> Self::ValueType<Vec<(i64, Prop)>> {
        self.iter().property_history(name)
    }

    /// Returns the history of the vertices which is a vector of the times
    /// the vertex was updated.
    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.iter().history()
    }

    /// Returns an iterator over the vertices and all their properties
    ///
    /// # Arguments
    ///
    /// * `include_static` - If true, static properties are included
    ///
    /// # Returns
    ///
    /// An iterator over the vertices and all their properties
    fn properties(&self, include_static: bool) -> Self::ValueType<HashMap<String, Prop>> {
        self.iter().properties(include_static)
    }

    /// Returns an iterator over the vertices and all their properties at all times
    fn property_histories(&self) -> Self::ValueType<HashMap<String, Vec<(i64, Prop)>>> {
        self.iter().property_histories()
    }

    /// Returns the names of all the properties of the vertices.
    ///
    /// # Arguments
    ///
    /// * `include_static` - If true, static properties are included
    fn property_names(&self, include_static: bool) -> Self::ValueType<Vec<String>> {
        self.iter().property_names(include_static)
    }

    /// Checks if a property exists on this vertices
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property
    /// * `include_static` - If true, static properties are included
    ///
    /// # Returns
    ///
    /// A vector of booleans indicating if the property exists in each vertex
    fn has_property(&self, name: String, include_static: bool) -> Self::ValueType<bool> {
        self.iter().has_property(name, include_static)
    }

    /// Checks if a static property exists on the vertices
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property
    ///
    /// # Returns
    ///
    /// A vector of booleans indicating if the property exists in each vertex
    fn has_static_property(&self, name: String) -> Self::ValueType<bool> {
        self.iter().has_static_property(name)
    }

    /// Returns the static property value of the vertices given the name of the property.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property
    ///
    /// # Returns
    ///
    /// An iterator of the static property of the vertices
    fn static_property(&self, name: String) -> Self::ValueType<Option<Prop>> {
        self.iter().static_property(name)
    }

    /// Returns static properties of the vertices
    ///
    /// # Returns
    ///
    /// An iterator of the static properties of the vertices
    fn static_properties(&self) -> Self::ValueType<HashMap<String, Prop>> {
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
