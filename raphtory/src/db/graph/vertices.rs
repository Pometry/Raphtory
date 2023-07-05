use crate::db::api::properties::internal::TemporalProperties;
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

    pub fn len(&self) -> usize {
        self.graph.num_vertices()
    }

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

    fn id(&self) -> Self::ValueType<u64> {
        self.iter().id()
    }

    fn name(&self) -> Self::ValueType<String> {
        self.iter().name()
    }

    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.iter().earliest_time()
    }

    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.iter().latest_time()
    }

    fn property(&self, name: String, include_static: bool) -> Self::ValueType<Option<Prop>> {
        self.iter().property(name, include_static)
    }

    fn property_history(&self, name: String) -> Self::ValueType<Vec<(i64, Prop)>> {
        self.iter().property_history(name)
    }

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.iter().history()
    }

    fn properties(&self) -> Self::ValueType<TemporalProperties<VertexView<G>>> {
        self.iter().properties()
    }

    fn property_histories(&self) -> Self::ValueType<HashMap<String, Vec<(i64, Prop)>>> {
        self.iter().property_histories()
    }

    fn property_names(&self, include_static: bool) -> Self::ValueType<Vec<String>> {
        self.iter().property_names(include_static)
    }

    fn has_property(&self, name: String, include_static: bool) -> Self::ValueType<bool> {
        self.iter().has_property(name, include_static)
    }

    fn has_static_property(&self, name: String) -> Self::ValueType<bool> {
        self.iter().has_static_property(name)
    }

    fn static_property(&self, name: String) -> Self::ValueType<Option<Prop>> {
        self.iter().static_property(name)
    }

    fn static_properties(&self) -> Self::ValueType<HashMap<String, Prop>> {
        self.iter().static_properties()
    }

    fn degree(&self) -> Self::ValueType<usize> {
        self.iter().degree()
    }

    fn in_degree(&self) -> Self::ValueType<usize> {
        self.iter().in_degree()
    }

    fn out_degree(&self) -> Self::ValueType<usize> {
        self.iter().out_degree()
    }

    fn edges(&self) -> Self::EList {
        Box::new(self.iter().map(|v| v.edges()))
    }

    fn in_edges(&self) -> Self::EList {
        Box::new(self.iter().map(|v| v.in_edges()))
    }

    fn out_edges(&self) -> Self::EList {
        Box::new(self.iter().map(|v| v.out_edges()))
    }

    fn neighbours(&self) -> PathFromGraph<G> {
        let dir = Direction::BOTH;
        PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir })
    }

    fn in_neighbours(&self) -> PathFromGraph<G> {
        let dir = Direction::IN;
        PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir })
    }

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

    fn default_layer(&self) -> Self::LayeredViewType {
        Vertices {
            graph: self.graph.default_layer(),
        }
    }

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
