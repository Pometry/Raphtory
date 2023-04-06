use crate::edge::EdgeView;
use crate::path::{Operations, PathFromGraph};
use crate::vertex::VertexView;
use crate::view_api::vertex::BoxedIter;
use crate::view_api::*;
use docbrown_core::tgraph::VertexRef;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;
use std::ops::{Index, Range};

#[derive(Clone)]
pub struct Vertices<G: GraphViewOps> {
    graph: G,
    window: Option<Range<i64>>,
}

impl<G: GraphViewOps> Vertices<G> {
    pub(crate) fn new(graph: G) -> Vertices<G> {
        Self {
            graph,
            window: None,
        }
    }
    pub fn iter(&self) -> Box<dyn Iterator<Item = VertexView<G>> + Send> {
        let g = self.graph.clone();
        let w = self.window.clone();
        Box::new(
            g.vertex_refs()
                .map(move |v| VertexView::new_windowed(g.clone(), v, w.clone())),
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
    type PathType = PathFromGraph<G>;
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

    fn properties(&self, include_static: bool) -> Self::ValueType<HashMap<String, Prop>> {
        self.iter().properties(include_static)
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
        match &self.window {
            None => PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir }),
            Some(w) => PathFromGraph::new(
                self.graph.clone(),
                Operations::NeighboursWindow {
                    dir,
                    t_start: w.start,
                    t_end: w.end,
                },
            ),
        }
    }

    fn in_neighbours(&self) -> PathFromGraph<G> {
        let dir = Direction::IN;
        match &self.window {
            None => PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir }),
            Some(w) => PathFromGraph::new(
                self.graph.clone(),
                Operations::NeighboursWindow {
                    dir,
                    t_start: w.start,
                    t_end: w.end,
                },
            ),
        }
    }

    fn out_neighbours(&self) -> PathFromGraph<G> {
        let dir = Direction::OUT;
        match &self.window {
            None => PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir }),
            Some(w) => PathFromGraph::new(
                self.graph.clone(),
                Operations::NeighboursWindow {
                    dir,
                    t_start: w.start,
                    t_end: w.end,
                },
            ),
        }
    }
}

impl<G: GraphViewOps> TimeOps for Vertices<G> {
    type WindowedViewType = Self;

    fn start(&self) -> Option<i64> {
        match &self.window {
            None => self.graph.start(),
            Some(w) => Some(w.start),
        }
    }

    fn end(&self) -> Option<i64> {
        match &self.window {
            None => self.graph.end(),
            Some(w) => Some(w.end),
        }
    }

    fn window(&self, t_start: i64, t_end: i64) -> Self::WindowedViewType {
        Self {
            graph: self.graph.clone(),
            window: Some(self.actual_start(t_start)..self.actual_end(t_end)),
        }
    }
}

impl<G: GraphViewOps> IntoIterator for Vertices<G> {
    type Item = VertexView<G>;
    type IntoIter = Box<dyn Iterator<Item = VertexView<G>> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
