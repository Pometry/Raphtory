use crate::edge::EdgeView;
use crate::path::{Operations, PathFromGraph};
use crate::vertex::VertexView;
use crate::view_api::*;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;

pub struct Vertices<G: GraphViewOps> {
    graph: G,
}

impl<G: GraphViewOps> Vertices<G> {
    pub(crate) fn new(graph: G) -> Vertices<G> {
        Self { graph }
    }
    pub fn iter(&self) -> Box<dyn Iterator<Item = VertexView<G>> + Send> {
        let g = self.graph.clone();
        Box::new(g.vertex_refs().map(move |v| VertexView::new(g.clone(), v)))
    }

    pub fn id(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.iter().id()
    }

    pub fn name(&self) -> Box<dyn Iterator<Item = String> + Send> {
        self.iter().name()
    }

    fn property(
        &self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Option<Prop>> + Send> {
        self.iter().property(name, include_static)
    }

    fn property_history(&self, name: String) -> Box<dyn Iterator<Item = Vec<(i64, Prop)>> + Send> {
        self.iter().property_history(name)
    }

    fn properties(
        &self,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = HashMap<String, Prop>> + Send> {
        self.iter().properties(include_static)
    }

    fn property_histories(
        &self,
    ) -> Box<dyn Iterator<Item = HashMap<String, Vec<(i64, Prop)>>> + Send> {
        self.iter().property_histories()
    }

    fn property_names(&self, include_static: bool) -> Box<dyn Iterator<Item = Vec<String>> + Send> {
        self.iter().property_names(include_static)
    }

    fn has_property(
        &self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = bool> + Send> {
        self.iter().has_property(name, include_static)
    }

    fn has_static_property(&self, name: String) -> Box<dyn Iterator<Item = bool> + Send> {
        self.iter().has_static_property(name)
    }

    fn static_property(&self, name: String) -> Box<dyn Iterator<Item = Option<Prop>> + Send> {
        self.iter().static_property(name)
    }

    pub fn degree(&self) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().degree()
    }

    pub fn degree_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().degree_window(t_start, t_end)
    }

    pub fn in_degree(&self) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().in_degree()
    }

    pub fn in_degree_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().in_degree_window(t_start, t_end)
    }

    pub fn out_degree(&self) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().out_degree()
    }

    pub fn out_degree_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().out_degree_window(t_start, t_end)
    }

    pub fn edges(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(|v| v.edges()))
    }

    pub fn edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(move |v| v.edges_window(t_start, t_end)))
    }

    pub fn in_edges(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(|v| v.in_edges()))
    }

    pub fn in_edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(move |v| v.in_edges_window(t_start, t_end)))
    }

    pub fn out_edges(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(|v| v.out_edges()))
    }

    pub fn out_edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(move |v| v.out_edges_window(t_start, t_end)))
    }

    pub fn neighbours(&self) -> PathFromGraph<G> {
        PathFromGraph::new(
            self.graph.clone(),
            Operations::Neighbours {
                dir: Direction::BOTH,
            },
        )
    }

    pub fn neighbours_window(&self, t_start: i64, t_end: i64) -> PathFromGraph<G> {
        let g = self.graph.clone();
        PathFromGraph::new(
            g,
            Operations::NeighboursWindow {
                dir: Direction::BOTH,
                t_start,
                t_end,
            },
        )
    }

    pub fn in_neighbours(&self) -> PathFromGraph<G> {
        let g = self.graph.clone();
        PathFromGraph::new(g, Operations::Neighbours { dir: Direction::IN })
    }

    pub fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> PathFromGraph<G> {
        let g = self.graph.clone();
        PathFromGraph::new(
            g,
            Operations::NeighboursWindow {
                dir: Direction::IN,
                t_start,
                t_end,
            },
        )
    }

    pub fn out_neighbours(&self) -> PathFromGraph<G> {
        let g = self.graph.clone();
        PathFromGraph::new(
            g,
            Operations::Neighbours {
                dir: Direction::OUT,
            },
        )
    }

    pub fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> PathFromGraph<G> {
        let g = self.graph.clone();
        PathFromGraph::new(
            g,
            Operations::NeighboursWindow {
                dir: Direction::OUT,
                t_start,
                t_end,
            },
        )
    }
}

impl<G: GraphViewOps> IntoIterator for Vertices<G> {
    type Item = VertexView<G>;
    type IntoIter = Box<dyn Iterator<Item = VertexView<G>> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
