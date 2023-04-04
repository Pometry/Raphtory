use crate::edge::EdgeView;
use crate::vertex::VertexView;
use crate::view_api::*;
use docbrown_core::tgraph::VertexRef;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

#[derive(Copy, Clone)]
pub(crate) enum Operations {
    Neighbours {
        dir: Direction,
    },
    NeighboursWindow {
        dir: Direction,
        t_start: i64,
        t_end: i64,
    },
}

impl Operations {
    fn op<G: GraphViewOps>(
        self,
        graph: G,
        iter: Box<dyn Iterator<Item = VertexRef> + Send>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        match self {
            Operations::Neighbours { dir } => {
                Box::new(iter.flat_map(move |v| graph.neighbours(v, dir)))
            }
            Operations::NeighboursWindow {
                dir,
                t_start,
                t_end,
            } => Box::new(iter.flat_map(move |v| graph.neighbours_window(v, t_start, t_end, dir))),
        }
    }
}

pub struct PathFromGraph<G: GraphViewOps> {
    graph: G,
    operations: Arc<Vec<Operations>>,
}

impl<G: GraphViewOps> PathFromGraph<G> {
    pub(crate) fn new(graph: G, operation: Operations) -> PathFromGraph<G> {
        PathFromGraph {
            graph,
            operations: Arc::new(vec![operation]),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = PathFromVertex<G>> + Send> {
        let g = self.graph.clone();
        let ops = self.operations.clone();
        Box::new(self.graph.vertex_refs().map(move |v| PathFromVertex {
            graph: g.clone(),
            vertex: v,
            operations: ops.clone(),
        }))
    }

    pub fn id(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = u64> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.id()))
    }

    pub fn name(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = String> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.name()))
    }

    fn property(
        &self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = Option<Prop>> + Send>> + Send> {
        Box::new(
            self.iter()
                .map(move |it| it.property(name.clone(), include_static.clone())),
        )
    }

    fn property_history(
        &self,
        name: String,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = Vec<(i64, Prop)>> + Send>> + Send> {
        Box::new(self.iter().map(move |it| it.property_history(name.clone())))
    }

    fn properties(
        &self,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = HashMap<String, Prop>> + Send>> + Send>
    {
        Box::new(
            self.iter()
                .map(move |it| it.properties(include_static.clone())),
        )
    }

    fn property_histories(
        &self,
    ) -> Box<
        dyn Iterator<Item = Box<dyn Iterator<Item = HashMap<String, Vec<(i64, Prop)>>> + Send>>
            + Send,
    > {
        Box::new(self.iter().map(|it| it.property_histories()))
    }

    fn property_names(
        &self,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = Vec<String>> + Send>> + Send> {
        Box::new(
            self.iter()
                .map(move |it| it.property_names(include_static.clone())),
        )
    }

    fn has_property(
        &self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = bool> + Send>> + Send> {
        Box::new(
            self.iter()
                .map(move |it| it.has_property(name.clone(), include_static.clone())),
        )
    }

    fn has_static_property(
        &self,
        name: String,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = bool> + Send>> + Send> {
        Box::new(
            self.iter()
                .map(move |it| it.has_static_property(name.clone())),
        )
    }

    fn static_property(
        &self,
        name: String,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = Option<Prop>> + Send>> + Send> {
        Box::new(self.iter().map(move |it| it.static_property(name.clone())))
    }

    pub fn degree(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.degree()))
    }

    pub fn degree_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(move |it| it.degree_window(t_start, t_end)))
    }

    pub fn in_degree(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.in_degree()))
    }

    pub fn in_degree_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(
            self.iter()
                .map(move |it| it.in_degree_window(t_start, t_end)),
        )
    }

    pub fn out_degree(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.out_degree()))
    }

    pub fn out_degree_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(
            self.iter()
                .map(move |it| it.out_degree_window(t_start, t_end)),
        )
    }

    pub fn edges(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(self.iter().map(|it| it.edges()))
    }

    pub fn edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(self.iter().map(move |it| it.edges_window(t_start, t_end)))
    }

    pub fn in_edges(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(self.iter().map(|it| it.in_edges()))
    }

    pub fn in_edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(
            self.iter()
                .map(move |it| it.in_edges_window(t_start, t_end)),
        )
    }

    pub fn out_edges(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(self.iter().map(|it| it.out_edges()))
    }

    pub fn out_edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(
            self.iter()
                .map(move |it| it.out_edges_window(t_start, t_end)),
        )
    }

    pub fn neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours {
            dir: Direction::BOTH,
        });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    pub fn neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::BOTH,
        });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    pub fn in_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours { dir: Direction::IN });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    pub fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::IN,
        });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    pub fn out_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours {
            dir: Direction::OUT,
        });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    pub fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::OUT,
        });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }
}

pub struct PathFromVertex<G: GraphViewOps + Clone> {
    graph: G,
    vertex: VertexRef,
    operations: Arc<Vec<Operations>>,
}

impl<G: GraphViewOps> PathFromVertex<G> {
    pub fn iter(&self) -> Box<dyn Iterator<Item = VertexView<G>> + Send> {
        let init: Box<dyn Iterator<Item = VertexRef> + Send> = Box::new(iter::once(self.vertex));
        let g = self.graph.clone();
        let ops = self.operations.clone();
        Box::new(
            ops.iter()
                .fold(init, |it, op| Box::new(op.op(g.clone(), it)))
                .map(move |v| VertexView::new(g.clone(), v)),
        )
    }

    pub(crate) fn new<V: Into<VertexRef>>(
        graph: G,
        vertex: V,
        operation: Operations,
    ) -> PathFromVertex<G> {
        PathFromVertex {
            graph,
            vertex: vertex.into(),
            operations: Arc::new(vec![operation]),
        }
    }
}
impl<G: GraphViewOps> PathFromVertex<G> {
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
        self.properties(include_static)
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

    pub fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().edges()
    }

    pub fn edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().edges_window(t_start, t_end)
    }

    pub fn in_edges(&self) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().in_edges()
    }

    pub fn in_edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().in_edges_window(t_start, t_end)
    }

    pub fn out_edges(&self) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().out_edges()
    }

    pub fn out_edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().out_edges_window(t_start, t_end)
    }

    pub fn neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours {
            dir: Direction::BOTH,
        });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    pub fn neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::BOTH,
        });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    pub fn in_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours { dir: Direction::IN });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    pub fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::IN,
        });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    pub fn out_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours {
            dir: Direction::OUT,
        });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    pub fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::OUT,
        });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }
}

impl<G: GraphViewOps> IntoIterator for PathFromVertex<G> {
    type Item = VertexView<G>;
    type IntoIter = Box<dyn Iterator<Item = VertexView<G>> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
