use crate::core::tgraph::VertexRef;
use crate::core::{Direction, Prop};
use crate::db::edge::EdgeView;
use crate::db::vertex::VertexView;
use crate::db::view_api::BoxedIter;
use crate::db::view_api::*;
use std::collections::HashMap;
use std::iter;
use std::ops::Range;
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
                Box::new(iter.flat_map(move |v| graph.neighbours(v, dir, None)))
            }
            Operations::NeighboursWindow {
                dir,
                t_start,
                t_end,
            } => Box::new(
                iter.flat_map(move |v| graph.neighbours_window(v, t_start, t_end, dir, None)),
            ),
        }
    }
}

#[derive(Clone)]
pub struct PathFromGraph<G: GraphViewOps> {
    graph: G,
    operations: Arc<Vec<Operations>>,
    window: Option<Range<i64>>,
}

impl<G: GraphViewOps> PathFromGraph<G> {
    pub(crate) fn new(graph: G, operation: Operations) -> PathFromGraph<G> {
        PathFromGraph {
            graph,
            operations: Arc::new(vec![operation]),
            window: None,
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = PathFromVertex<G>> + Send> {
        let g = self.graph.clone();
        let ops = self.operations.clone();
        let w = self.window.clone();
        Box::new(self.graph.vertex_refs().map(move |v| PathFromVertex {
            graph: g.clone(),
            vertex: v,
            operations: ops.clone(),
            window: w.clone(),
        }))
    }
}

impl<G: GraphViewOps> VertexViewOps for PathFromGraph<G> {
    type Graph = G;
    type ValueType<T> = Box<dyn Iterator<Item = Box<dyn Iterator<Item = T> + Send>> + Send>;
    type PathType = Self;
    type EList = Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send>;

    fn id(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = u64> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.id()))
    }

    fn name(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = String> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.name()))
    }

    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        Box::new(self.iter().map(|it| it.earliest_time()))
    }

    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        Box::new(self.iter().map(|it| it.latest_time()))
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

    fn history(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = Vec<i64>> + Send>> + Send> {
        Box::new(self.iter().map(move |it| it.history()))
    }

    fn properties(
        &self,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = HashMap<String, Prop>> + Send>> + Send>
    {
        Box::new(self.iter().map(move |it| it.properties(include_static)))
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
        Box::new(self.iter().map(move |it| it.property_names(include_static)))
    }

    fn has_property(
        &self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = bool> + Send>> + Send> {
        Box::new(
            self.iter()
                .map(move |it| it.has_property(name.clone(), include_static)),
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

    fn degree(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.degree()))
    }

    fn in_degree(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.in_degree()))
    }

    fn out_degree(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.out_degree()))
    }

    fn edges(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.edges()))
    }

    fn in_edges(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.in_edges()))
    }

    fn out_edges(&self) -> BoxedIter<BoxedIter<EdgeView<G>>> {
        Box::new(self.iter().map(|it| it.out_edges()))
    }

    fn neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        let dir = Direction::BOTH;
        match &self.window {
            None => new_ops.push(Operations::Neighbours { dir }),
            Some(window) => new_ops.push(Operations::NeighboursWindow {
                dir,
                t_start: window.start,
                t_end: window.end,
            }),
        }
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
            window: None,
        }
    }

    fn in_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        let dir = Direction::IN;
        match &self.window {
            None => new_ops.push(Operations::Neighbours { dir }),
            Some(window) => new_ops.push(Operations::NeighboursWindow {
                dir,
                t_start: window.start,
                t_end: window.end,
            }),
        }
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
            window: None,
        }
    }

    fn out_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        let dir = Direction::OUT;
        match &self.window {
            None => new_ops.push(Operations::Neighbours { dir }),
            Some(window) => new_ops.push(Operations::NeighboursWindow {
                dir,
                t_start: window.start,
                t_end: window.end,
            }),
        }
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
            window: None,
        }
    }
}

impl<G: GraphViewOps> TimeOps for PathFromGraph<G> {
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
            operations: self.operations.clone(),
            window: Some(self.actual_start(t_start)..self.actual_end(t_end)),
        }
    }
}

#[derive(Clone)]
pub struct PathFromVertex<G: GraphViewOps> {
    graph: G,
    vertex: VertexRef,
    operations: Arc<Vec<Operations>>,
    window: Option<Range<i64>>,
}

impl<G: GraphViewOps> PathFromVertex<G> {
    pub fn iter(&self) -> Box<dyn Iterator<Item = VertexView<G>> + Send> {
        let init: Box<dyn Iterator<Item = VertexRef> + Send> = Box::new(iter::once(self.vertex));
        let g = self.graph.clone();
        let ops = self.operations.clone();
        let iter = ops
            .iter()
            .fold(init, |it, op| Box::new(op.op(g.clone(), it)))
            .map(move |v| VertexView::new(g.clone(), v));
        let window = self.window.clone();
        if let Some(window) = window {
            Box::new(iter.map(move |v| v.window(window.start, window.end)))
        } else {
            Box::new(iter)
        }
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
            window: None,
        }
    }
}
impl<G: GraphViewOps> VertexViewOps for PathFromVertex<G> {
    type Graph = G;
    type ValueType<T> = BoxedIter<T>;
    type PathType = Self;
    type EList = BoxedIter<EdgeView<G>>;

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
        self.iter().edges()
    }

    fn in_edges(&self) -> Self::EList {
        self.iter().in_edges()
    }

    fn out_edges(&self) -> Self::EList {
        self.iter().out_edges()
    }

    fn neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        let dir = Direction::BOTH;
        match &self.window {
            None => new_ops.push(Operations::Neighbours { dir }),
            Some(window) => new_ops.push(Operations::NeighboursWindow {
                dir,
                t_start: window.start,
                t_end: window.end,
            }),
        }
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
            window: None,
        }
    }

    fn in_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        let dir = Direction::IN;
        match &self.window {
            None => new_ops.push(Operations::Neighbours { dir }),
            Some(window) => new_ops.push(Operations::NeighboursWindow {
                dir,
                t_start: window.start,
                t_end: window.end,
            }),
        }
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
            window: None,
        }
    }

    fn out_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        let dir = Direction::OUT;
        match &self.window {
            None => new_ops.push(Operations::Neighbours { dir }),
            Some(window) => new_ops.push(Operations::NeighboursWindow {
                dir,
                t_start: window.start,
                t_end: window.end,
            }),
        }
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
            window: None,
        }
    }
}

impl<G: GraphViewOps> TimeOps for PathFromVertex<G> {
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
            vertex: self.vertex,
            operations: self.operations.clone(),
            window: Some(self.actual_start(t_start)..self.actual_end(t_end)),
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
