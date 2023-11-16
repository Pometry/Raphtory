use crate::{
    core::{
        entities::{vertices::vertex_ref::VertexRef, VID},
        utils::time::IntoTime,
        Direction,
    },
    db::{
        api::{
            properties::Properties,
            view::{internal::extend_filter, BoxedIter, Layer, LayerOps},
        },
        graph::{
            edge::EdgeView,
            vertex::VertexView,
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
        },
    },
    prelude::*,
};
use std::{iter, sync::Arc};

#[derive(Copy, Clone)]
pub enum Operations {
    Neighbours {
        dir: Direction,
    },
    NeighboursWindow {
        dir: Direction,
        start: i64,
        end: i64,
    },
}

impl Operations {
    fn op<G: GraphViewOps>(
        self,
        graph: G,
        iter: Box<dyn Iterator<Item = VID> + Send>,
    ) -> Box<dyn Iterator<Item = VID> + Send> {
        let layer_ids = graph.layer_ids();
        let edge_filter = graph.edge_filter().cloned();
        match self {
            Operations::Neighbours { dir } => Box::new(iter.flat_map(move |v| {
                graph.neighbours(v, dir, layer_ids.clone(), edge_filter.as_ref())
            })),
            Operations::NeighboursWindow { dir, start, end } => {
                let graph1 = graph.clone();
                let filter = Some(extend_filter(edge_filter, move |e, l| {
                    graph1.include_edge_window(e, start..end, l)
                }));
                Box::new(iter.flat_map(move |v| {
                    graph.neighbours(v, dir, layer_ids.clone(), filter.as_ref())
                }))
            }
        }
    }
}

#[derive(Clone)]
pub struct PathFromGraph<G: GraphViewOps> {
    pub graph: G,
    pub operations: Arc<Vec<Operations>>,
}

impl<G: GraphViewOps> PathFromGraph<G> {
    pub fn new(graph: G, operation: Operations) -> PathFromGraph<G> {
        PathFromGraph {
            graph,
            operations: Arc::new(vec![operation]),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = PathFromVertex<G>> + Send> {
        let g = self.graph.clone();
        let ops = self.operations.clone();
        Box::new(
            g.vertex_refs(g.layer_ids(), g.edge_filter())
                .map(move |v| PathFromVertex {
                    graph: g.clone(),
                    vertex: v,
                    operations: ops.clone(),
                }),
        )
    }
}

impl<G: GraphViewOps> VertexViewOps for PathFromGraph<G> {
    type Graph = G;
    type ValueType<T> = Box<dyn Iterator<Item = Box<dyn Iterator<Item = T> + Send>> + Send>;
    type PathType<'a> = Self where Self: 'a;
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

    fn history(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = Vec<i64>> + Send>> + Send> {
        Box::new(self.iter().map(move |it| it.history()))
    }

    fn properties(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = Properties<VertexView<G>>> + Send>> + Send>
    {
        Box::new(self.iter().map(move |it| it.properties()))
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
        new_ops.push(Operations::Neighbours { dir });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    fn in_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        let dir = Direction::IN;
        new_ops.push(Operations::Neighbours { dir });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    fn out_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        let dir = Direction::OUT;
        new_ops.push(Operations::Neighbours { dir });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }
}

impl<G: GraphViewOps> TimeOps for PathFromGraph<G> {
    type WindowedViewType = PathFromGraph<WindowedGraph<G>>;

    fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn window<T: IntoTime>(&self, start: T, end: T) -> Self::WindowedViewType {
        PathFromGraph {
            graph: self.graph.window(start, end),
            operations: self.operations.clone(),
        }
    }
}

impl<G: GraphViewOps> LayerOps for PathFromGraph<G> {
    type LayeredViewType = PathFromGraph<LayeredGraph<G>>;

    fn default_layer(&self) -> Self::LayeredViewType {
        PathFromGraph {
            graph: self.graph.default_layer(),
            operations: self.operations.clone(),
        }
    }

    fn layer<L: Into<Layer>>(&self, name: L) -> Option<Self::LayeredViewType> {
        Some(PathFromGraph {
            graph: self.graph.layer(name)?,
            operations: self.operations.clone(),
        })
    }
}

#[derive(Clone)]
pub struct PathFromVertex<G: GraphViewOps> {
    pub graph: G,
    pub vertex: VID,
    pub operations: Arc<Vec<Operations>>,
}

impl<G: GraphViewOps> PathFromVertex<G> {
    pub fn iter_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        let init: Box<dyn Iterator<Item = VID> + Send> = Box::new(iter::once(self.vertex));
        let g = self.graph.clone();
        let ops = self.operations.clone();
        let iter = ops
            .iter()
            .fold(init, |it, op| Box::new(op.op(g.clone(), it)));
        Box::new(iter)
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = VertexView<G>> + Send> {
        let g = self.graph.clone();
        let iter = self
            .iter_refs()
            .map(move |v| VertexView::new_internal(g.clone(), v));
        Box::new(iter)
    }

    pub fn new<V: Into<VertexRef>>(
        graph: G,
        vertex: V,
        operation: Operations,
    ) -> PathFromVertex<G> {
        let v = graph.internalise_vertex_unchecked(vertex.into());
        PathFromVertex {
            graph,
            vertex: v,
            operations: Arc::new(vec![operation]),
        }
    }

    pub fn neighbours_window(&self, dir: Direction, start: i64, end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow { dir, start, end });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }
}

impl<G: GraphViewOps> VertexViewOps for PathFromVertex<G> {
    type Graph = G;
    type ValueType<T> = BoxedIter<T>;
    type PathType<'a> = Self where Self: 'a;
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

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.iter().history()
    }

    fn properties(&self) -> Self::ValueType<Properties<VertexView<G>>> {
        self.iter().properties()
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
        new_ops.push(Operations::Neighbours { dir });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    fn in_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        let dir = Direction::IN;
        new_ops.push(Operations::Neighbours { dir });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    fn out_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        let dir = Direction::OUT;
        new_ops.push(Operations::Neighbours { dir });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }
}

impl<G: GraphViewOps> TimeOps for PathFromVertex<G> {
    type WindowedViewType = PathFromVertex<WindowedGraph<G>>;

    fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn window<T: IntoTime>(&self, start: T, end: T) -> Self::WindowedViewType {
        PathFromVertex {
            graph: self.graph.window(start, end),
            vertex: self.vertex,
            operations: self.operations.clone(),
        }
    }
}

impl<G: GraphViewOps> LayerOps for PathFromVertex<G> {
    type LayeredViewType = PathFromVertex<LayeredGraph<G>>;

    fn default_layer(&self) -> Self::LayeredViewType {
        PathFromVertex {
            graph: self.graph.default_layer(),
            vertex: self.vertex,
            operations: self.operations.clone(),
        }
    }

    fn layer<L: Into<Layer>>(&self, name: L) -> Option<Self::LayeredViewType> {
        Some(PathFromVertex {
            graph: self.graph.layer(name)?,
            vertex: self.vertex,
            operations: self.operations.clone(),
        })
    }
}

impl<G: GraphViewOps> IntoIterator for PathFromVertex<G> {
    type Item = VertexView<G>;
    type IntoIter = Box<dyn Iterator<Item = VertexView<G>> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
