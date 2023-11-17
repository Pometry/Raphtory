use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, VID},
        utils::time::IntoTime,
        Direction,
    },
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{extend_filter, Base},
                BaseVertexViewOps, BoxedIter, IntoDynBoxed, Layer, LayerOps,
            },
        },
        graph::{
            edge::EdgeView,
            vertex::VertexView,
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
        },
    },
    prelude::*,
};
use neo4rs::Path;
use std::{iter, sync::Arc};

pub(crate) type Operation = Arc<dyn Fn(VID) -> BoxedIter<VID> + Send + Sync>;
#[derive(Clone)]
pub struct PathFromGraph<G: GraphViewOps, GH: GraphViewOps> {
    pub graph: GH,
    base_graph: G,
    op: Operation,
}

impl<G: GraphViewOps> PathFromGraph<G, G> {
    pub fn new<OP: Fn(VID) -> BoxedIter<VID> + Send + Sync>(graph: G, op: OP) -> Self {
        let base_graph = graph.clone();
        let op: Operation = Arc::new(op);
        PathFromGraph {
            graph,
            base_graph,
            op,
        }
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> PathFromGraph<G, GH> {
    fn base_iter(&self) -> BoxedIter<VID> {
        self.graph
            .vertex_refs(self.graph.layer_ids(), self.graph.edge_filter())
    }

    fn filter_one_hop<GHH: GraphViewOps>(&self, graph: GHH) -> PathFromGraph<G, GHH> {
        let base_graph = self.base_graph.clone();
        let op = self.op.clone();
        PathFromGraph {
            graph,
            base_graph,
            op,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = PathFromVertex<G, GH>> + Send {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let op = self.op.clone();
        self.base_iter().map(move |vertex| {
            PathFromVertex::new_one_hop_filtered(
                base_graph.clone(),
                graph.clone(),
                vertex,
                op.clone(),
            )
        })
    }

    pub fn iter_refs(&self) -> impl Iterator<Item = BoxedIter<VID>> + Send {
        let op = self.op.clone();
        self.base_iter().map(move |vid| op(vid))
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> BaseVertexViewOps for PathFromGraph<G, GH> {
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T> = BoxedIter<BoxedIter<T>>;
    type PropType = VertexView<GH, GH>;
    type PathType = PathFromGraph<G, G>;
    type Edge = EdgeView<G>;
    type EList = BoxedIter<BoxedIter<EdgeView<G>>>;

    fn map<O, F: for<'a> Fn(&'a Self::Graph, VID) -> O + Send>(&self, op: F) -> Self::ValueType<O> {
        let graph = self.graph.clone();
        self.iter_refs()
            .map(move |it| it.map(move |vertex| op(&graph, vertex)).into_dyn_boxed())
            .into_dyn_boxed()
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|g, v| Properties::new(VertexView::new_internal(g.clone(), v)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync,
    >(
        &self,
        op: F,
    ) -> Self::EList {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        self.iter_refs()
            .map(move |it| {
                it.flat_map(move |vertex| {
                    op(&graph, vertex).map(|edge| EdgeView::new(base_graph.clone(), edge))
                })
                .into_dyn_boxed()
            })
            .into_dyn_boxed()
    }

    fn hop<
        I: Iterator<Item = VID> + Send,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let old_op = self.op.clone();
        let graph = self.graph.clone();
        PathFromGraph::new(self.base_graph.clone(), move |v| {
            Box::new(old_op(v).flat_map(move |vv| op(&graph, vv)))
        })
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> IntoIterator for PathFromGraph<G, GH> {
    type Item = PathFromVertex<G, GH>;
    type IntoIter = BoxedIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().into_dyn_boxed()
    }
}

#[derive(Clone)]
pub struct PathFromVertex<G: GraphViewOps, GH: GraphViewOps> {
    pub graph: GH,
    base_graph: G,
    pub vertex: VID,
    pub op: Operation,
}

impl<G: GraphViewOps> PathFromVertex<G, G> {
    pub(crate) fn new<V: Into<VertexRef>, OP: Fn(VID) -> BoxedIter<VID> + Send + Sync>(
        graph: G,
        vertex: V,
        op: OP,
    ) -> PathFromVertex<G, G> {
        let vertex = graph.internalise_vertex_unchecked(vertex.into());
        let base_graph = graph.clone();
        let op: Operation = Arc::new(op);
        PathFromVertex {
            base_graph,
            graph,
            vertex,
            op,
        }
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> PathFromVertex<G, GH> {
    pub fn iter_refs(&self) -> BoxedIter<VID> {
        let op = &self.op;
        op(self.vertex)
    }

    pub fn iter(&self) -> BoxedIter<VertexView<G, GH>> {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        let iter = self.iter_refs().map(move |vertex| {
            VertexView::new_one_hop_filtered(base_graph.clone(), graph.clone(), vertex)
        });
        Box::new(iter)
    }
    fn filter_one_hop<GHH: GraphViewOps>(&self, graph: GHH) -> PathFromVertex<G, GHH> {
        let base_graph = self.base_graph.clone();
        let op = self.op.clone();
        let vertex = self.vertex;
        PathFromVertex {
            graph,
            base_graph,
            vertex,
            op,
        }
    }

    pub(crate) fn new_one_hop_filtered(
        base_graph: G,
        graph: GH,
        vertex: VID,
        op: Operation,
    ) -> Self {
        Self {
            base_graph,
            graph,
            vertex,
            op,
        }
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> BaseVertexViewOps for PathFromVertex<G, GH> {
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T> = BoxedIter<T>;
    type PropType = VertexView<GH, GH>;
    type PathType = PathFromVertex<G, G>;
    type Edge = EdgeView<G>;
    type EList = BoxedIter<EdgeView<G>>;

    fn map<O, F: for<'a> Fn(&'a Self::Graph, VID) -> O + Send>(&self, op: F) -> Self::ValueType<O> {
        let graph = self.graph.clone();
        Box::new(self.iter_refs().map(move |vertex| op(&graph, vertex)))
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.map(|g, v| Properties::new(VertexView::new_internal(g.clone(), v)))
    }

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync,
    >(
        &self,
        op: F,
    ) -> Self::EList {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        Box::new(self.iter_refs().flat_map(move |vertex| {
            op(&graph, vertex).map(move |edge| EdgeView::new(base_graph.clone(), edge))
        }))
    }

    fn hop<
        I: Iterator<Item = VID> + Send,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync,
    >(
        &self,
        op: F,
    ) -> Self::PathType {
        let old_op = self.op.clone();
        let graph = self.graph.clone();

        PathFromVertex::new(self.base_graph.clone(), self.vertex, move |v| {
            Box::new(old_op(v).flat_map(move |vv| op(&graph, vv)))
        })
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> IntoIterator for PathFromVertex<G, GH> {
    type Item = VertexView<G, GH>;
    type IntoIter = BoxedIter<VertexView<G, GH>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    #[test]
    fn test_vertex_view_ops() {
        let g = Graph::new();

        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();

        let n = Vec::from_iter(g.vertex(1).unwrap().neighbours().id());
        assert_eq!(n, [2])
    }
}
