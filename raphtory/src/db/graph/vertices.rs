use crate::{
    core::entities::{edges::edge_ref::EdgeRef, VID},
    db::{
        api::view::{internal::GraphOps, BaseVertexViewOps, IntoDynBoxed},
        graph::path::PathFromGraph,
    },
};
use crate::{
    core::{entities::vertices::vertex_ref::VertexRef, utils::time::IntoTime, Direction},
    db::{
        api::{
            properties::Properties,
            view::{BoxedIter, Layer, LayerOps},
        },
        graph::{
            edge::EdgeView,
            // path::{Operations, PathFromGraph},
            vertex::VertexView,
            views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
        },
    },
    prelude::*,
};
use std::{iter, sync::Arc};

#[derive(Clone)]
pub struct Vertices<G: GraphViewOps, GH: GraphViewOps> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
}

impl<G: GraphViewOps> Vertices<G, G> {
    pub fn new(graph: G) -> Vertices<G, G> {
        let base_graph = graph.clone();
        Self { base_graph, graph }
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> Vertices<G, GH> {
    fn iter_refs(&self) -> impl Iterator<Item = VID> {
        self.graph
            .vertex_refs(self.graph.layer_ids(), self.graph.edge_filter())
    }
    pub fn iter(&self) -> impl Iterator<Item = VertexView<G, GH>> {
        let base_graph = self.base_graph.clone();
        let g = self.graph.clone();
        self.iter_refs()
            .map(move |v| VertexView::new_one_hop_filtered(base_graph.clone(), g.clone(), v))
    }

    /// Returns the number of vertices in the graph.
    pub fn len(&self) -> usize {
        self.graph.count_vertices()
    }

    /// Returns true if the graph contains no vertices.
    pub fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    pub fn get<V: Into<VertexRef>>(&self, vertex: V) -> Option<VertexView<G, GH>> {
        let vid = self.graph.internalise_vertex(vertex.into())?;
        Some(VertexView::new_one_hop_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            vid,
        ))
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> BaseVertexViewOps for Vertices<G, GH> {
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T> = BoxedIter<T>;
    type PathType = PathFromGraph<G, G>;
    type PropType = VertexView<GH, GH>;
    type Edge = EdgeView<G>;
    type EList = BoxedIter<BoxedIter<EdgeView<G>>>;

    fn map<O, F: for<'a> Fn(&'a Self::Graph, VID) -> O + Send + Sync>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        let g = self.graph.clone();
        Box::new(self.iter_refs().map(move |v| op(&g, v)))
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
            .map(move |v| {
                op(&graph, v)
                    .map(|edge| EdgeView::new(base_graph.clone(), edge))
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
        let graph = self.graph.clone();
        PathFromGraph::new(self.base_graph.clone(), move |v| {
            op(&graph, v).into_dyn_boxed()
        })
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> TimeOps for Vertices<G, GH> {
    type WindowedViewType = Vertices<G, WindowedGraph<GH>>;

    fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn window<T: IntoTime>(&self, start: T, end: T) -> Self::WindowedViewType {
        let base_graph = self.base_graph.clone();
        let graph = self.graph.window(start, end);
        Vertices { base_graph, graph }
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> LayerOps for Vertices<G, GH> {
    type LayeredViewType = Vertices<G, LayeredGraph<GH>>;

    /// Create a view including all the vertices in the default layer
    ///
    /// Returns:
    ///
    /// A view including all the vertices in the default layer
    fn default_layer(&self) -> Self::LayeredViewType {
        let base_graph = self.base_graph.clone();
        let graph = self.graph.default_layer();
        Vertices { base_graph, graph }
    }

    /// Create a view including all the vertices in the given layer
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the layer
    ///
    /// Returns:
    ///
    /// A view including all the vertices in the given layer
    fn layer<L: Into<Layer>>(&self, name: L) -> Option<Self::LayeredViewType> {
        let base_graph = self.base_graph.clone();
        let graph = self.graph.layer(name)?;
        Some(Vertices { base_graph, graph })
    }
}

impl<G: GraphViewOps, GH: GraphViewOps> IntoIterator for Vertices<G, GH> {
    type Item = VertexView<G, GH>;
    type IntoIter = BoxedIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().into_dyn_boxed()
    }
}
