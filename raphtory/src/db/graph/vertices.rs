use crate::{
    core::entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
    db::{
        api::view::{
            internal::{GraphOps, InternalLayerOps, OneHopFilter},
            BaseVertexViewOps, BoxedLIter, IntoDynBoxed,
        },
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
use std::{iter, marker::PhantomData, sync::Arc};

#[derive(Clone)]
pub struct Vertices<'graph, G, GH> {
    pub(crate) base_graph: G,
    pub(crate) graph: GH,
    _marker: PhantomData<&'graph G>,
}

impl<'graph, G: GraphViewOps<'graph>> Vertices<'graph, G, G> {
    pub fn new(graph: G) -> Vertices<'graph, G, G> {
        let base_graph = graph.clone();
        Self {
            base_graph,
            graph,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> Vertices<'graph, G, GH> {
    pub fn new_filtered(base_graph: G, graph: GH) -> Self {
        Self {
            base_graph,
            graph,
            _marker: PhantomData,
        }
    }
    fn iter_refs(&self) -> impl Iterator<Item = VID> + 'graph {
        self.graph
            .vertex_refs(self.graph.layer_ids(), self.graph.edge_filter())
    }
    pub fn iter(&self) -> BoxedLIter<'graph, VertexView<G, GH>> {
        let base_graph = self.base_graph.clone();
        let g = self.graph.clone();
        self.iter_refs()
            .map(move |v| VertexView::new_one_hop_filtered(base_graph.clone(), g.clone(), v))
            .into_dyn_boxed()
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

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> InternalLayerOps
    for Vertices<'graph, G, GH>
{
    fn layer_ids(&self) -> LayerIds {
        self.graph.layer_ids()
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.graph.layer_ids_from_names(key)
    }
}

impl<'graph, G: GraphViewOps<'graph> + 'graph, GH: GraphViewOps<'graph> + 'graph>
    BaseVertexViewOps<'graph> for Vertices<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T: 'graph> = BoxedLIter<'graph, T>;
    type PropType = VertexView<GH, GH>;
    type PathType = PathFromGraph<'graph, G, G>;
    type Edge = EdgeView<G, GH>;
    type EList = BoxedLIter<'graph, BoxedLIter<'graph, EdgeView<G, GH>>>;

    fn map<O: 'graph, F: for<'a> Fn(&'a Self::Graph, VID) -> O + Send + Sync + 'graph>(
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
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::EList {
        let graph = self.graph.clone();
        let base_graph = self.base_graph.clone();
        self.iter_refs()
            .map(move |v| {
                let base_graph = base_graph.clone();
                let graph = graph.clone();
                op(&graph, v)
                    .map(move |edge| {
                        EdgeView::new_filtered(base_graph.clone(), graph.clone(), edge)
                    })
                    .into_dyn_boxed()
            })
            .into_dyn_boxed()
    }

    fn hop<
        I: Iterator<Item = VID> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync + 'graph,
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

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for Vertices<'graph, G, GH>
{
    type Graph = GH;
    type Filtered<GHH: GraphViewOps<'graph>> = Vertices<'graph, G, GHH>;

    fn current_filter(&self) -> &Self::Graph {
        &self.graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph>>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let base_graph = self.base_graph.clone();
        Vertices {
            base_graph,
            graph: filtered_graph,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph> + 'graph, GH: GraphViewOps<'graph> + 'graph> IntoIterator
    for Vertices<'graph, G, GH>
{
    type Item = VertexView<G, GH>;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().into_dyn_boxed()
    }
}
