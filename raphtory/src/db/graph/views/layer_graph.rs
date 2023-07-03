use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, VID},
        Direction,
    },
    db::api::view::internal::{
        Base, GraphOps, InheritCoreOps, InheritMaterialize, InheritTimeSemantics,
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct LayeredGraph<G: GraphViewOps> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The layer this graphs points to.
    pub layer: usize,
}

impl<G: GraphViewOps> Base for LayeredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: GraphViewOps> InheritTimeSemantics for LayeredGraph<G> {}

impl<G: GraphViewOps> InheritCoreOps for LayeredGraph<G> {}

impl<G: GraphViewOps> InheritMaterialize for LayeredGraph<G> {}

impl<G: GraphViewOps> LayeredGraph<G> {
    pub fn new(graph: G, layer: usize) -> Self {
        Self { graph, layer }
    }

    /// Return None if the intersection between the previously requested layers and the layer of
    /// this view is null
    fn constrain(&self, layer: Option<usize>) -> Option<usize> {
        Some(layer.unwrap_or(self.layer))
    }
}

impl<G: GraphViewOps> GraphOps for LayeredGraph<G> {
    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        self.graph.local_vertex_ref(v)
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        let layers = self.graph.get_unique_layers_internal();
        layers
            .into_iter()
            .filter(|id| *id == self.layer)
            .collect_vec()
    }

    fn get_layer_id(&self, key: Option<&str>) -> Option<usize> {
        self.graph.get_layer_id(key)
    }

    fn vertices_len(&self) -> usize {
        self.graph.vertices_len()
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        self.constrain(layer)
            .map(|layer| self.graph.edges_len(Some(layer)))
            .unwrap_or(0)
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        // FIXME: there is something wrong here, the layer should be able to be None, which would mean, whatever layer this is
        layer == self.layer && self.graph.has_edge_ref(src, dst, layer)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.graph.has_vertex_ref(v)
    }

    fn degree(&self, v: VID, d: Direction, layer: Option<usize>) -> usize {
        self.constrain(layer)
            .map(|layer| self.graph.degree(v, d, Some(layer)))
            .unwrap_or(0)
    }

    fn vertex_ref(&self, v: u64) -> Option<VID> {
        self.graph.vertex_ref(v)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        self.graph.vertex_refs()
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        (layer == self.layer)
            .then(|| self.graph.edge_ref(src, dst, layer))
            .flatten()
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        // TODO: create a function empty_iter which returns a boxed empty iterator so we use it in all these functions
        self.constrain(layer)
            .map(|layer| self.graph.edge_refs(Some(layer)))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph.vertex_edges(v, d, self.constrain(layer))
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.constrain(layer)
            .map(|layer| self.graph.neighbours(v, d, Some(layer)))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }
}
