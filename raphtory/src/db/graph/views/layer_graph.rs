use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::{
            internal::{Base, GraphOps, InheritCoreOps, InheritMaterialize, InheritTimeSemantics},
            Layer,
        },
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct LayeredGraph<G: GraphViewOps> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The layer this graphs points to.
    pub layers: LayerIds,
}

impl<G: GraphViewOps> Base for LayeredGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: GraphViewOps> InheritTimeSemantics for LayeredGraph<G> {}

impl<G: GraphViewOps> InheritCoreOps for LayeredGraph<G> {}

impl<G: GraphViewOps> InheritMaterialize for LayeredGraph<G> {}

impl<G: GraphViewOps> InheritPropertiesOps for LayeredGraph<G> {}

impl<G: GraphViewOps> LayeredGraph<G> {
    pub fn new(graph: G, layers: LayerIds) -> Self {
        Self { graph, layers }
    }

    /// Get the intersection between the previously requested layers and the layers of
    /// this view
    fn constrain(&self, layers: LayerIds) -> LayerIds {
        match layers {
            LayerIds::None => LayerIds::None,
            LayerIds::All => self.layers.clone(),
            _ => match &self.layers {
                LayerIds::All => layers,
                LayerIds::One(id) => match layers.find(*id) {
                    Some(layer) => LayerIds::One(layer),
                    None => LayerIds::None,
                },
                LayerIds::Multiple(ids) => {
                    // intersect the layers
                    let new_layers = ids.iter().filter_map(|id| layers.find(*id)).collect_vec();
                    match new_layers.len() {
                        0 => LayerIds::None,
                        1 => LayerIds::One(new_layers[0]),
                        _ => LayerIds::Multiple(new_layers.into()),
                    }
                }
                LayerIds::None => LayerIds::None,
            },
        }
    }
}

impl<G: GraphViewOps> GraphOps for LayeredGraph<G> {
    fn layer_ids(&self) -> LayerIds {
        self.layers.clone()
    }

    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        self.graph.local_vertex_ref(v)
    }

    fn find_edge_id(&self, e_id: EID) -> Option<EdgeRef> {
        let edge_ref = self.graph.find_edge_id(e_id)?;
        let edge_ref_in_layer =
            self.graph
                .has_edge_ref(edge_ref.src(), edge_ref.dst(), self.layers.clone());

        if edge_ref_in_layer {
            Some(edge_ref)
        } else {
            None
        }
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.constrain(self.graph.layer_ids_from_names(key))
    }

    fn edge_layer_ids(&self, e_id: EID) -> LayerIds {
        let layer_ids = self.graph.edge_layer_ids(e_id);
        self.constrain(layer_ids)
    }

    fn vertices_len(&self) -> usize {
        self.graph.vertices_len()
    }

    fn edges_len(&self, layers: LayerIds) -> usize {
        self.graph.edges_len(self.constrain(layers))
    }

    fn has_edge_ref(&self, src: VID, dst: VID, layer: LayerIds) -> bool {
        self.graph.has_edge_ref(src, dst, self.constrain(layer))
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.graph.has_vertex_ref(v)
    }

    fn degree(&self, v: VID, d: Direction, layer: LayerIds) -> usize {
        self.graph.degree(v, d, self.constrain(layer))
    }

    fn vertex_ref(&self, v: u64) -> Option<VID> {
        self.graph.vertex_ref(v)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        self.graph.vertex_refs()
    }

    fn edge_ref(&self, src: VID, dst: VID, layer: LayerIds) -> Option<EdgeRef> {
        self.graph.edge_ref(src, dst, self.constrain(layer))
    }

    fn edge_refs(&self, layer: LayerIds) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph.edge_refs(self.constrain(layer))
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph.vertex_edges(v, d, self.constrain(layer))
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
    ) -> Box<dyn Iterator<Item = VID> + Send> {
        self.graph.neighbours(v, d, self.constrain(layer))
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64, layers: LayerIds) -> usize {
        self.graph.edges_len_window(t_start, t_end, self.constrain(layers))
    }
}
