use std::ops::Range;

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::{
            internal::{Base, GraphOps, InheritCoreOps, InheritMaterialize, TimeSemantics},
            BoxedIter, Layer,
        },
    },
    prelude::{GraphViewOps, Prop},
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

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

// impl<G: GraphViewOps> InheritTimeSemantics for LayeredGraph<G> {}

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

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: LayerIds) -> bool {
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

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: LayerIds) -> Option<EdgeRef> {
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
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.neighbours(v, d, self.constrain(layer))
    }
}

impl<G: GraphViewOps> TimeSemantics for LayeredGraph<G> {
    fn include_vertex_window(&self, v: VID, w: Range<i64>) -> bool {
        self.graph.include_vertex_window(v, w)
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>, layer_ids: LayerIds) -> bool {
        self.graph
            .include_edge_window(e, w, self.constrain(layer_ids))
    }

    fn edge_t(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.graph.edge_t(e, self.constrain(layer_ids))
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.graph.edge_layers(e, self.constrain(layer_ids))
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.graph.edge_window_t(e, w, self.constrain(layer_ids))
    }

    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        self.graph
            .edge_window_layers(e, w, self.constrain(layer_ids))
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        self.graph.edge_earliest_time(e, self.constrain(layer_ids))
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        self.graph
            .edge_earliest_time_window(e, w, self.constrain(layer_ids))
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        self.graph.edge_latest_time(e, self.constrain(layer_ids))
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        self.graph
            .edge_latest_time_window(e, w, self.constrain(layer_ids))
    }

    fn edge_deletion_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        self.graph
            .edge_deletion_history(e, self.constrain(layer_ids))
    }

    fn edge_deletion_history_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Vec<i64> {
        self.graph
            .edge_deletion_history_window(e, w, self.constrain(layer_ids))
    }

    fn temporal_prop_vec(&self, name: &str) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec(name)
    }

    fn temporal_prop_vec_window(&self, name: &str, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec_window(name, t_start, t_end)
    }

    fn temporal_vertex_prop_vec(&self, v: VID, name: &str) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VID,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: &str,
        t_start: i64,
        t_end: i64,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_edge_prop_vec_window(e, name, t_start, t_end, self.constrain(layer_ids))
    }

    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        name: &str,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_edge_prop_vec(e, name, self.constrain(layer_ids))
    }
}
