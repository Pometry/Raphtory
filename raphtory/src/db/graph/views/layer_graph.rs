use std::ops::Range;

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::internal::{
            Base, GraphOps, InheritCoreOps, InheritMaterialize, InheritTimeSemantics, TimeSemantics,
        },
        view::{BoxedIter, Layer},
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

    /// Return None if the intersection between the previously requested layers and the layer of
    /// this view is null
    fn constrain(&self, layers: LayerIds) -> Option<LayerIds> {
        match &self.layers {
            LayerIds::All => Some(layers),
            LayerIds::One(id) => layers.find(*id).map(|layer| LayerIds::One(layer)),
            LayerIds::Multiple(ids) => {
                // intersect the layers
                let new_layers = ids.iter().filter_map(|id| layers.find(*id)).collect_vec();
                if new_layers.is_empty() {
                    None
                } else {
                    Some(LayerIds::Multiple(new_layers.into()))
                }
            }
        }
    }
}

impl<G: GraphViewOps> GraphOps for LayeredGraph<G> {
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

    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        self.graph.local_vertex_ref(v)
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        let layers = self.graph.get_unique_layers_internal();
        layers
            .into_iter()
            .filter_map(|id| self.layers.find(id))
            .collect_vec()
    }

    fn get_layer_id(&self, key: Layer) -> Option<LayerIds> {
        self.graph.get_layer_id(key)
    }

    fn vertices_len(&self) -> usize {
        self.graph.vertices_len()
    }

    fn edges_len(&self, layers: LayerIds) -> usize {
        self.constrain(layers)
            .map(|layer| self.graph.edges_len(layer))
            .unwrap_or(0)
    }

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: LayerIds) -> bool {
        // FIXME: there is something wrong here, the layer should be able to be None, which would mean, whatever layer this is
        self.constrain(layer)
            .map(|layer| self.graph.has_edge_ref(src, dst, layer))
            .unwrap_or(false)
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.graph.has_vertex_ref(v)
    }

    fn degree(&self, v: VID, d: Direction, layer: LayerIds) -> usize {
        self.constrain(layer)
            .map(|layer| self.graph.degree(v, d, layer))
            .unwrap_or(0)
    }

    fn vertex_ref(&self, v: u64) -> Option<VID> {
        self.graph.vertex_ref(v)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        self.graph.vertex_refs()
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: LayerIds) -> Option<EdgeRef> {
        self.constrain(layer)
            .and_then(|layer| self.graph.edge_ref(src, dst, layer))
    }

    fn edge_refs(&self, layer: LayerIds) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        // TODO: create a function empty_iter which returns a boxed empty iterator so we use it in all these functions
        self.constrain(layer)
            .map(|layer| self.graph.edge_refs(layer))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.constrain(layer)
            .map(|layer| self.graph.vertex_edges(v, d, layer))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.constrain(layer)
            .map(|layer| self.graph.neighbours(v, d, layer))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn get_layer_ids(&self, e_id: EID) -> Option<LayerIds> {
        let layer_ids = self.graph.get_layer_ids(e_id)?;
        self.constrain(layer_ids)
    }
}

impl<G: GraphViewOps> TimeSemantics for LayeredGraph<G> {
    fn include_vertex_window(&self, v: VID, w: Range<i64>) -> bool {
        self.graph.include_vertex_window(v, w)
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
        self.constrain(layer_ids)
            .map(|layers| {
                self.graph
                    .temporal_edge_prop_vec_window(e, name, t_start, t_end, layers)
            })
            .unwrap_or_else(|| Vec::new())
    }

    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        name: &str,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.constrain(layer_ids)
            .map(|layers| self.graph.temporal_edge_prop_vec(e, name, layers))
            .unwrap_or_else(|| Vec::new())
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>, layer_ids: LayerIds) -> bool {
        self.constrain(layer_ids)
            .map(|layers| self.graph.include_edge_window(e, w, layers))
            .unwrap_or(false)
    }

    fn edge_t(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.constrain(layer_ids)
            .map(|layers| self.graph.edge_t(e, layers))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.constrain(layer_ids)
            .map(|layers| self.graph.edge_layers(e, layers))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.constrain(layer_ids)
            .map(|layers| self.graph.edge_window_t(e, w, layers))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        self.constrain(layer_ids)
            .map(|layers| self.graph.edge_window_layers(e, w, layers))
            .unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        self.constrain(layer_ids)
            .and_then(|layers| self.graph.edge_earliest_time(e, layers))
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        self.constrain(layer_ids)
            .and_then(|layers| self.graph.edge_earliest_time_window(e, w, layers))
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        self.constrain(layer_ids)
            .and_then(|layers| self.graph.edge_latest_time(e, layers))
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        self.constrain(layer_ids)
            .and_then(|layers| self.graph.edge_latest_time_window(e, w, layers))
    }

    fn edge_deletion_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        self.constrain(layer_ids)
            .map(|layers| self.graph.edge_deletion_history(e, layers))
            .unwrap_or_else(|| Vec::new())
    }

    fn edge_deletion_history_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Vec<i64> {
        self.constrain(layer_ids)
            .map(|layers| self.graph.edge_deletion_history_window(e, w, layers))
            .unwrap_or_else(|| Vec::new())
    }
}
