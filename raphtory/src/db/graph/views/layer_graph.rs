use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            vertices::vertex_ref::VertexRef,
            LayerIds, EID, VID,
        },
        Direction,
    },
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::{
            internal::{
                Base, EdgeFilter, EdgeFilterOps, GraphOps, InheritCoreOps, InheritGraphOps,
                InheritMaterialize, InheritTimeSemantics, LayerOps,
            },
            Layer,
        },
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use std::sync::Arc;

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

impl<G: GraphViewOps> InheritGraphOps for LayeredGraph<G> {}

impl<G: GraphViewOps> EdgeFilterOps for LayeredGraph<G> {
    fn edge_filter(&self) -> Option<EdgeFilter> {
        match self.graph.edge_filter() {
            None => Some(Arc::new(|e, l| e.has_layer(l))),
            Some(f) => Some(Arc::new(move |e, l| e.has_layer(l) && f(e, l))),
        }
    }
}

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

impl<G: GraphViewOps> LayerOps for LayeredGraph<G> {
    fn layer_ids(&self) -> LayerIds {
        self.layers.clone()
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.constrain(self.graph.layer_ids_from_names(key))
    }

    fn edge_layer_ids(&self, e: &EdgeStore) -> LayerIds {
        let layer_ids = self.graph.edge_layer_ids(e);
        self.constrain(layer_ids)
    }
}
