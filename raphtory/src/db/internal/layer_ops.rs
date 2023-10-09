use crate::{
    core::entities::{edges::edge_store::EdgeStore, LayerIds},
    db::{
        api::view::internal::{
            core_views::edge::{CoreEdgeOps, CoreEdgeView},
            InternalLayerOps,
        },
        graph::graph::InternalGraph,
    },
    prelude::Layer,
};

impl InternalLayerOps for InternalGraph {
    fn layer_ids(&self) -> LayerIds {
        LayerIds::All
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.inner().layer_id(key)
    }

    fn edge_layer_ids(&self, e: &CoreEdgeView) -> LayerIds {
        e.layer_ids()
    }
}
