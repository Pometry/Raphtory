use crate::{
    core::entities::{edges::edge_store::EdgeStore, LayerIds},
    db::{api::view::internal::LayerOps, graph::graph::InternalGraph},
    prelude::Layer,
};

impl LayerOps for InternalGraph {
    fn layer_ids(&self) -> LayerIds {
        LayerIds::All
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.inner().layer_id(key)
    }

    fn edge_layer_ids(&self, e: &EdgeStore) -> LayerIds {
        e.layer_ids()
    }
}
