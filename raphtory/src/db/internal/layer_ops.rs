use crate::{
    core::{entities::LayerIds, utils::errors::GraphError},
    db::{api::view::internal::InternalLayerOps, graph::graph::InternalGraph},
    prelude::Layer,
};

impl InternalLayerOps for InternalGraph {
    fn layer_ids(&self) -> LayerIds {
        LayerIds::All
    }

    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError> {
        self.inner().layer_id(key)
    }
}
