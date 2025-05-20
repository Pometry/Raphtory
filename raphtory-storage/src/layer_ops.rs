use crate::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, locked::LockedGraph},
};
use raphtory_api::core::entities::{Layer, LayerIds};
use raphtory_core::entities::graph::tgraph::InvalidLayer;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LayerIdsError {
    #[error(transparent)]
    InvalidLayer(#[from] InvalidLayer),
}
pub trait InternalLayerOps: CoreGraphOps {
    /// get the layer ids for the graph view
    fn layer_ids(&self) -> &LayerIds;

    /// Get the layer id for the given layer name
    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, LayerIdsError> {
        let layer_ids = match self.core_graph() {
            GraphStorage::Mem(LockedGraph { graph, .. }) | GraphStorage::Unlocked(graph) => {
                graph.layer_ids(key)
            }
        }?;
        Ok(layer_ids.intersect(self.layer_ids()))
    }

    /// Get the valid layer ids for given layer names
    fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        match self.core_graph() {
            GraphStorage::Unlocked(graph) | GraphStorage::Mem(LockedGraph { graph, .. }) => {
                graph.valid_layer_ids(key)
            }
        }
    }
}

impl InternalLayerOps for GraphStorage {
    fn layer_ids(&self) -> &LayerIds {
        &LayerIds::All
    }
}
